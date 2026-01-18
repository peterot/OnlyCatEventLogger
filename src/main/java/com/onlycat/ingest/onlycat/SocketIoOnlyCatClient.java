package com.onlycat.ingest.onlycat;

import com.onlycat.ingest.config.OnlyCatProperties;
import com.onlycat.ingest.service.EventIngestService;
import io.socket.client.Ack;
import io.socket.client.IO;
import io.socket.client.Manager;
import io.socket.client.Socket;
import io.socket.parser.Packet;
import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

@Component
public class SocketIoOnlyCatClient implements OnlyCatClient, ApplicationRunner, AutoCloseable {
    private static final Logger log = LoggerFactory.getLogger(SocketIoOnlyCatClient.class);

    /**
     * Hard allowlist of outbound emits.
     * Keep this list small and "read-only" in spirit.
     */
    private static final Set<String> ALLOWED_EMITS = Set.of(
            "getDevices",
            "getDevice",
            "getEvents",
            "getLastSeenRfidCodesByDevice",
            "getRfidProfile"
    );

    private final OnlyCatProperties properties;
    private final EventIngestService ingestService;

    private final AtomicInteger sampleLogged = new AtomicInteger();
    private final AtomicInteger infoLogged = new AtomicInteger();
    private final AtomicInteger packetSamples = new AtomicInteger();

    private final AtomicBoolean subscribedThisSession = new AtomicBoolean(false);
    private final AtomicBoolean anyListenerAvailable = new AtomicBoolean(false);

    // Enrichment: RFID -> cat label (name) cache and pending user events awaiting enrichment.
    private final Map<String, String> rfidLabelCache = new ConcurrentHashMap<>();
    private final Map<Integer, PendingUserEvent> pendingUserEvents = new ConcurrentHashMap<>();

    private final ScheduledExecutorService enrichmentScheduler = Executors.newSingleThreadScheduledExecutor(r -> {
        Thread t = new Thread(r, "onlycat-enrichment");
        t.setDaemon(true);
        return t;
    });

    private Socket socket;

    public SocketIoOnlyCatClient(OnlyCatProperties properties, EventIngestService ingestService) {
        this.properties = properties;
        this.ingestService = ingestService;
    }

    @Override
    public void run(ApplicationArguments args) {
        connect();
    }

    @Override
    public synchronized void connect() {
        if (socket != null && socket.connected()) {
            return;
        }

        IO.Options options = buildOptions();
        try {
            String target = properties.getGatewayUrl() + properties.getNamespace();
            socket = IO.socket(target, options);
        } catch (Exception e) {
            throw new IllegalStateException("Unable to create Socket.IO client", e);
        }

        socket.on(Socket.EVENT_CONNECT, args -> {
            log.info("Connected to OnlyCat gateway");
            subscribedThisSession.set(false);
            emitReadOnlySubscriptions();
        });

        socket.on(Socket.EVENT_CONNECT_ERROR, args -> {
            log.warn("Connection error args={}", Arrays.toString(args));
            if (args != null && args.length > 0 && args[0] instanceof Throwable t) {
                log.warn("Connection error throwable", t);
                if (t.getCause() != null) {
                    log.warn("Connection error cause", t.getCause());
                }
            }
        });

        socket.on(Socket.EVENT_DISCONNECT, args -> {
            log.warn("Disconnected: {}", Arrays.toString(args));
            subscribedThisSession.set(false);
        });

        socket.on("reconnect", args -> log.info("Reconnected"));
        socket.on("reconnecting", args -> log.info("Reconnecting..."));
        socket.on("error", args -> log.error("Socket error: {}", Arrays.toString(args)));
        socket.on("message", args -> handleAnyEvent("message", args));

        // Reconnect events are emitted by the Manager in socket.io-client-java
        socket.io().on(Manager.EVENT_RECONNECT, a -> log.info("Manager reconnect: {}", Arrays.toString(a)));
        socket.io().on(Manager.EVENT_RECONNECT_ATTEMPT, a -> log.info("Manager reconnect attempt: {}", Arrays.toString(a)));
        socket.io().on(Manager.EVENT_RECONNECT_ERROR, a -> log.warn("Manager reconnect error: {}", Arrays.toString(a)));
        socket.io().on(Manager.EVENT_RECONNECT_FAILED, a -> log.warn("Manager reconnect failed: {}", Arrays.toString(a)));
        socket.io().on(Manager.EVENT_ERROR, a -> log.warn("Manager error: {}", Arrays.toString(a)));
        socket.io().on(Manager.EVENT_TRANSPORT, a -> log.info("Manager transport: {}", Arrays.toString(a)));

        registerCatchAll(socket);
        registerPacketInterceptor(socket);

        socket.connect();
        log.info("Connecting to {}{} (token redacted)", properties.getGatewayUrl(), properties.getNamespace());
    }

    private IO.Options buildOptions() {
        IO.Options opts = new IO.Options();
        opts.reconnection = true;
        opts.reconnectionAttempts = Integer.MAX_VALUE;
        opts.reconnectionDelay = 1000;
        opts.reconnectionDelayMax = 10_000;
        opts.timeout = 20_000;

        // Prefer websocket first; some deployments reject/disable XHR polling.
        opts.transports = new String[]{"websocket", "polling"};
        opts.forceNew = true;

        Map<String, List<String>> headers = new HashMap<>();
        if (StringUtils.hasText(properties.getPlatform())) {
            headers.put("platform", List.of(properties.getPlatform()));
        }
        if (StringUtils.hasText(properties.getDevice())) {
            headers.put("device", List.of(properties.getDevice()));
        }
        if (!headers.isEmpty()) {
            opts.extraHeaders = headers;
        }

        // Token passed to the gateway only; do NOT log it.
        if (StringUtils.hasText(properties.getToken())) {
            Map<String, String> auth = new HashMap<>();
            auth.put("token", properties.getToken());
            opts.auth = auth;
            // Some deployments also accept token in query.
            opts.query = "token=" + URLEncoder.encode(properties.getToken(), StandardCharsets.UTF_8);
        }

        return opts;
    }

    private void registerCatchAll(Socket socket) {
        try {
            Class<?> anyListenerClass = Class.forName("io.socket.emitter.Emitter$AnyListener");
            Method onAny = socket.getClass().getMethod("onAny", anyListenerClass);
            Object listenerProxy = Proxy.newProxyInstance(
                    anyListenerClass.getClassLoader(),
                    new Class[]{anyListenerClass},
                    new AnyInvocationHandler());
            onAny.invoke(socket, listenerProxy);
            anyListenerAvailable.set(true);
            log.info("Registered catch-all Socket.IO handler");
        } catch (Exception ex) {
            // This is fine: many versions of the Java client do not ship AnyListener.
            anyListenerAvailable.set(false);
            log.info("Catch-all not available ({}) - registering basic handlers", ex.getMessage());
            for (String event : List.of("connect", "authenticated", "userUpdate", "device_event", "cat_event", "events")) {
                socket.on(event, args -> handleAnyEvent(event, args));
            }
        }
    }

    private void registerPacketInterceptor(Socket socket) {
        // Inspect low-level packets to capture all event names/payloads, including ACK packets.
        socket.io().on(Manager.EVENT_PACKET, args -> {
            if (args == null || args.length == 0 || !(args[0] instanceof Packet packet)) {
                return;
            }

            // Log raw packet metadata early to understand what the server is sending.
            int n = packetSamples.getAndIncrement();
            if (n < 50) {
                log.info("Packet raw nsp={} type={} dataClass={} data={}",
                        packet.nsp,
                        packet.type,
                        (packet.data == null ? "null" : packet.data.getClass().getName()),
                        String.valueOf(packet.data));
            } else {
                log.debug("Packet raw nsp={} type={} dataClass={} data={}",
                        packet.nsp,
                        packet.type,
                        (packet.data == null ? "null" : packet.data.getClass().getName()),
                        String.valueOf(packet.data));
            }

            // EVENT packets contain [eventName, ...payload]
            if (packet.type == io.socket.parser.Parser.EVENT || packet.type == io.socket.parser.Parser.BINARY_EVENT) {
                JSONArray array = null;
                if (packet.data instanceof JSONArray arr) {
                    array = arr;
                } else if (packet.data instanceof String str) {
                    String trimmed = str.trim();
                    if (trimmed.startsWith("[")) {
                        try {
                            array = new JSONArray(trimmed);
                        } catch (Exception ignore) {
                            // fall through; raw packet is still logged
                        }
                    }
                }

                if (array != null && array.length() >= 1) {
                    String eventName = array.optString(0, "unknown");
                    Object[] payload = new Object[Math.max(0, array.length() - 1)];
                    for (int i = 1; i < array.length(); i++) {
                        payload[i - 1] = array.opt(i);
                    }
                    if (!anyListenerAvailable.get()) {
                        handleAnyEvent(eventName, payload);
                    } else {
                        log.debug("Skipping EVENT packet dispatch because onAny is active: {}", eventName);
                    }
                } else {
                    log.info("EVENT packet without JSON array payload: dataClass={} data={}",
                            packet.data == null ? "null" : packet.data.getClass().getName(),
                            String.valueOf(packet.data));
                }
                return;
            }

            // ACK packets contain [...payload] and will NOT show up as EVENTs.
            if (packet.type == io.socket.parser.Parser.ACK || packet.type == io.socket.parser.Parser.BINARY_ACK) {
                if (packet.data instanceof JSONArray array) {
                    Object[] payload = new Object[array.length()];
                    for (int i = 0; i < array.length(); i++) {
                        payload[i] = array.opt(i);
                    }
                    handleAnyEvent("__ack__", payload);
                } else if (packet.data != null) {
                    handleAnyEvent("__ack__", new Object[]{packet.data});
                }
            }
        });
    }

    private void handleAnyEvent(String event, Object[] args) {
        // Special-case: enrich userEventUpdate(create) with cat identity (RFID profile label).
        if ("userEventUpdate".equals(event) && args != null && args.length >= 1) {
            JSONObject obj = coerceToJsonObject(args[0]);
            if (obj != null) {
                try {
                    Integer eventId = extractEventId(obj);
                    String deviceId = extractDeviceId(obj);
                    String type = obj.optString("type", "");

                    // Only enrich create events with a known deviceId/eventId.
                    if (eventId != null && StringUtils.hasText(deviceId) && "create".equalsIgnoreCase(type)) {
                        PendingUserEvent pending = new PendingUserEvent(eventId, deviceId, obj);
                        PendingUserEvent existing = pendingUserEvents.putIfAbsent(eventId, pending);
                        if (existing == null) {
                            // Fallback: if enrichment doesn't complete quickly, ingest raw event anyway.
                            ScheduledFuture<?> fallback = enrichmentScheduler.schedule(() -> {
                                PendingUserEvent p = pendingUserEvents.remove(eventId);
                                if (p != null && !p.ingested) {
                                    log.info("Enrichment timeout for eventId={} deviceId={} - ingesting raw event", eventId, deviceId);
                                    JSONObject decorated = decorateUserEventUpdate(new JSONObject(p.original.toString()));
                                    ingestService.handleInbound(event, new Object[]{decorated});
                                    p.ingested = true;
                                }
                            }, 3, TimeUnit.SECONDS);
                            pending.fallback = fallback;

                            // Kick off enrichment asynchronously via ACK callbacks.
                            resolveCatForUserEvent(pending);
                            return; // do not ingest yet; enrichment will ingest (or fallback will)
                        }
                    }
                } catch (Exception e) {
                    log.debug("UserEvent enrichment pre-check failed; ingesting raw event", e);
                }
            }
        }

        // For userEventUpdate, decorate with friendly classification/trigger strings even if we can't RFID-enrich.
        if ("userEventUpdate".equals(event) && args != null && args.length >= 1) {
            JSONObject obj = coerceToJsonObject(args[0]);
            if (obj != null) {
                JSONObject decorated = decorateUserEventUpdate(new JSONObject(obj.toString()));
                Object[] newArgs = Arrays.copyOf(args, args.length);
                newArgs[0] = decorated;
                logInbound(event, newArgs);
                ingestService.handleInbound(event, newArgs);
                return;
            }
        }

        logInbound(event, args);
        ingestService.handleInbound(event, args);
    }

    /**
     * Read-only subscription chain used by other prototype clients:
     * - getDevices {subscribe:true}
     * - for each deviceId -> getDevice {deviceId, subscribe:true}
     * - getEvents {subscribe:true, limit:50}
     *
     * This should not change device state; it requests data and server push.
     */
    private void emitReadOnlySubscriptions() {
        if (!StringUtils.hasText(properties.getRequestDeviceListEvent())) {
            log.info("Listen-only mode: requestDeviceListEvent not set; not emitting subscriptions");
            return;
        }
        if (!subscribedThisSession.compareAndSet(false, true)) {
            log.debug("Subscriptions already emitted for this session; skipping");
            return;
        }

        String event = properties.getRequestDeviceListEvent();
        if (!isAllowedEmit(event)) {
            log.warn("Refusing to emit '{}' because it is not allowlisted", event);
            return;
        }

        JSONObject payload = new JSONObject();
        payload.put("subscribe", true);

        log.info("Emitting read-only subscription: {} {}", event, payload);
        safeEmit(event, payload, ackArgs -> {
            log.info("ACK for '{}' -> {}", event, Arrays.toString(ackArgs));
            handleAnyEvent(event + ":ack", ackArgs);

            // Try to extract deviceIds from common response shapes.
            Set<String> deviceIds = extractDeviceIdsFromAck(ackArgs);
            if (!deviceIds.isEmpty()) {
                log.info("Discovered {} deviceId(s): {}", deviceIds.size(), deviceIds);
                for (String deviceId : deviceIds) {
                    emitGetDeviceSubscribe(deviceId);
                }
            } else {
                log.info("No deviceIds discovered from ACK; will still subscribe to events");
            }

            emitGetEventsSubscribe();
        });
    }

    private void emitGetDeviceSubscribe(String deviceId) {
        if (!StringUtils.hasText(deviceId)) {
            return;
        }
        if (!isAllowedEmit("getDevice")) {
            return;
        }
        JSONObject p = new JSONObject();
        p.put("deviceId", deviceId);
        p.put("subscribe", true);

        log.info("Emitting read-only subscription: getDevice {}", p);
        safeEmit("getDevice", p, ackArgs -> {
            log.info("ACK for 'getDevice' ({}) -> {}", deviceId, Arrays.toString(ackArgs));
            handleAnyEvent("getDevice:ack", ackArgs);
        });
    }

    private void emitGetEventsSubscribe() {
        if (!isAllowedEmit("getEvents")) {
            return;
        }
        JSONObject p = new JSONObject();
        p.put("subscribe", true);
        p.put("limit", 1);

        log.info("Emitting read-only subscription: getEvents {}", p);
        safeEmit("getEvents", p, ackArgs -> {
            log.info("ACK for 'getEvents' -> {}", Arrays.toString(ackArgs));
            handleAnyEvent("getEvents:ack", ackArgs);
        });
    }

    private void safeEmit(String event, Object payload, Ack ack) {
        if (!isAllowedEmit(event)) {
            log.warn("Refusing to emit '{}' because it is not allowlisted", event);
            return;
        }
        if (socket == null) {
            log.warn("Cannot emit '{}' because socket is null", event);
            return;
        }
        try {
            socket.emit(event, payload, ack);
        } catch (Exception e) {
            log.warn("Emit failed for '{}' payload={}", event, payload, e);
        }
    }

    private boolean isAllowedEmit(String event) {
        return StringUtils.hasText(event) && ALLOWED_EMITS.contains(event);
    }

    /**
     * Attempt to extract deviceIds from a variety of shapes seen in other clients.
     * We keep this permissive and non-throwing; raw ACK JSON is still recorded.
     */
    private Set<String> extractDeviceIdsFromAck(Object[] ackArgs) {
        Set<String> out = new HashSet<>();
        if (ackArgs == null || ackArgs.length == 0) {
            return out;
        }

        for (Object a : ackArgs) {
            if (a == null) continue;

            // Common shape: first arg is an array of device objects.
            if (a instanceof JSONArray arr) {
                for (int i = 0; i < arr.length(); i++) {
                    Object el = arr.opt(i);
                    extractDeviceIdFromObject(el, out);
                }
                continue;
            }

            // Another common shape: first arg is an object { devices: [...] }
            if (a instanceof JSONObject obj) {
                if (obj.has("devices") && obj.opt("devices") instanceof JSONArray arr) {
                    for (int i = 0; i < arr.length(); i++) {
                        extractDeviceIdFromObject(arr.opt(i), out);
                    }
                } else {
                    extractDeviceIdFromObject(obj, out);
                }
                continue;
            }

            // Sometimes it arrives as a String containing JSON.
            if (a instanceof String s) {
                try {
                    if (s.trim().startsWith("{")) {
                        extractDeviceIdFromObject(new JSONObject(s), out);
                    } else if (s.trim().startsWith("[")) {
                        JSONArray arr = new JSONArray(s);
                        for (int i = 0; i < arr.length(); i++) {
                            extractDeviceIdFromObject(arr.opt(i), out);
                        }
                    }
                } catch (Exception ignore) {
                    // ignore parsing failures; raw JSON is still captured elsewhere
                }
            }
        }

        return out;
    }

    private void extractDeviceIdFromObject(Object obj, Set<String> out) {
        if (obj == null) return;

        if (obj instanceof JSONObject jo) {
            // Common candidates.
            for (String key : List.of("deviceId", "id", "_id")) {
                Object v = jo.opt(key);
                if (v != null) {
                    String s = String.valueOf(v);
                    if (StringUtils.hasText(s) && s.length() <= 128) {
                        out.add(s);
                    }
                }
            }
        }
    }

    @Override
    public synchronized void disconnect() {
        if (socket != null) {
            socket.disconnect();
            socket.close();
        }
    }

    @Override
    public boolean isConnected() {
        return socket != null && socket.connected();
    }

    @Override
    public void close() {
        disconnect();
        enrichmentScheduler.shutdownNow();
    }

    private class AnyInvocationHandler implements InvocationHandler {
        @Override
        public Object invoke(Object proxy, Method method, Object[] args) {
            // AnyListener signature: void call(String event, Object... args)
            if (args != null && args.length >= 1) {
                String eventName = String.valueOf(args[0]);
                Object[] payload = Arrays.copyOfRange(args, 1, args.length);
                handleAnyEvent(eventName, payload);
            }
            return null;
        }
    }


    private void logInbound(String event, Object[] args) {
        int n = infoLogged.getAndIncrement();
        if (n < 20) {
            log.info("Inbound event #{} {} payload={}", n + 1, event, Arrays.toString(args));
        } else if (sampleLogged.getAndIncrement() < 2) {
            log.info("Sample inbound event {} payload={}", event, Arrays.toString(args));
        } else {
            log.debug("Inbound event {} payload={}", event, Arrays.toString(args));
        }
    }

    private JSONObject coerceToJsonObject(Object o) {
        if (o == null) return null;
        if (o instanceof JSONObject jo) return jo;
        if (o instanceof String s) {
            String t = s.trim();
            if (t.startsWith("{")) {
                try {
                    return new JSONObject(t);
                } catch (Exception ignore) {
                    return null;
                }
            }
        }
        return null;
    }

    private Integer extractEventId(JSONObject userEventUpdate) {
        // Prefer top-level eventId, otherwise look in body.eventId
        if (userEventUpdate.has("eventId")) {
            try {
                return userEventUpdate.getInt("eventId");
            } catch (Exception ignore) {
            }
        }
        JSONObject body = userEventUpdate.optJSONObject("body");
        if (body != null && body.has("eventId")) {
            try {
                return body.getInt("eventId");
            } catch (Exception ignore) {
            }
        }
        return null;
    }

    private String extractDeviceId(JSONObject userEventUpdate) {
        // Prefer top-level deviceId, otherwise look in body.deviceId
        String d = userEventUpdate.optString("deviceId", "");
        if (StringUtils.hasText(d)) return d;
        JSONObject body = userEventUpdate.optJSONObject("body");
        if (body != null) {
            d = body.optString("deviceId", "");
            if (StringUtils.hasText(d)) return d;
        }
        return "";
    }

    private void resolveCatForUserEvent(PendingUserEvent pending) {
        if (!isAllowedEmit("getLastSeenRfidCodesByDevice")) {
            log.warn("Cannot enrich eventId={} because getLastSeenRfidCodesByDevice is not allowlisted", pending.eventId);
            return;
        }

        JSONObject p = new JSONObject();
        p.put("deviceId", pending.deviceId);
        // Small backfill to match eventId -> rfidCode without pulling tons of history
        p.put("limit", 20);

        log.info("Enrichment: getLastSeenRfidCodesByDevice {}", p);
        safeEmit("getLastSeenRfidCodesByDevice", p, ackArgs -> {
            handleAnyEvent("getLastSeenRfidCodesByDevice:ack", ackArgs);

            String rfidCode = extractRfidForEventId(ackArgs, pending.eventId);
            if (!StringUtils.hasText(rfidCode)) {
                log.info("Enrichment: no rfidCode found for eventId={} deviceId={}", pending.eventId, pending.deviceId);
                return;
            }

            // Cache hit
            String label = rfidLabelCache.get(rfidCode);
            if (StringUtils.hasText(label)) {
                ingestEnrichedUserEvent(pending, rfidCode, label);
                return;
            }

            // Fetch profile for name/label
            resolveRfidProfileAndIngest(pending, rfidCode);
        });
    }

    private String extractRfidForEventId(Object[] ackArgs, int eventId) {
        if (ackArgs == null || ackArgs.length == 0) return "";

        for (Object a : ackArgs) {
            // Typical shape: first arg is an array of objects with fields incl. rfidCode, eventId
            if (a instanceof JSONArray arr) {
                for (int i = 0; i < arr.length(); i++) {
                    Object el = arr.opt(i);
                    String code = extractRfidFromObject(el, eventId);
                    if (StringUtils.hasText(code)) return code;
                }
            } else if (a instanceof JSONObject obj) {
                // Sometimes wrapped: { items: [...] } or similar
                for (String key : List.of("items", "data", "events")) {
                    Object v = obj.opt(key);
                    if (v instanceof JSONArray arr) {
                        for (int i = 0; i < arr.length(); i++) {
                            String code = extractRfidFromObject(arr.opt(i), eventId);
                            if (StringUtils.hasText(code)) return code;
                        }
                    }
                }
                String code = extractRfidFromObject(obj, eventId);
                if (StringUtils.hasText(code)) return code;
            } else if (a instanceof String s) {
                String t = s.trim();
                try {
                    if (t.startsWith("[")) {
                        JSONArray arr = new JSONArray(t);
                        for (int i = 0; i < arr.length(); i++) {
                            String code = extractRfidFromObject(arr.opt(i), eventId);
                            if (StringUtils.hasText(code)) return code;
                        }
                    } else if (t.startsWith("{")) {
                        JSONObject obj = new JSONObject(t);
                        String code = extractRfidFromObject(obj, eventId);
                        if (StringUtils.hasText(code)) return code;
                    }
                } catch (Exception ignore) {
                    // ignore
                }
            }
        }

        return "";
    }

    private String extractRfidFromObject(Object obj, int targetEventId) {
        if (!(obj instanceof JSONObject jo)) return "";
        Integer eid = null;
        try {
            if (jo.has("eventId")) {
                eid = jo.getInt("eventId");
            } else if (jo.has("globalId")) {
                // Not sure; prefer eventId
                eid = null;
            }
        } catch (Exception ignore) {
        }

        if (eid != null && eid == targetEventId) {
            String code = jo.optString("rfidCode", "");
            if (StringUtils.hasText(code)) return code;
        }
        return "";
    }

    private void resolveRfidProfileAndIngest(PendingUserEvent pending, String rfidCode) {
        if (!isAllowedEmit("getRfidProfile")) {
            log.warn("Cannot enrich eventId={} because getRfidProfile is not allowlisted", pending.eventId);
            return;
        }

        JSONObject p = new JSONObject();
        p.put("rfidCode", rfidCode);
        log.info("Enrichment: getRfidProfile {}", p);
        safeEmit("getRfidProfile", p, ackArgs -> {
            handleAnyEvent("getRfidProfile:ack", ackArgs);

            String label = extractRfidLabel(ackArgs);
            if (StringUtils.hasText(label)) {
                rfidLabelCache.put(rfidCode, label);
                ingestEnrichedUserEvent(pending, rfidCode, label);
            } else {
                log.info("Enrichment: getRfidProfile returned no label for rfidCode={}", rfidCode);
            }
        });
    }

    private String extractRfidLabel(Object[] ackArgs) {
        if (ackArgs == null || ackArgs.length == 0) return "";
        for (Object a : ackArgs) {
            if (a instanceof JSONObject obj) {
                String label = obj.optString("label", "");
                if (StringUtils.hasText(label)) return label;
                // Sometimes wrapped
                JSONObject body = obj.optJSONObject("body");
                if (body != null) {
                    label = body.optString("label", "");
                    if (StringUtils.hasText(label)) return label;
                }
            } else if (a instanceof String s) {
                String t = s.trim();
                if (t.startsWith("{")) {
                    try {
                        JSONObject obj = new JSONObject(t);
                        String label = obj.optString("label", "");
                        if (StringUtils.hasText(label)) return label;
                    } catch (Exception ignore) {
                        // ignore
                    }
                }
            }
        }
        return "";
    }

    private void ingestEnrichedUserEvent(PendingUserEvent pending, String rfidCode, String label) {
        PendingUserEvent removed = pendingUserEvents.remove(pending.eventId);
        if (removed == null) {
            // Already ingested by fallback
            return;
        }
        if (removed.fallback != null) {
            removed.fallback.cancel(false);
        }
        if (removed.ingested) {
            return;
        }

        // Attach enrichment fields to the event JSON.
        JSONObject enriched = decorateUserEventUpdate(new JSONObject(removed.original.toString()));
        enriched.put("rfidCode", rfidCode);
        enriched.put("catLabel", label);
        JSONObject cat = enriched.optJSONObject("cat");
        if (cat == null) {
            cat = new JSONObject();
        }
        cat.put("rfidCode", rfidCode);
        cat.put("label", label);
        enriched.put("cat", cat);

        log.info("Enrichment complete for eventId={} deviceId={} catLabel={}", removed.eventId, removed.deviceId, label);
        ingestService.handleInbound("userEventUpdate", new Object[]{enriched});
        removed.ingested = true;
    }

    private static class PendingUserEvent {
        final int eventId;
        final String deviceId;
        final JSONObject original;
        volatile boolean ingested = false;
        volatile ScheduledFuture<?> fallback;

        PendingUserEvent(int eventId, String deviceId, JSONObject original) {
            this.eventId = eventId;
            this.deviceId = deviceId;
            this.original = original;
        }
    }
}

    /**
     * Add friendly fields for eventClassification/eventTriggerSource.
     * Based on OnlyCat's published Door Policy schema enums.
     */
    private JSONObject decorateUserEventUpdate(JSONObject userEventUpdate) {
        if (userEventUpdate == null) return null;

        JSONObject body = userEventUpdate.optJSONObject("body");
        if (body == null) {
            body = new JSONObject();
            userEventUpdate.put("body", body);
        }

        // Values may live top-level or under body; prefer body if present.
        Integer trigger = readInt(body, "eventTriggerSource");
        if (trigger == null) trigger = readInt(userEventUpdate, "eventTriggerSource");

        Integer classification = readInt(body, "eventClassification");
        if (classification == null) classification = readInt(userEventUpdate, "eventClassification");

        if (trigger != null) {
            String triggerCode = mapEventTriggerSource(trigger);
            String triggerPretty = prettyEventTriggerSource(trigger);
            userEventUpdate.put("eventTriggerSourceCode", triggerCode);
            userEventUpdate.put("eventTriggerSourceLabel", triggerPretty);
            body.put("eventTriggerSourceCode", triggerCode);
            body.put("eventTriggerSourceLabel", triggerPretty);
        }

        if (classification != null) {
            String classCode = mapEventClassification(classification);
            String classPretty = prettyEventClassification(classification);
            userEventUpdate.put("eventClassificationCode", classCode);
            userEventUpdate.put("eventClassificationLabel", classPretty);
            body.put("eventClassificationCode", classCode);
            body.put("eventClassificationLabel", classPretty);

            // Convenience booleans for spreadsheets.
            userEventUpdate.put("isEntry", classification == 2);
            userEventUpdate.put("isExit", classification == 3);
            userEventUpdate.put("isPrey", classification == 4);
            body.put("isEntry", classification == 2);
            body.put("isExit", classification == 3);
            body.put("isPrey", classification == 4);
        }

        return userEventUpdate;
    }

    private Integer readInt(JSONObject obj, String key) {
        if (obj == null || key == null) return null;
        Object v = obj.opt(key);
        if (v == null) return null;
        if (v instanceof Number n) return n.intValue();
        if (v instanceof String s) {
            try {
                return Integer.parseInt(s);
            } catch (Exception ignore) {
                return null;
            }
        }
        return null;
    }

    // Trigger source enum mapping: 0 MANUAL, 1 APP, 2 PET, 3 DEVICE
    private String mapEventTriggerSource(int value) {
        return switch (value) {
            case 0 -> "MANUAL";
            case 1 -> "APP";
            case 2 -> "PET";
            case 3 -> "DEVICE";
            default -> "UNKNOWN(" + value + ")";
        };
    }

    private String prettyEventTriggerSource(int value) {
        return switch (value) {
            case 0 -> "Manual";
            case 1 -> "App";
            case 2 -> "Pet";
            case 3 -> "Device";
            default -> "Unknown (" + value + ")";
        };
    }

    // Classification enum mapping: 0 NONE, 1 MOVEMENT, 2 ENTRY, 3 EXIT, 4 PREY
    private String mapEventClassification(int value) {
        return switch (value) {
            case 0 -> "NONE";
            case 1 -> "MOVEMENT";
            case 2 -> "ENTRY";
            case 3 -> "EXIT";
            case 4 -> "PREY";
            default -> "UNKNOWN(" + value + ")";
        };
    }

    private String prettyEventClassification(int value) {
        return switch (value) {
            case 0 -> "None";
            case 1 -> "Movement";
            case 2 -> "Entry";
            case 3 -> "Exit";
            case 4 -> "Prey";
            default -> "Unknown (" + value + ")";
        };
    }