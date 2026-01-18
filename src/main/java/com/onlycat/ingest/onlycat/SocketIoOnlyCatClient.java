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
        // Log a handful of inbound events at INFO so users don't need DEBUG to verify flow.
        int n = infoLogged.getAndIncrement();
        if (n < 20) {
            log.info("Inbound event #{} {} payload={}", n + 1, event, Arrays.toString(args));
        } else if (sampleLogged.getAndIncrement() < 2) {
            log.info("Sample inbound event {} payload={}", event, Arrays.toString(args));
        } else {
            log.debug("Inbound event {} payload={}", event, Arrays.toString(args));
        }
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
}
