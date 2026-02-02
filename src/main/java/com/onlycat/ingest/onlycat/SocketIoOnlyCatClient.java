package com.onlycat.ingest.onlycat;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.onlycat.ingest.config.BackfillProperties;
import com.onlycat.ingest.config.OnlyCatProperties;
import com.onlycat.ingest.model.OnlyCatEventClassification;
import com.onlycat.ingest.model.OnlyCatEventTriggerSource;
import com.onlycat.ingest.model.OnlyCatInboundEvent;
import com.onlycat.ingest.model.OnlyCatRfidEvent;
import com.onlycat.ingest.model.OnlyCatRfidProfile;
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
import org.springframework.context.ApplicationEventPublisher;
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
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

@Component
public class SocketIoOnlyCatClient implements OnlyCatClient, OnlyCatEmitter, ApplicationRunner, AutoCloseable {
    private static final Logger log = LoggerFactory.getLogger(SocketIoOnlyCatClient.class);

    /**
     * Hard allowlist of outbound emits.
     * Keep this list small and "read-only" in spirit.
     */
    private static final Set<String> ALLOWED_EMITS = Set.of(
            "getDevices",
            "getDevice",
            "getEvents",
            "getDeviceEvents",
            "getLastSeenRfidCodesByDevice",
            "getRfidProfile"
    );
    private static final long EVENTS_REFRESH_DELAY_MS = 1500;

    private final OnlyCatProperties properties;
    private final BackfillProperties backfillProperties;
    private final ApplicationEventPublisher eventPublisher;
    private final ObjectMapper objectMapper;
    private final ScheduledExecutorService refreshExecutor;

    private final AtomicInteger sampleLogged = new AtomicInteger();
    private final AtomicInteger infoLogged = new AtomicInteger();
    private final AtomicInteger packetSamples = new AtomicInteger();

    private final AtomicBoolean subscribedThisSession = new AtomicBoolean(false);
    private final AtomicBoolean anyListenerAvailable = new AtomicBoolean(false);

    private Socket socket;

    public SocketIoOnlyCatClient(OnlyCatProperties properties,
                                 BackfillProperties backfillProperties,
                                 ApplicationEventPublisher eventPublisher,
                                 ObjectMapper objectMapper) {
        this.properties = properties;
        this.backfillProperties = backfillProperties;
        this.eventPublisher = eventPublisher;
        this.objectMapper = objectMapper;
        this.refreshExecutor = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread thread = new Thread(r, "onlycat-event-refresh");
            thread.setDaemon(true);
            return thread;
        });
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
        opts.reconnection = properties.isReconnectEnabled();
        opts.reconnectionAttempts = properties.isReconnectEnabled() ? Integer.MAX_VALUE : 0;
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
            if (properties.isSendAuthPayload()) {
                opts.auth = auth;
            }
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
        logInbound(event, args);
        OnlyCatInboundEvent inbound = parseInboundEvent(event, args);
        scheduleEventRefresh(inbound);
        eventPublisher.publishEvent(inbound);
    }

    private OnlyCatInboundEvent parseInboundEvent(String event, Object[] args) {
        Object payload = firstPayload(args);
        OnlyCatUserEventUpdatePayload parsed = readPayload(payload);
        if (parsed == null) {
            return new OnlyCatInboundEvent(
                    event,
                    null,
                    null,
                    null,
                    null,
                    null,
                    null,
                    null,
                    null,
                    args
            );
        }

        Integer eventId = parsed.effectiveEventId();
        String eventType = parsed.type();
        Integer eventTriggerSourceCode = parsed.effectiveEventTriggerSource();
        Integer eventClassificationCode = parsed.effectiveEventClassification();
        String deviceId = parsed.effectiveDeviceId();
        String timestamp = parsed.effectiveTimestamp();
        Long globalId = parsed.effectiveGlobalId();
        String accessToken = parsed.effectiveAccessToken();

        OnlyCatEventTriggerSource eventTriggerSource = OnlyCatEventTriggerSource.fromCode(eventTriggerSourceCode);
        OnlyCatEventClassification eventClassification = OnlyCatEventClassification.fromCode(eventClassificationCode);

        return new OnlyCatInboundEvent(
                event,
                eventId,
                eventType,
                eventTriggerSource,
                eventClassification,
                deviceId,
                timestamp,
                globalId,
                accessToken,
                args
        );
    }

    private OnlyCatUserEventUpdatePayload readPayload(Object payload) {
        Object candidate = findPayloadObject(payload);
        if (candidate == null) {
            return null;
        }
        if (candidate instanceof OnlyCatUserEventUpdatePayload typed) {
            return typed;
        }
        if (candidate instanceof JSONObject jo) {
            return objectMapper.convertValue(jo.toMap(), OnlyCatUserEventUpdatePayload.class);
        }
        if (candidate instanceof Map<?, ?> map) {
            return objectMapper.convertValue(map, OnlyCatUserEventUpdatePayload.class);
        }
        return null;
    }

    private Object findPayloadObject(Object payload) {
        if (payload == null) {
            return null;
        }
        if (payload instanceof OnlyCatUserEventUpdatePayload) {
            return payload;
        }
        if (payload instanceof JSONObject jo) {
            return isUserEventPayload(jo) ? payload : null;
        }
        if (payload instanceof Map<?, ?> map) {
            return isUserEventPayload(map) ? payload : null;
        }
        if (payload instanceof JSONArray array) {
            for (int i = 0; i < array.length(); i++) {
                Object candidate = findPayloadObject(array.opt(i));
                if (candidate != null) {
                    return candidate;
                }
            }
        }
        if (payload instanceof List<?> list) {
            for (Object item : list) {
                Object candidate = findPayloadObject(item);
                if (candidate != null) {
                    return candidate;
                }
            }
        }
        return null;
    }

    private boolean isUserEventPayload(JSONObject payload) {
        return payload.has("type")
                || payload.has("body")
                || payload.has("eventClassification")
                || payload.has("eventTriggerSource")
                || payload.has("accessToken");
    }

    private boolean isUserEventPayload(Map<?, ?> payload) {
        return payload.containsKey("type")
                || payload.containsKey("body")
                || payload.containsKey("eventClassification")
                || payload.containsKey("eventTriggerSource")
                || payload.containsKey("accessToken");
    }

    private Object firstPayload(Object[] args) {
        if (args == null || args.length == 0) {
            return null;
        }
        for (Object arg : args) {
            if (arg != null) {
                return arg;
            }
        }
        return null;
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

        OnlyCatSubscribeRequest request = new OnlyCatSubscribeRequest(true);
        Map<String, Object> payload = objectMapper.convertValue(request, new TypeReference<Map<String, Object>>() {});

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
        OnlyCatDeviceSubscribeRequest request = new OnlyCatDeviceSubscribeRequest(deviceId, true);
        Map<String, Object> payload = objectMapper.convertValue(request, new TypeReference<Map<String, Object>>() {});

        log.info("Emitting read-only subscription: getDevice {}", payload);
        safeEmit("getDevice", payload, ackArgs -> {
            log.info("ACK for 'getDevice' ({}) -> {}", deviceId, Arrays.toString(ackArgs));
            handleAnyEvent("getDevice:ack", ackArgs);
        });
    }

    private void emitGetEventsSubscribe() {
        if (!isAllowedEmit("getEvents")) {
            return;
        }
        OnlyCatEventsSubscribeRequest request = new OnlyCatEventsSubscribeRequest(true, 1);
        Map<String, Object> payload = objectMapper.convertValue(request, new TypeReference<Map<String, Object>>() {});

        log.info("Emitting read-only subscription: getEvents {}", payload);
        safeEmit("getEvents", payload, ackArgs -> {
            log.info("ACK for 'getEvents' -> {}", Arrays.toString(ackArgs));
            handleAnyEvent("getEvents:ack", ackArgs);
        });
    }

    private void emitGetEventsRefresh(String reason) {
        if (!isAllowedEmit("getEvents")) {
            return;
        }
        OnlyCatEventsSubscribeRequest request = new OnlyCatEventsSubscribeRequest(false, 1);
        Map<String, Object> payload = objectMapper.convertValue(request, new TypeReference<Map<String, Object>>() {});

        log.info("Emitting read-only refresh: getEvents {} reason={}", payload, reason);
        safeEmit("getEvents", payload, ackArgs -> {
            log.info("ACK for 'getEvents' refresh -> {}", Arrays.toString(ackArgs));
            handleAnyEvent("getEvents:ack", ackArgs);
        });
    }

    @Override
    public List<OnlyCatRfidEvent> requestLastSeenRfidCodesByDevice(String deviceId, int limit) {
        if (!isAllowedEmit("getLastSeenRfidCodesByDevice")) {
            log.warn("Refusing to emit 'getLastSeenRfidCodesByDevice' because it is not allowlisted");
            return List.of();
        }
        if (!StringUtils.hasText(deviceId)) {
            log.warn("Refusing to emit 'getLastSeenRfidCodesByDevice' because deviceId is blank");
            return List.of();
        }
        OnlyCatDeviceRfidRequest request = new OnlyCatDeviceRfidRequest(deviceId, Math.max(1, limit));
        Map<String, Object> payload = objectMapper.convertValue(request, new TypeReference<Map<String, Object>>() {});
        log.info("Emitting read-only request: getLastSeenRfidCodesByDevice {}", payload);
        CompletableFuture<Object[]> response = new CompletableFuture<>();
        safeEmit("getLastSeenRfidCodesByDevice", payload, response::complete);
        Object[] ackArgs = awaitAck("getLastSeenRfidCodesByDevice", response);
        return parseRfidEventsFromAck(ackArgs);
    }

    @Override
    public List<OnlyCatInboundEvent> requestDeviceEvents(String deviceId, java.time.Instant from, java.time.Instant to, int limit) {
        String eventName = "getDeviceEvents";
        if (!isAllowedEmit(eventName)) {
            log.warn("Refusing to emit '{}' because it is not allowlisted", eventName);
            return List.of();
        }
        if (!StringUtils.hasText(deviceId)) {
            log.warn("Refusing to emit '{}' because deviceId is blank", eventName);
            return List.of();
        }
        int effectiveLimit = Math.max(1, limit);
        Map<String, Object> payload = new HashMap<>();
        payload.put("deviceId", deviceId);
        payload.put("limit", effectiveLimit);
        if (from != null) {
            payload.put("from", from.toString());
        }
        if (to != null) {
            payload.put("to", to.toString());
        }
        log.info("Emitting read-only request: {} {}", eventName, payload);
        CompletableFuture<Object[]> response = new CompletableFuture<>();
        safeEmit(eventName, payload, response::complete);
        Object[] ackArgs = awaitAck(eventName, response);
        return parseDeviceEventsFromAck(eventName, ackArgs);
    }

    @Override
    public List<String> requestDeviceIds() {
        String event = properties.getRequestDeviceListEvent();
        if (!StringUtils.hasText(event)) {
            log.warn("Backfill auto-discovery skipped: requestDeviceListEvent is blank");
            return List.of();
        }
        if (!isAllowedEmit(event)) {
            log.warn("Refusing to emit '{}' because it is not allowlisted", event);
            return List.of();
        }
        OnlyCatSubscribeRequest request = new OnlyCatSubscribeRequest(false);
        Map<String, Object> payload = objectMapper.convertValue(request, new TypeReference<Map<String, Object>>() {});
        log.info("Emitting read-only request: {} {}", event, payload);
        CompletableFuture<Object[]> response = new CompletableFuture<>();
        safeEmit(event, payload, response::complete);
        Object[] ackArgs = awaitAck(event, response);
        return extractDeviceIdsFromAck(ackArgs).stream().sorted().toList();
    }

    @Override
    public Optional<OnlyCatRfidProfile> requestRfidProfile(String rfidCode) {
        if (!isAllowedEmit("getRfidProfile")) {
            log.warn("Refusing to emit 'getRfidProfile' because it is not allowlisted");
            return Optional.empty();
        }
        if (!StringUtils.hasText(rfidCode)) {
            log.warn("Refusing to emit 'getRfidProfile' because rfidCode is blank");
            return Optional.empty();
        }
        OnlyCatRfidProfileRequest request = new OnlyCatRfidProfileRequest(rfidCode);
        Map<String, Object> payload = objectMapper.convertValue(request, new TypeReference<Map<String, Object>>() {});
        log.info("Emitting read-only request: getRfidProfile {}", payload);
        CompletableFuture<Object[]> response = new CompletableFuture<>();
        safeEmit("getRfidProfile", payload, response::complete);
        Object[] ackArgs = awaitAck("getRfidProfile", response);
        return parseRfidProfileFromAck(ackArgs);
    }

    private Object[] awaitAck(String event, CompletableFuture<Object[]> response) {
        try {
            return response.get(3, TimeUnit.SECONDS);
        } catch (Exception ex) {
            log.warn("Timed out waiting for ACK for '{}'", event);
            return new Object[0];
        }
    }

    private List<OnlyCatRfidEvent> parseRfidEventsFromAck(Object[] ackArgs) {
        if (ackArgs == null || ackArgs.length == 0 || ackArgs[0] == null) {
            return List.of();
        }
        List<?> list = toRfidEventList(ackArgs[0]);
        if (list == null || list.isEmpty()) {
            return List.of();
        }
        try {
            return objectMapper.convertValue(list, new TypeReference<List<OnlyCatRfidEvent>>() {});
        } catch (IllegalArgumentException ignore) {
            return List.of();
        }
    }

    private List<OnlyCatInboundEvent> parseDeviceEventsFromAck(String eventName, Object[] ackArgs) {
        if (ackArgs == null || ackArgs.length == 0 || ackArgs[0] == null) {
            return List.of();
        }
        List<?> list = toDeviceEventList(ackArgs[0]);
        if (list == null || list.isEmpty()) {
            return List.of();
        }
        List<OnlyCatInboundEvent> out = new java.util.ArrayList<>();
        logBackfillSample(eventName, list);
        for (Object item : list) {
            OnlyCatUserEventUpdatePayload payload = toUserEventPayload(item);
            if (payload == null) {
                continue;
            }
            Integer eventId = payload.effectiveEventId();
            String eventType = payload.type();
            Integer eventTriggerSourceCode = payload.effectiveEventTriggerSource();
            Integer eventClassificationCode = payload.effectiveEventClassification();
            String deviceId = payload.effectiveDeviceId();
            String timestamp = payload.effectiveTimestamp();
            Long globalId = payload.effectiveGlobalId();
            String accessToken = payload.effectiveAccessToken();

            OnlyCatEventTriggerSource eventTriggerSource = OnlyCatEventTriggerSource.fromCode(eventTriggerSourceCode);
            OnlyCatEventClassification eventClassification = OnlyCatEventClassification.fromCode(eventClassificationCode);
            Map<String, Object> backfillMeta = buildBackfillMeta(item);
            out.add(new OnlyCatInboundEvent(
                    "userEventUpdate",
                    eventId,
                    eventType,
                    eventTriggerSource,
                    eventClassification,
                    deviceId,
                    timestamp,
                    globalId,
                    accessToken,
                    new Object[]{backfillMeta, item}
            ));
        }
        if (out.isEmpty()) {
            log.info("No parseable events returned for {} ack={}", eventName, Arrays.toString(ackArgs));
        }
        return out;
    }

    private List<?> toRfidEventList(Object value) {
        if (value instanceof JSONArray arr) {
            return normalizeRfidEventList(arr.toList());
        }
        if (value instanceof JSONObject obj) {
            return List.of(obj.toMap());
        }
        if (value instanceof Map<?, ?> map) {
            return List.of(map);
        }
        if (value instanceof List<?> list) {
            return normalizeRfidEventList(list);
        }
        return null;
    }

    private List<?> toDeviceEventList(Object value) {
        if (value instanceof JSONArray arr) {
            return normalizeEventList(arr.toList());
        }
        if (value instanceof JSONObject obj) {
            return extractEventListFromMap(obj.toMap());
        }
        if (value instanceof Map<?, ?> map) {
            return extractEventListFromMap(map);
        }
        if (value instanceof List<?> list) {
            return normalizeEventList(list);
        }
        return null;
    }

    private List<?> extractEventListFromMap(Map<?, ?> map) {
        if (map == null || map.isEmpty()) {
            return List.of();
        }
        Object events = map.get("events");
        if (events instanceof List<?> list) {
            return normalizeEventList(list);
        }
        Object data = map.get("data");
        if (data instanceof List<?> list) {
            return normalizeEventList(list);
        }
        return List.of(map);
    }

    private List<?> normalizeEventList(List<?> list) {
        if (list == null || list.isEmpty()) {
            return list;
        }
        Object first = list.get(0);
        if (first instanceof List<?> nested) {
            return nested;
        }
        return list;
    }

    private void logBackfillSample(String eventName, List<?> list) {
        if (list == null || list.isEmpty()) {
            log.info("Backfill {} returned 0 items", eventName);
            return;
        }
        Object first = list.get(0);
        if (first instanceof JSONObject obj) {
            Map<String, Object> map = obj.toMap();
            log.info("Backfill {} returned {} items; first keys={}", eventName, list.size(), map.keySet());
            log.debug("Backfill {} first item={}", eventName, map);
            return;
        }
        if (first instanceof Map<?, ?> map) {
            log.info("Backfill {} returned {} items; first keys={}", eventName, list.size(), map.keySet());
            log.debug("Backfill {} first item={}", eventName, map);
            return;
        }
        log.info("Backfill {} returned {} items; first type={}", eventName, list.size(), first == null ? "null" : first.getClass().getName());
        log.debug("Backfill {} first item={}", eventName, first);
    }

    private Map<String, Object> buildBackfillMeta(Object item) {
        Map<String, Object> meta = new HashMap<>();
        meta.put("__backfill__", true);
        if (item instanceof JSONObject obj) {
            addBackfillHints(meta, obj.toMap());
        } else if (item instanceof Map<?, ?> map) {
            addBackfillHints(meta, map);
        }
        return meta;
    }

    private void addBackfillHints(Map<String, Object> meta, Map<?, ?> map) {
        if (map == null || map.isEmpty()) {
            return;
        }
        String rfidCode = firstString(map, "rfidCode", "rfid_code", "rfid");
        if (StringUtils.hasText(rfidCode)) {
            meta.put("rfidCode", rfidCode);
        }
        List<String> labels = extractLabels(map);
        if (!labels.isEmpty()) {
            meta.put("catLabels", labels);
        }
    }

    private String firstString(Map<?, ?> map, String... keys) {
        for (String key : keys) {
            Object value = map.get(key);
            if (value == null) {
                continue;
            }
            String text = String.valueOf(value);
            if (StringUtils.hasText(text)) {
                return text;
            }
        }
        return null;
    }

    private List<String> extractLabels(Map<?, ?> map) {
        Object value = map.get("catLabels");
        if (value == null) {
            value = map.get("cat_labels");
        }
        if (value instanceof List<?> list) {
            return list.stream()
                    .map(String::valueOf)
                    .filter(StringUtils::hasText)
                    .toList();
        }
        String label = firstString(map, "catLabel", "cat_label", "label");
        if (StringUtils.hasText(label)) {
            if (label.contains("|")) {
                return Arrays.stream(label.split("\\|"))
                        .map(String::trim)
                        .filter(StringUtils::hasText)
                        .toList();
            }
            return List.of(label);
        }
        return List.of();
    }

    private List<?> normalizeRfidEventList(List<?> list) {
        if (list == null || list.isEmpty()) {
            return list;
        }
        Object first = list.get(0);
        if (first instanceof List<?> nested) {
            return nested;
        }
        return list;
    }

    private OnlyCatUserEventUpdatePayload toUserEventPayload(Object value) {
        if (value == null) {
            return null;
        }
        if (value instanceof OnlyCatUserEventUpdatePayload typed) {
            return typed;
        }
        if (value instanceof JSONObject obj) {
            return objectMapper.convertValue(obj.toMap(), OnlyCatUserEventUpdatePayload.class);
        }
        if (value instanceof Map<?, ?> map) {
            return objectMapper.convertValue(map, OnlyCatUserEventUpdatePayload.class);
        }
        if (value instanceof List<?> list && list.size() == 1) {
            return toUserEventPayload(list.get(0));
        }
        return null;
    }

    private Optional<OnlyCatRfidProfile> parseRfidProfileFromAck(Object[] ackArgs) {
        if (ackArgs == null || ackArgs.length == 0 || ackArgs[0] == null) {
            return Optional.empty();
        }
        Object value = ackArgs[0];
        return toRfidProfile(value);
    }

    private Optional<OnlyCatRfidProfile> toRfidProfile(Object value) {
        if (value == null) {
            return Optional.empty();
        }
        if (value instanceof JSONObject obj) {
            return toRfidProfile(obj.toMap());
        }
        if (value instanceof Map<?, ?> map) {
            OnlyCatRfidProfile profile = convertValue(map, OnlyCatRfidProfile.class);
            return hasProfileData(profile) ? Optional.of(profile) : Optional.empty();
        }
        if (value instanceof List<?> list && list.size() == 1) {
            return toRfidProfile(list.get(0));
        }
        return Optional.empty();
    }

    private boolean hasProfileData(OnlyCatRfidProfile profile) {
        return profile != null && (StringUtils.hasText(profile.label()) || profile.userId() != null);
    }

    private <T> T convertValue(Object value, Class<T> targetType) {
        try {
            return objectMapper.convertValue(value, targetType);
        } catch (IllegalArgumentException ex) {
            return null;
        }
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
            if (ack == null) {
                socket.emit(event, payload);
            } else {
                socket.emit(event, payload, ack);
            }
        } catch (Exception e) {
            log.warn("Emit failed for '{}' payload={}", event, payload, e);
        }
    }

    private boolean isAllowedEmit(String event) {
        if (!StringUtils.hasText(event)) {
            return false;
        }
        if (ALLOWED_EMITS.contains(event)) {
            return true;
        }
        return backfillProperties.isEnabled()
                && "getDeviceEvents".equals(event);
    }

    /**
     * Extract device IDs from the first ACK payload (array of device objects or a device object).
     */
    private Set<String> extractDeviceIdsFromAck(Object[] ackArgs) {
        Set<String> out = new HashSet<>();
        if (ackArgs == null || ackArgs.length == 0) {
            return out;
        }
        Object first = ackArgs[0];
        if (first instanceof JSONArray arr) {
            for (int i = 0; i < arr.length(); i++) {
                extractDeviceIdFromObject(arr.opt(i), out);
            }
            return out;
        }
        if (first instanceof JSONObject obj) {
            if (obj.has("devices") && obj.opt("devices") instanceof JSONArray arr) {
                for (int i = 0; i < arr.length(); i++) {
                    extractDeviceIdFromObject(arr.opt(i), out);
                }
            } else {
                extractDeviceIdFromObject(obj, out);
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
        refreshExecutor.shutdownNow();
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

    private void scheduleEventRefresh(OnlyCatInboundEvent inbound) {
        if (inbound == null || inbound.eventId() == null) {
            return;
        }
        if (!"userEventUpdate".equals(inbound.eventName())) {
            return;
        }
        if (!"create".equalsIgnoreCase(inbound.eventType())) {
            return;
        }
        refreshExecutor.schedule(
                () -> emitGetEventsRefresh("post-create"),
                EVENTS_REFRESH_DELAY_MS,
                TimeUnit.MILLISECONDS
        );
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
}
