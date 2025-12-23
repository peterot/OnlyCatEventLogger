package com.onlycat.ingest.onlycat;

import com.onlycat.ingest.config.OnlyCatProperties;
import com.onlycat.ingest.service.EventIngestService;
import io.socket.client.IO;
import io.socket.client.Socket;
import io.socket.client.Manager;
import io.socket.emitter.Emitter;
import io.socket.parser.Packet;
import org.json.JSONArray;
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
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

@Component
public class SocketIoOnlyCatClient implements OnlyCatClient, ApplicationRunner, AutoCloseable {
    private static final Logger log = LoggerFactory.getLogger(SocketIoOnlyCatClient.class);
    private final OnlyCatProperties properties;
    private final EventIngestService ingestService;
    private final AtomicInteger sampleLogged = new AtomicInteger();
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
            socket = IO.socket(properties.getGatewayUrl(), options);
        } catch (Exception e) {
            throw new IllegalStateException("Unable to create Socket.IO client", e);
        }

        socket.on(Socket.EVENT_CONNECT, args -> {
            log.info("Connected to OnlyCat gateway");
            emitSmokeTest();
        });
        socket.on(Socket.EVENT_CONNECT_ERROR, args -> log.warn("Connection error: {}", Arrays.toString(args)));
        socket.on(Socket.EVENT_DISCONNECT, args -> log.warn("Disconnected: {}", Arrays.toString(args)));
        socket.on("reconnect", args -> log.info("Reconnected"));
        socket.on("reconnecting", args -> log.info("Reconnecting..."));
        socket.on("error", args -> log.error("Socket error: {}", Arrays.toString(args)));
        socket.on("message", args -> handleAnyEvent("message", args));

        registerCatchAll(socket);
        registerPacketInterceptor(socket);

        socket.connect();
        log.info("Connecting to {} (token redacted)", properties.getGatewayUrl());
    }

    private IO.Options buildOptions() {
        IO.Options opts = new IO.Options();
        opts.reconnection = true;
        opts.reconnectionAttempts = Integer.MAX_VALUE;
        opts.reconnectionDelay = 1000;
        opts.reconnectionDelayMax = 10_000;
        opts.timeout = 20_000;
        opts.transports = new String[]{"websocket"};
        // Pass token without logging it.
        if (StringUtils.hasText(properties.getToken())) {
            opts.query = "token=" + URLEncoder.encode(properties.getToken(), StandardCharsets.UTF_8);
            Map<String, List<String>> headers = new HashMap<>();
            headers.put("Authorization", List.of("Bearer " + properties.getToken()));
            opts.extraHeaders = headers;
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
            log.info("Registered catch-all Socket.IO handler");
        } catch (Exception ex) {
            log.info("Catch-all not available ({}) - registering basic handlers", ex.getMessage());
            for (String event : List.of("connect", "authenticated", "device_event", "cat_event")) {
                socket.on(event, args -> handleAnyEvent(event, args));
            }
        }
    }

    private void registerPacketInterceptor(Socket socket) {
        // Fallback: inspect low-level packets to capture all event names/payloads.
        socket.io().on(Manager.EVENT_PACKET, args -> {
            if (args == null || args.length == 0 || !(args[0] instanceof Packet packet)) {
                return;
            }
            if (packet.type == io.socket.parser.Parser.EVENT || packet.type == io.socket.parser.Parser.BINARY_EVENT) {
                if (packet.data instanceof JSONArray array && array.length() >= 1) {
                    String eventName = array.optString(0, "unknown");
                    Object[] payload = new Object[Math.max(0, array.length() - 1)];
                    for (int i = 1; i < array.length(); i++) {
                        payload[i - 1] = array.opt(i);
                    }
                    handleAnyEvent(eventName, payload);
                }
            }
        });
    }

    private void handleAnyEvent(String event, Object[] args) {
        if (sampleLogged.getAndIncrement() < 2) {
            log.info("Sample inbound event {} payload={}", event, Arrays.toString(args));
        } else {
            log.debug("Inbound event {} payload={}", event, Arrays.toString(args));
        }
        ingestService.handleInbound(event, args);
    }

    private void emitSmokeTest() {
        String event = properties.getRequestDeviceListEvent();
        if (!StringUtils.hasText(event)) {
            return;
        }
        log.info("Emitting smoke-test event '{}'", event);
        socket.emit(event);
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
