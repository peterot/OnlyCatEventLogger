package com.onlycat.ingest;

import com.corundumstudio.socketio.AckRequest;
import com.corundumstudio.socketio.Configuration;
import com.corundumstudio.socketio.SocketIOServer;
import com.onlycat.ingest.model.OnlyCatEvent;
import com.onlycat.ingest.sheets.GoogleSheetsAppender;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.mockito.Mockito;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

import java.io.IOException;
import java.net.ServerSocket;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;

@SpringBootTest
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class SocketIoEndToEndIntegrationTest {

    private static final int PORT = findFreePort();
    private static final CountDownLatch connected = new CountDownLatch(1);
    private static final SocketIOServer SERVER = startServer();

    @MockBean
    private GoogleSheetsAppender sheetsAppender;

    @DynamicPropertySource
    static void registerProperties(DynamicPropertyRegistry registry) {
        registry.add("onlycat.gatewayUrl", () -> "http://localhost:" + PORT);
        registry.add("onlycat.namespace", () -> "/");
        registry.add("onlycat.token", () -> "test-token");
        registry.add("onlycat.requestDeviceListEvent", () -> "");
    }

    @AfterAll
    void shutdownServer() {
        SERVER.stop();
    }

    @Test
    void emitsSocketEventAndAppendsEnrichedRow() throws Exception {
        CountDownLatch appended = new CountDownLatch(1);
        AtomicReference<OnlyCatEvent> captured = new AtomicReference<>();

        Mockito.doAnswer(invocation -> {
            OnlyCatEvent event = invocation.getArgument(0);
            if (event.rawJson() != null && event.rawJson().contains("\"catLabel\"")) {
                captured.set(event);
                appended.countDown();
            }
            return null;
        }).when(sheetsAppender).append(any(OnlyCatEvent.class));

        assertThat(connected.await(5, TimeUnit.SECONDS)).isTrue();

        Map<String, Object> body = new HashMap<>();
        body.put("eventId", 1001);
        body.put("eventTriggerSource", "2");
        body.put("eventClassification", 0);
        body.put("globalId", 900001);
        body.put("accessToken", "test-token");
        body.put("deviceId", "OC-TEST-DEVICE-1");
        body.put("timestamp", "2026-01-18T22:29:59.000Z");

        Map<String, Object> payload = new HashMap<>();
        payload.put("eventId", 1001);
        payload.put("type", "create");
        payload.put("body", body);
        payload.put("deviceId", "OC-TEST-DEVICE-1");

        SERVER.getBroadcastOperations().sendEvent("userEventUpdate", payload);

        assertThat(appended.await(5, TimeUnit.SECONDS)).isTrue();

        OnlyCatEvent event = captured.get();
        assertThat(event).isNotNull();
        assertThat(event.eventType()).isEqualTo("create");
        assertThat(event.deviceId()).isEqualTo("OC-TEST-DEVICE-1");
        assertThat(event.eventTimeUtc()).isEqualTo(Instant.parse("2026-01-18T22:29:59Z"));
        assertThat(event.rawJson()).contains("\"catLabel\":\"Cleo\"");
    }

    private static SocketIOServer startServer() {
        Configuration config = new Configuration();
        config.setHostname("localhost");
        config.setPort(PORT);

        SocketIOServer server = new SocketIOServer(config);
        server.addConnectListener(client -> connected.countDown());

        server.addEventListener("getLastSeenRfidCodesByDevice", Object.class,
                (client, data, ackRequest) -> sendRfidAck(ackRequest));
        server.addEventListener("getRfidProfile", Object.class,
                (client, data, ackRequest) -> sendProfileAck(ackRequest));

        server.start();
        return server;
    }

    private static void sendRfidAck(AckRequest ackRequest) {
        if (!ackRequest.isAckRequested()) {
            return;
        }
        Map<String, Object> match = new HashMap<>();
        match.put("eventId", 1001);
        match.put("rfidCode", "TEST-RFID-0001");
        match.put("deviceId", "OC-TEST-DEVICE-1");
        match.put("timestamp", "2026-01-18T22:29:59.000Z");
        ackRequest.sendAckData(List.of(match));
    }

    private static void sendProfileAck(AckRequest ackRequest) {
        if (!ackRequest.isAckRequested()) {
            return;
        }
        Map<String, Object> profile = new HashMap<>();
        profile.put("createdAt", "2025-03-31T18:19:00.000Z");
        profile.put("label", "Cleo");
        profile.put("userId", 1001);
        profile.put("updatedAt", "2025-03-31T18:19:00.000Z");
        ackRequest.sendAckData(profile);
    }

    private static int findFreePort() {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        } catch (IOException e) {
            throw new IllegalStateException("Unable to find free port", e);
        }
    }
}
