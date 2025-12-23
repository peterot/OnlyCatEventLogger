package com.onlycat.ingest.config;

import jakarta.validation.constraints.NotBlank;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

@Validated
@ConfigurationProperties(prefix = "onlycat")
public class OnlyCatProperties {

    /**
     * Socket.IO gateway URL.
     */
    private String gatewayUrl = "https://gateway.onlycat.com";

    /**
     * Auth token copied from the OnlyCat mobile app.
     */
    @NotBlank
    private String token;

    /**
     * Optional smoke-test event name to request device list or similar.
     */
    private String requestDeviceListEvent = "getDevices";

    /**
     * Namespace to connect to (e.g., "/", "/catflap"). Default is root.
     */
    private String namespace = "/";

    /**
     * Optional list of subscription events to emit after connect (no payload).
     */
    private java.util.List<String> subscribeEvents = java.util.List.of();

    /**
     * Client platform header (mirrors Home Assistant client).
     */
    private String platform = "onlycat-java";

    /**
     * Client device header (mirrors Home Assistant client).
     */
    private String device = "onlycat-event-logger";

    public String getGatewayUrl() {
        return gatewayUrl;
    }

    public void setGatewayUrl(String gatewayUrl) {
        this.gatewayUrl = gatewayUrl;
    }

    public String getToken() {
        return token;
    }

    public void setToken(String token) {
        this.token = token;
    }

    public String getRequestDeviceListEvent() {
        return requestDeviceListEvent;
    }

    public void setRequestDeviceListEvent(String requestDeviceListEvent) {
        this.requestDeviceListEvent = requestDeviceListEvent;
    }

    public String getNamespace() {
        return namespace == null || namespace.isBlank() ? "/" : namespace;
    }

    public void setNamespace(String namespace) {
        this.namespace = namespace;
    }

    public java.util.List<String> getSubscribeEvents() {
        return subscribeEvents;
    }

    public void setSubscribeEvents(java.util.List<String> subscribeEvents) {
        this.subscribeEvents = subscribeEvents;
    }

    public String getPlatform() {
        return platform;
    }

    public void setPlatform(String platform) {
        this.platform = platform;
    }

    public String getDevice() {
        return device;
    }

    public void setDevice(String device) {
        this.device = device;
    }
}
