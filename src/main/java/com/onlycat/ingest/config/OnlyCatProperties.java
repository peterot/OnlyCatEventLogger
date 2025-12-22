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
}
