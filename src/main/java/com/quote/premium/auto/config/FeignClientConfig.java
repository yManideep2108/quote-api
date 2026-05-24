package com.quote.premium.auto.config;

import feign.RequestInterceptor;
import org.slf4j.MDC;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class FeignClientConfig {

    @Bean
    public RequestInterceptor requestInterceptor() {

        return requestTemplate -> {

            String correlationId =
                    MDC.get("correlationId");

            if (correlationId != null) {
                requestTemplate.header(
                        "correlationId",
                        correlationId
                );
            }
        };
    }
}