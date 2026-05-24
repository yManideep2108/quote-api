package com.quote.premium.auto.client;

import com.quote.premium.auto.config.FeignClientConfig;
import com.quote.premium.auto.dto.RiskResponse;
import org.springframework.cloud.openfeign.FeignClient;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;

import java.util.List;

@FeignClient(name = "risk-service" , url = "http://risk-service:8083", configuration = FeignClientConfig.class)
public interface RiskClient {
    @PostMapping("/risk")
    RiskResponse getRisk(@RequestBody List<String> vin) ;
}
