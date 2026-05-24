package com.quote.premium.auto.service;

import com.common.avro.QuoteEvent;
import com.quote.premium.auto.client.RiskClient;
import com.quote.premium.auto.dto.QuoteResponse;
import com.quote.premium.auto.dto.QuoteStatus;
import com.quote.premium.auto.dto.RiskResponse;
import io.github.resilience4j.circuitbreaker.annotation.CircuitBreaker;
import io.github.resilience4j.retry.annotation.Retry;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
public class QuoteService {

    @Autowired
    private RiskClient riskClient;

    @Retry(name = "riskRetry", fallbackMethod = "fallbackQuote")
    @CircuitBreaker(name = "riskCB", fallbackMethod = "fallbackQuote")
    public QuoteResponse getQuote(List<String> vins) {

        double totalPremium = 0;
        boolean isRejected = false;
        char lastChar ;
        int digit ;

        System.out.println("👉 Calling Risk Service...");
        RiskResponse risk = riskClient.getRisk(vins);
        for (String vin : vins) {
            lastChar = '0';
            digit = 0;
            lastChar = vin.charAt(vin.length() - 1);
            digit = Character.getNumericValue(lastChar);
            if (digit  %2 == 0 ) {
                totalPremium += 3000;
            } else {
                totalPremium += 1500;
            }
        }

        QuoteResponse response = new QuoteResponse();
        response.setTotalPremium(totalPremium);

        if (risk.getRiskScore() > 50 ) {
            response.setStatus(QuoteStatus.REJECTED);
        } else {
            response.setStatus(QuoteStatus.ACTIVE);
        }
        return response;
    }

    public QuoteResponse fallbackQuote(List<String> vins, Exception ex) {

       // log.error("⚠️ Fallback triggered. Reason: {}", ex.getClass().getSimpleName());

        QuoteResponse response = new QuoteResponse();
        response.setTotalPremium(0.0);
        response.setStatus(QuoteStatus.PENDING); // 🔥 KEY
        return response;
    }
}