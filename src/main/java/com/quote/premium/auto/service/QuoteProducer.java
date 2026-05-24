package com.quote.premium.auto.service;

import com.common.avro.QuoteResponseEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Service
@RequiredArgsConstructor
@Slf4j
public class QuoteProducer {

    private final KafkaTemplate<String, QuoteResponseEvent>
            quoteResponseKafkaTemplate;

    public void sendQuoteResponse(
            QuoteResponseEvent responseEvent) {

        log.info(
                "🔥 Sending QuoteResponseEvent to policy-topic: {}",
                responseEvent
        );

        quoteResponseKafkaTemplate.send(
                "policy-topic",
                responseEvent
        );

        log.info("✅ Sent successfully");
    }
}