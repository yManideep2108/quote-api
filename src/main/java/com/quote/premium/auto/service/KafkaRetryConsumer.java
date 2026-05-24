package com.quote.premium.auto.service;

import com.common.avro.QuoteEvent;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Service
public class KafkaRetryConsumer {

    @Autowired
    private KafkaTemplate<Object, Object> retryProducer;

    @Autowired
    private KafkaTemplate<Object, Object> dlqProducer;
    @KafkaListener(
            topics = "quote-topic-retry-1",
            groupId = "retry-group-1"
    )
    public void retry1(QuoteEvent event) {

        try {
            System.out.println("⏳ Retry-1 processing: " + event);

            Thread.sleep(5000); // 5 sec delay

            // simulate failure again
            throw new RuntimeException("Retry-1 failed");

        } catch (Exception ex) {
            event.setRetryCount(2);
            retryProducer.send("quote-topic-retry-2", event);
        }
    }

    @KafkaListener(
            topics = "quote-topic-retry-2",
            groupId = "retry-group-2"
    )
    public void retry2(QuoteEvent event) {

        try {
            System.out.println("⏳ Retry-2 processing: " + event);

            Thread.sleep(10000);

            throw new RuntimeException("Retry-2 failed");

        } catch (Exception ex) {

            System.out.println("📦 Sending to DLQ: " + event); // 🔥 ADD THIS

            dlqProducer.send("quote-topic-dlq", event);
        }
    }
}
