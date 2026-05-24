package com.quote.premium.auto.service;

import com.common.avro.QuoteEvent;
import com.common.avro.QuoteResponseEvent;
import com.quote.premium.auto.dto.QuoteResponse;
import com.quote.premium.auto.entity.ProcessedEvent;
import com.quote.premium.auto.repository.ProcessedEventRepository;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;


@Service
public class KafkaConsumerService {

    private static final Logger log =
            LoggerFactory.getLogger(KafkaConsumerService.class);

    private static final String CORRELATION_ID = "correlationId";

    @Autowired
    QuoteService quoteService;

    @Autowired
    QuoteProducer quoteProducer;

    @Autowired
    private ProcessedEventRepository repository;

    @Autowired
    private KafkaTemplate<Object, Object> retryProducer;

    @Autowired
    private KafkaTemplate<Object, Object> dlqProducer;

    private final Counter kafkaRetryCounter;

    @Autowired
    public KafkaConsumerService(MeterRegistry meterRegistry) {

        this.kafkaRetryCounter =
                Counter.builder("kafka.retry.count")
                        .description("Total Kafka retries")
                        .register(meterRegistry);
    }

    @KafkaListener(
            topics = "quote-topic",
            groupId = "quote-group",
            containerFactory = "kafkaListenerContainerFactory"
    )
    public void consume(ConsumerRecord<String, QuoteEvent> record) {

        // 🔥 Step 1: Read correlationId from Kafka headers
        Header header =
                record.headers().lastHeader(CORRELATION_ID);

        String correlationId = null;

        if (header != null) {
            correlationId = new String(header.value());
        }

        // 🔥 Step 2: Put into MDC
        MDC.put(CORRELATION_ID, correlationId);

        try {

            QuoteEvent event = record.value();

            Long policyId = event.getPolicyId();

            log.info("Received quote event for policyId={}", policyId);

            // 🔥 Step 3: Idempotency check
            if (repository.existsById(policyId)) {
                log.warn("Duplicate message skipped for policyId={}", policyId);
                return;
            }

            // 🔥 Step 4: Business processing
            log.info("Processing quote event");
//             if (true){ throw new RuntimeException("Simulated failure"); }
// call quote service
            QuoteResponse response =
                    quoteService.getQuote(
                            event.getVins()
                                    .stream()
                                    .map(Object::toString)
                                    .toList()
                    );

// 🔥 Build response event
            QuoteResponseEvent responseEvent =
                    QuoteResponseEvent.newBuilder()
                            .setPolicyId(policyId)
                            .setTotalPremium(response.getTotalPremium())
                            .setStatus(response.getStatus().toString())
                            .build();

// 🔥 Send response back to policy service
            quoteProducer.sendQuoteResponse(responseEvent);

            log.info("Quote response sent to policy-topic");

            // 🔥 Step 5: Mark success
            repository.save(new ProcessedEvent(policyId));

            log.info("Successfully processed event");

        } catch (Exception ex) {

            QuoteEvent event = record.value();

            int retryCount = event.getRetryCount();

            log.error(
                    "Error processing event. retryCount={}",
                    retryCount,
                    ex
            );

            // 🔥 Retry logic
            if (retryCount == 0) {

                event.setRetryCount(1);

                log.warn("Sending event to retry topic 1");
                kafkaRetryCounter.increment();
                retryProducer.send("quote-topic-retry-1", event);

            } else if (retryCount == 1) {

                event.setRetryCount(2);

                log.warn("Sending event to retry topic 2");
                kafkaRetryCounter.increment();
                retryProducer.send("quote-topic-retry-2", event);

            } else {

                log.error("Sending event to DLQ");

                dlqProducer.send("quote-topic-dlq", event);
            }

        } finally {

            // 🔥 VERY IMPORTANT
            MDC.clear();
        }
    }
}