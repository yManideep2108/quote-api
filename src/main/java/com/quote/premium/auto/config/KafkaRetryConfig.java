package com.quote.premium.auto.config;

import com.common.avro.QuoteEvent;
import com.common.avro.QuoteResponseEvent;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.*;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.util.backoff.FixedBackOff;

import java.util.HashMap;
import java.util.Map;

@Configuration
public class KafkaRetryConfig {

    // =========================================================
    // COMMON PRODUCER CONFIG
    // =========================================================

    private Map<String, Object> commonProducerConfigs() {

        Map<String, Object> config = new HashMap<>();

        config.put(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
                "kafka-broker:9092"
        );

        config.put(
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                StringSerializer.class
        );

        config.put(
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                KafkaAvroSerializer.class
        );

        config.put(
                "schema.registry.url",
                "http://schema-registry:8081"
        );

        return config;
    }

    // =========================================================
    // GENERIC PRODUCER FACTORY (DLQ / RETRY)
    // =========================================================

    @Bean
    public ProducerFactory<Object, Object> producerFactory() {

        return new DefaultKafkaProducerFactory<>(
                commonProducerConfigs()
        );
    }

    // =========================================================
    // GENERIC TEMPLATE (DLQ / RETRY ONLY)
    // =========================================================

    @Bean
    @Primary
    public KafkaTemplate<Object, Object> kafkaTemplate(
            ProducerFactory<Object, Object> producerFactory) {

        return new KafkaTemplate<>(producerFactory);
    }

    // =========================================================
    // QUOTE RESPONSE PRODUCER FACTORY
    // =========================================================

    @Bean
    public ProducerFactory<String, QuoteResponseEvent>
    quoteResponseProducerFactory() {

        return new DefaultKafkaProducerFactory<>(
                commonProducerConfigs()
        );
    }

    // =========================================================
    // QUOTE RESPONSE TEMPLATE
    // =========================================================

    @Bean
    public KafkaTemplate<String, QuoteResponseEvent>
    quoteResponseKafkaTemplate() {

        return new KafkaTemplate<>(
                quoteResponseProducerFactory()
        );
    }

    // =========================================================
    // DLQ + RETRY ERROR HANDLER
    // =========================================================

    @Bean
    public DefaultErrorHandler errorHandler(
            KafkaTemplate<Object, Object> kafkaTemplate) {

        DeadLetterPublishingRecoverer recoverer =
                new DeadLetterPublishingRecoverer(
                        kafkaTemplate,
                        (record, ex) ->
                                new TopicPartition(
                                        "quote-topic-dlq",
                                        record.partition()
                                )
                );

        FixedBackOff backOff =
                new FixedBackOff(2000L, 3);

        DefaultErrorHandler handler =
                new DefaultErrorHandler(
                        recoverer,
                        backOff
                );

        handler.setRetryListeners(
                (record, ex, deliveryAttempt) -> {

                    System.out.println(
                            "🔥 Retry Attempt: "
                                    + deliveryAttempt
                    );
                }
        );

        handler.setCommitRecovered(true);

        handler.setSeekAfterError(false);

        return handler;
    }

    // =========================================================
    // LISTENER FACTORY
    // =========================================================

    @Bean(name = "kafkaListenerContainerFactory")
    public ConcurrentKafkaListenerContainerFactory<
            String,
            QuoteEvent
            > kafkaListenerContainerFactory(

            ConsumerFactory<String, QuoteEvent> consumerFactory,

            DefaultErrorHandler errorHandler) {

        ConcurrentKafkaListenerContainerFactory<
                String,
                QuoteEvent
                > factory =
                new ConcurrentKafkaListenerContainerFactory<>();

        factory.setConsumerFactory(consumerFactory);

        factory.setCommonErrorHandler(errorHandler);

        return factory;
    }
}