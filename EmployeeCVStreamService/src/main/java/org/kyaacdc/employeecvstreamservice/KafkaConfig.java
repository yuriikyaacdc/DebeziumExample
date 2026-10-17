package org.kyaacdc.employeecvstreamservice;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.LogAndContinueExceptionHandler;
import org.kyaacdc.core.OutboxDTO;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.kafka.autoconfigure.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafkaStreams;
import org.springframework.kafka.config.KafkaStreamsConfiguration;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.support.serializer.JacksonJsonDeserializer;
import org.springframework.kafka.support.serializer.JacksonJsonSerde;
import org.springframework.kafka.support.serializer.JsonSerde;

import java.util.HashMap;
import java.util.Map;

@Configuration
@EnableKafkaStreams
public class KafkaConfig {

    @Value("unknown-outbox-topic")
    private String unknownOutboxTopic;

    @Bean
    public KafkaStreamsConfiguration defaultKafkaStreamsConfig(KafkaProperties kafkaProperties) {

        var props = new HashMap<String, Object>(kafkaProperties.getStreams().getProperties());

        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "streaming-service");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaProperties.getBootstrapServers());
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, JacksonJsonSerde.class);
        props.put(StreamsConfig.DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG, LogAndContinueExceptionHandler.class);

        return new KafkaStreamsConfiguration(props);
    }

    @Bean
    public ObjectMapper outboxObjectMapper() {
        return new ObjectMapper();
    }

    @Bean
    public JsonSerde<OutboxDTO> outboxDTOJacksonJsonSerde(KafkaStreamsConfiguration kafkaStreamsConfiguration) {
        JsonSerde<OutboxDTO> outboxSerde = new JsonSerde<>(OutboxDTO.class);
        outboxSerde.deserializer().configure(
                Map.of(JacksonJsonDeserializer.TRUSTED_PACKAGES, "*",
                        JacksonJsonDeserializer.VALUE_DEFAULT_TYPE, OutboxDTO.class),
                false
        );

        return outboxSerde;
    }


    @Bean
    NewTopic createOutboxTopic() {
        return TopicBuilder.name("cv-app.public.outbox")
                .partitions(3)
                .replicas(3)
                .configs(Map.of("min.insync.replicas", "2"))
                .build();
    }
    @Bean
    NewTopic createUnknownOutboxTopic() {
        return TopicBuilder.name("unknown-outbox-topic")
                .partitions(3)
                .replicas(3)
                .configs(Map.of("min.insync.replicas", "2"))
                .build();
    }
    @Bean
    NewTopic createCvTopic() {
        return TopicBuilder.name("cv-topic")
                .partitions(3)
                .replicas(3)
                .configs(Map.of("min.insync.replicas", "2"))
                .build();
    }
}
