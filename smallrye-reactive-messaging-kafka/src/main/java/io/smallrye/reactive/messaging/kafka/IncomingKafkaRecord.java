package io.smallrye.reactive.messaging.kafka;

import java.time.Instant;
import java.util.concurrent.CompletionStage;
import java.util.function.Supplier;

import org.apache.kafka.common.header.Headers;
import org.eclipse.microprofile.reactive.messaging.Metadata;

import io.smallrye.reactive.messaging.ce.CloudEventMetadata;
import io.smallrye.reactive.messaging.kafka.fault.KafkaFailureHandler;
import io.smallrye.reactive.messaging.kafka.impl.ce.IncomingKafkaCloudEventHelper;
import io.vertx.mutiny.kafka.client.consumer.KafkaConsumer;
import io.vertx.mutiny.kafka.client.consumer.KafkaConsumerRecord;

public class IncomingKafkaRecord<K, T> implements KafkaRecord<K, T> {

    private final KafkaConsumer<K, T> consumer;
    private final Metadata metadata;
    private final IncomingKafkaRecordMetadata<K, T> kafkaMetadata;
    private final KafkaFailureHandler onNack;
    private final T payload;

    public IncomingKafkaRecord(KafkaConsumer<K, T> consumer, KafkaConsumerRecord<K, T> record,
            KafkaFailureHandler onNack, boolean cloudEventEnabled) {
        this.consumer = consumer;
        this.kafkaMetadata = new IncomingKafkaRecordMetadata<>(record);

        Metadata metadata = null;
        T payload = null;
        boolean payloadSet = false;
        if (cloudEventEnabled) {
            // Cloud Event detection
            IncomingKafkaCloudEventHelper.CloudEventMode mode = IncomingKafkaCloudEventHelper.getCloudEventMode(record);
            switch (mode) {
                case NOT_A_CLOUD_EVENT:
                    metadata = Metadata.of(this.kafkaMetadata);
                    break;
                case STRUCTURED:
                    CloudEventMetadata<T> event = IncomingKafkaCloudEventHelper
                            .createFromStructuredCloudEvent(record);
                    metadata = Metadata.of(this.kafkaMetadata,
                            event);
                    payloadSet = true;
                    payload = event.getData();
                    break;
                case BINARY:
                    metadata = Metadata
                            .of(this.kafkaMetadata, IncomingKafkaCloudEventHelper.createFromBinaryCloudEvent(record));
                    break;
            }
        } else {
            metadata = Metadata.of(this.kafkaMetadata);
        }

        this.metadata = metadata;
        this.onNack = onNack;
        if (payload == null && !payloadSet) {
            this.payload = record.value();
        } else {
            this.payload = payload;
        }
    }

    @Override
    public T getPayload() {
        return payload;
    }

    @Override
    public K getKey() {
        return kafkaMetadata.getKey();
    }

    @Override
    public String getTopic() {
        return kafkaMetadata.getTopic();
    }

    @Override
    public int getPartition() {
        return kafkaMetadata.getPartition();
    }

    @Override
    public Instant getTimestamp() {
        return kafkaMetadata.getTimestamp();
    }

    @Override
    public Headers getHeaders() {
        return kafkaMetadata.getHeaders();
    }

    public long getOffset() {
        return kafkaMetadata.getOffset();
    }

    @Override
    public Metadata getMetadata() {
        return metadata;
    }

    @Override
    public Supplier<CompletionStage<Void>> getAck() {
        return this::ack;
    }

    @Override
    public CompletionStage<Void> ack() {
        return consumer.commit().subscribeAsCompletionStage();
    }

    @Override
    public CompletionStage<Void> nack(Throwable reason) {
        return onNack.handle(this, reason);
    }
}
