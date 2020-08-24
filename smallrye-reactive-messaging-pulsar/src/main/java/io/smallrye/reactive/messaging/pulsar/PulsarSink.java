package io.smallrye.reactive.messaging.pulsar;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.pulsar.client.api.*;
import org.apache.pulsar.client.impl.ProducerBuilderImpl;
import org.apache.pulsar.client.impl.PulsarClientImpl;
import org.eclipse.microprofile.config.Config;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.Metadata;
import org.eclipse.microprofile.reactive.streams.operators.ReactiveStreams;
import org.eclipse.microprofile.reactive.streams.operators.SubscriberBuilder;

/**
 * @author Sherwin Pinto
 */
public class PulsarSink<T> {
    private static final Set<String> ALLOWABLE_PRODUCER_PROPERTIES = new HashSet<>();
    private final PulsarClient pulsarClient;
    private final PulsarConnectorOutgoingConfiguration pcoc;

    static {
        ALLOWABLE_PRODUCER_PROPERTIES.add("topicName");
        ALLOWABLE_PRODUCER_PROPERTIES.add("producerName");
        ALLOWABLE_PRODUCER_PROPERTIES.add("sendTimeoutMs");
        ALLOWABLE_PRODUCER_PROPERTIES.add("blockIfQueueFull");
        ALLOWABLE_PRODUCER_PROPERTIES.add("maxPendingMessages");
        ALLOWABLE_PRODUCER_PROPERTIES.add("maxPendingMessagesAcrossPartitions");
        ALLOWABLE_PRODUCER_PROPERTIES.add("messageRoutingMode");
        ALLOWABLE_PRODUCER_PROPERTIES.add("hashingScheme");
        ALLOWABLE_PRODUCER_PROPERTIES.add("cryptoFailureAction");
        ALLOWABLE_PRODUCER_PROPERTIES.add("customMessageRouter");
        ALLOWABLE_PRODUCER_PROPERTIES.add("batchingMaxPublishDelayMicros");
        ALLOWABLE_PRODUCER_PROPERTIES.add("batchingPartitionSwitchFrequencyByPublishDelay");
        ALLOWABLE_PRODUCER_PROPERTIES.add("batchingMaxMessages");
        ALLOWABLE_PRODUCER_PROPERTIES.add("batchingMaxBytes");
        ALLOWABLE_PRODUCER_PROPERTIES.add("batchingEnabled");
        ALLOWABLE_PRODUCER_PROPERTIES.add("batcherBuilder");
        ALLOWABLE_PRODUCER_PROPERTIES.add("chunkingEnabled");
        ALLOWABLE_PRODUCER_PROPERTIES.add("cryptoKeyReader");
        ALLOWABLE_PRODUCER_PROPERTIES.add("messageCrypto");
        ALLOWABLE_PRODUCER_PROPERTIES.add("encryptionKeys");
        ALLOWABLE_PRODUCER_PROPERTIES.add("compressionType");
        ALLOWABLE_PRODUCER_PROPERTIES.add("initialSequenceId");
        ALLOWABLE_PRODUCER_PROPERTIES.add("autoUpdatePartitions");
        ALLOWABLE_PRODUCER_PROPERTIES.add("multiSchema");
        ALLOWABLE_PRODUCER_PROPERTIES.add("properties");
    }

    public PulsarSink(PulsarClient pulsarClient, PulsarConnectorOutgoingConfiguration pcoc) {
        this.pulsarClient = pulsarClient;
        this.pcoc = pcoc;
    }

    protected SubscriberBuilder<? extends Message<T>, Void> sink() {
        Producer<T> pulsarProducer = createPulsarProducer();
        return ReactiveStreams.<Message<T>> builder()
                .flatMapCompletionStage(message -> {
                    T payload = message.getPayload();
                    //TODO: Add Schema handling
                    return pulsarProducer.newMessage(Schema.BYTES)
                            .properties(getPropertiesFromMetada(message.getMetadata()))
                            .sendAsync();
                }).onError(throwable -> {
                    //TODO: Implement logging/error handling
                    throwable.printStackTrace();
                }).ignore();
    }

    private Producer<T> createPulsarProducer() {
        Producer<T> pulsarProducer = null;
        try {
            pulsarProducer = producerBuilder()
                    .create();
        } catch (Exception e) {
            //TODO: Add Exception Handling
            e.printStackTrace();
        }
        return pulsarProducer;
    }

    private Map<String, String> getPropertiesFromMetada(Metadata metadata) {
        Map<String, String> messagePropertierMap = new HashMap<>();
        metadata.iterator().forEachRemaining(o -> {

        });
        return messagePropertierMap;
    }

    private ProducerBuilder<T> producerBuilder() {
        final Map<String, Object> configMap = new HashMap<>();
        Config config = pcoc.config();
        pcoc.config().getPropertyNames()
                .forEach(e -> {
                    if (ALLOWABLE_PRODUCER_PROPERTIES.contains(e)) {
                        Object value = config.getValue(e, e.getClass());
                        configMap.put(e, value);
                    }
                });

        ProducerBuilder<T> producerBuilder = new ProducerBuilderImpl((PulsarClientImpl) pulsarClient, Schema.BYTES);
        producerBuilder.loadConf(configMap);
        return producerBuilder;
    }
}
