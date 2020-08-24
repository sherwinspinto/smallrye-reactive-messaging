package io.smallrye.reactive.messaging.pulsar;

import java.util.*;
import java.util.concurrent.CompletionStage;

import org.apache.pulsar.client.api.*;
import org.apache.pulsar.client.impl.ConsumerBuilderImpl;
import org.apache.pulsar.client.impl.PulsarClientImpl;
import org.eclipse.microprofile.config.Config;
import org.eclipse.microprofile.reactive.streams.operators.PublisherBuilder;
import org.eclipse.microprofile.reactive.streams.operators.ReactiveStreams;

import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.infrastructure.Infrastructure;
import io.smallrye.mutiny.subscription.MultiEmitter;

/**
 * @author Sherwin Pinto
 */
public class PulsarSource<T> {
    private final PulsarClient pulsarClient;
    private final PulsarConnectorIncomingConfiguration pcic;
    private static final Set<String> ALLOWABLE_PULSAR_CONSUMER_PROPS = new HashSet<>();
    private static final String SPECIAL_TYPE_FIELD_NAME_TOPICNAMES = "topicNames";
    private static final String IN = "";
    private static final String OUT = "";

    public static enum SCHEMA_TYPE {
        AVRO,
        PROTOBUF,
        JSON,
        BYTES;

        private SCHEMA_TYPE() {
        }
    }

    static {
        ALLOWABLE_PULSAR_CONSUMER_PROPS.add("topicNames");
        ALLOWABLE_PULSAR_CONSUMER_PROPS.add("topicsPattern");
        ALLOWABLE_PULSAR_CONSUMER_PROPS.add("subscriptionName");
        ALLOWABLE_PULSAR_CONSUMER_PROPS.add("subscriptionType");
        ALLOWABLE_PULSAR_CONSUMER_PROPS.add("subscriptionMode");
        ALLOWABLE_PULSAR_CONSUMER_PROPS.add("receiverQueueSize");
        ALLOWABLE_PULSAR_CONSUMER_PROPS.add("acknowledgementsGroupTimeMicros");
        ALLOWABLE_PULSAR_CONSUMER_PROPS.add("negativeAckRedeliveryDelayMicros");
        ALLOWABLE_PULSAR_CONSUMER_PROPS.add("maxTotalReceiverQueueSizeAcrossPartitions");
        ALLOWABLE_PULSAR_CONSUMER_PROPS.add("consumerName");
        ALLOWABLE_PULSAR_CONSUMER_PROPS.add("ackTimeoutMillis");
        ALLOWABLE_PULSAR_CONSUMER_PROPS.add("tickDurationMillis");
        ALLOWABLE_PULSAR_CONSUMER_PROPS.add("priorityLevel");
        ALLOWABLE_PULSAR_CONSUMER_PROPS.add("maxPendingChuckedMessage");
        ALLOWABLE_PULSAR_CONSUMER_PROPS.add("autoAckOldestChunkedMessageOnQueueFull");
        ALLOWABLE_PULSAR_CONSUMER_PROPS.add("expireTimeOfIncompleteChunkedMessageMillis");
        ALLOWABLE_PULSAR_CONSUMER_PROPS.add("cryptoKeyReader");
        ALLOWABLE_PULSAR_CONSUMER_PROPS.add("messageCrypto");
        ALLOWABLE_PULSAR_CONSUMER_PROPS.add("cryptoFailureAction");
        ALLOWABLE_PULSAR_CONSUMER_PROPS.add("properties");
        ALLOWABLE_PULSAR_CONSUMER_PROPS.add("readCompacted");
        ALLOWABLE_PULSAR_CONSUMER_PROPS.add("subscriptionInitialPosition");
        ALLOWABLE_PULSAR_CONSUMER_PROPS.add("patternAutoDiscoveryPeriod");
        ALLOWABLE_PULSAR_CONSUMER_PROPS.add("regexSubscriptionMode");
        ALLOWABLE_PULSAR_CONSUMER_PROPS.add("deadLetterPolicy");
        ALLOWABLE_PULSAR_CONSUMER_PROPS.add("retryEnable");
        ALLOWABLE_PULSAR_CONSUMER_PROPS.add("batchReceivePolicy");
        ALLOWABLE_PULSAR_CONSUMER_PROPS.add("autoUpdatePartitions");
        ALLOWABLE_PULSAR_CONSUMER_PROPS.add("replicateSubscriptionState");
        ALLOWABLE_PULSAR_CONSUMER_PROPS.add("resetIncludeHead");
        ALLOWABLE_PULSAR_CONSUMER_PROPS.add("keySharedPolicy");
    }

    protected PulsarSource(PulsarClient pulsarClient, PulsarConnectorIncomingConfiguration pcic) {
        this.pulsarClient = pulsarClient;
        this.pcic = pcic;
    }

    protected PublisherBuilder<IncomingPulsarMessage<T>> createPublisher() {
        return ReactiveStreams.<IncomingPulsarMessage<T>> fromPublisher(sourceUsingMessageListener());
    }

    private Uni<Consumer<T>> createConsumer() {
        return Uni.createFrom().completionStage(() -> {
            CompletionStage<Consumer<T>> consumer = buildConsumer()
                    .subscribeAsync();
            return consumer;
        });
    }

    private Uni<Consumer<T>> createConsumerWithMessageListener(final MultiEmitter<IncomingPulsarMessage<T>> multiEmitter) {
        return Uni.createFrom().completionStage(() -> {
            CompletionStage<Consumer<T>> consumer = buildConsumer()
                    .messageListener(new GenericMessageListener(multiEmitter))
                    .subscribeAsync();
            return consumer;
        });
    }

    protected Multi<IncomingPulsarMessage<T>> sourceUsingMessageListener() {

        Multi<IncomingPulsarMessage<T>> multi = Multi.createFrom().emitter(multiEmitter -> {
            createConsumerWithMessageListener((MultiEmitter<IncomingPulsarMessage<T>>) multiEmitter)
                    .subscribe().with(
                            success -> {
                                System.out.println(success);
                            },
                            failure -> {
                                System.out.println(failure);
                            });
        });
        return multi;
    }

    protected Multi<IncomingPulsarMessage<T>> source() {
        Uni<Consumer<T>> consumerUni = createConsumer();
        Multi<IncomingPulsarMessage<T>> multi = Multi.createFrom().uni(consumerUni)
                .onItem().transformToMultiAndConcatenate(pulsarConsumer -> Uni.createFrom().item(() -> {
                    try {
                        org.apache.pulsar.client.api.Message message = pulsarConsumer.receive();
                        IncomingPulsarMessage<T> incomingPulsarMessage = new IncomingPulsarMessage(pulsarConsumer, message);
                        return incomingPulsarMessage;
                    } catch (Exception e) {
                        e.printStackTrace();
                    }

                    return null;
                })
                        .runSubscriptionOn(Infrastructure.getDefaultExecutor())
                        .repeat().indefinitely());

        return multi;
    }

    private ConsumerBuilder buildConsumer() {
        final Map<String, Object> configMap = new HashMap<>();
        Config config = pcic.config();
        pcic.config().getPropertyNames()
                .forEach(e -> {
                    if (ALLOWABLE_PULSAR_CONSUMER_PROPS.contains(e)) {
                        Object value = handleSpecialTypes(e, config.getValue(e, e.getClass()));
                        configMap.put(e, value);
                    }
                });

        //        Schema schema = handleSchema();
        ConsumerBuilder consumerBuilder = new ConsumerBuilderImpl((PulsarClientImpl) pulsarClient, Schema.BYTES);
        consumerBuilder.loadConf(configMap);
        return consumerBuilder;
    }

    private Object handleSpecialTypes(String fieldName, Object value) {
        Object returnValue = null;
        switch (fieldName) {
            case SPECIAL_TYPE_FIELD_NAME_TOPICNAMES:
                returnValue = value != null
                        ? new TreeSet<String>(Arrays.asList(value.toString().split(",")))
                        : null;
                break;
            default:
                returnValue = value;
        }
        return returnValue;
    }

    private Schema handleSchema() {
        Schema result = Schema.BYTES;
        try {
            switch (pcic.getSchemaType().get()) {
                case AVRO: {
                    result = pcic.getSchema().isPresent() ? Schema.AVRO(Class.forName(pcic.getSchema().get())) : null;
                    break;
                }
                default:
                    result = Schema.BYTES;
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        return result;
    }
}
