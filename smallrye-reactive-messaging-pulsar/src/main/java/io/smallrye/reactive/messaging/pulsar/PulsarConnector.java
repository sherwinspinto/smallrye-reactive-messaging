package io.smallrye.reactive.messaging.pulsar;

import javax.enterprise.context.ApplicationScoped;

import org.apache.pulsar.client.api.*;
import org.eclipse.microprofile.config.Config;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.spi.Connector;
import org.eclipse.microprofile.reactive.messaging.spi.IncomingConnectorFactory;
import org.eclipse.microprofile.reactive.messaging.spi.OutgoingConnectorFactory;
import org.eclipse.microprofile.reactive.streams.operators.PublisherBuilder;
import org.eclipse.microprofile.reactive.streams.operators.SubscriberBuilder;

import io.smallrye.reactive.messaging.annotations.ConnectorAttribute;

/**
 * @author Sherwin Pinto
 */
@ApplicationScoped
@Connector(PulsarConnector.CONNECTOR_NAME)
//Consumer Properties
@ConnectorAttribute(name = "topicNames", type = "string", mandatory = true, direction = ConnectorAttribute.Direction.INCOMING, description = "The list pulsar topic being consumed from or produced to")
@ConnectorAttribute(name = "subscriptionName", type = "string", mandatory = true, direction = ConnectorAttribute.Direction.INCOMING, description = "The name of the supscription")
@ConnectorAttribute(name = "subscriptionType", type = "org.apache.pulsar.client.api.SubscriptionType", mandatory = true, direction = ConnectorAttribute.Direction.INCOMING, description = "The subscription type, possible values Exclusive/Shared/Failover/Key_Shared")
@ConnectorAttribute(name = "patternAutoDiscoveryPeriod", type = "double", direction = ConnectorAttribute.Direction.INCOMING, description = "")
@ConnectorAttribute(name = "subscriptionInitialPosition", type = "string", direction = ConnectorAttribute.Direction.INCOMING, description = "")
@ConnectorAttribute(name = "tickDurationMillis", type = "double", direction = ConnectorAttribute.Direction.INCOMING, description = "")
@ConnectorAttribute(name = "replicateSubscriptionState", type = "boolean", direction = ConnectorAttribute.Direction.INCOMING, description = "")
@ConnectorAttribute(name = "retryEnable", type = "boolean", direction = ConnectorAttribute.Direction.INCOMING, description = "")
@ConnectorAttribute(name = "negativeAckRedeliveryDelayMicros", type = "double", direction = ConnectorAttribute.Direction.INCOMING, description = "")
@ConnectorAttribute(name = "expireTimeOfIncompleteChunkedMessageMillis", type = "double", direction = ConnectorAttribute.Direction.INCOMING, description = "")
@ConnectorAttribute(name = "cryptoFailureAction", type = "string", direction = ConnectorAttribute.Direction.INCOMING, description = "")
@ConnectorAttribute(name = "ackTimeoutMillis", type = "double", direction = ConnectorAttribute.Direction.INCOMING, description = "")
@ConnectorAttribute(name = "autoAckOldestChunkedMessageOnQueueFull", type = "boolean", direction = ConnectorAttribute.Direction.INCOMING, description = "")
@ConnectorAttribute(name = "maxPendingChuckedMessage", type = "double", direction = ConnectorAttribute.Direction.INCOMING, description = "")
@ConnectorAttribute(name = "readCompacted", type = "boolean", direction = ConnectorAttribute.Direction.INCOMING, description = "")
@ConnectorAttribute(name = "regexSubscriptionMode", type = "string", direction = ConnectorAttribute.Direction.INCOMING, description = "")
@ConnectorAttribute(name = "priorityLevel", type = "double", direction = ConnectorAttribute.Direction.INCOMING, description = "")
@ConnectorAttribute(name = "maxTotalReceiverQueueSizeAcrossPartitions", type = "double", direction = ConnectorAttribute.Direction.INCOMING, description = "")
@ConnectorAttribute(name = "acknowledgementsGroupTimeMicros", type = "double", direction = ConnectorAttribute.Direction.INCOMING, description = "")
@ConnectorAttribute(name = "resetIncludeHead", type = "boolean", direction = ConnectorAttribute.Direction.INCOMING, description = "")
@ConnectorAttribute(name = "receiverQueueSize", type = "double", direction = ConnectorAttribute.Direction.INCOMING, description = "")
@ConnectorAttribute(name = "autoUpdatePartitions", type = "boolean", direction = ConnectorAttribute.Direction.INCOMING, description = "")
//@ConnectorAttribute(name = "properties", type = "java.util.Map", direction = ConnectorAttribute.Direction.INCOMING, description = "")
@ConnectorAttribute(name = "subscriptionMode", type = "org.apache.pulsar.client.api.SubscriptionMode", direction = ConnectorAttribute.Direction.INCOMING, description = "")
@ConnectorAttribute(name = "schemaType", type = "io.smallrye.reactive.messaging.pulsar.PulsarSource.SCHEMA_TYPE", direction = ConnectorAttribute.Direction.INCOMING, description = "")
@ConnectorAttribute(name = "schema", type = "string", direction = ConnectorAttribute.Direction.INCOMING, description = "")

//Producer Properties
@ConnectorAttribute(name = "initialSequenceId", type = "long", direction = ConnectorAttribute.Direction.OUTGOING, description = "")
@ConnectorAttribute(name = "chunkingEnabled", type = "boolean", direction = ConnectorAttribute.Direction.OUTGOING, description = "")
@ConnectorAttribute(name = "encryptionKeys", type = "java.util.Set", direction = ConnectorAttribute.Direction.OUTGOING, description = "")
@ConnectorAttribute(name = "batchingMaxMessages", type = "int", direction = ConnectorAttribute.Direction.OUTGOING, description = "")
@ConnectorAttribute(name = "compressionType", type = "org.apache.pulsar.client.api.CompressionType", direction = ConnectorAttribute.Direction.OUTGOING, description = "")
@ConnectorAttribute(name = "hashingScheme", type = "org.apache.pulsar.client.api.HashingScheme", direction = ConnectorAttribute.Direction.OUTGOING, description = "")
@ConnectorAttribute(name = "maxPendingMessagesAcrossPartitions", type = "int", direction = ConnectorAttribute.Direction.OUTGOING, description = "")
@ConnectorAttribute(name = "batchingEnabled", type = "boolean", direction = ConnectorAttribute.Direction.OUTGOING, description = "")
@ConnectorAttribute(name = "sendTimeoutMs", type = "long", direction = ConnectorAttribute.Direction.OUTGOING, description = "")
@ConnectorAttribute(name = "cryptoKeyReader", type = "org.apache.pulsar.client.api.CryptoKeyReader", direction = ConnectorAttribute.Direction.OUTGOING, description = "")
@ConnectorAttribute(name = "autoUpdatePartitions", type = "boolean", direction = ConnectorAttribute.Direction.OUTGOING, description = "")
@ConnectorAttribute(name = "customMessageRouter", type = "org.apache.pulsar.client.api.MessageRouter", direction = ConnectorAttribute.Direction.OUTGOING, description = "")
@ConnectorAttribute(name = "batcherBuilder", type = "org.apache.pulsar.client.api.BatcherBuilder", direction = ConnectorAttribute.Direction.OUTGOING, description = "")
@ConnectorAttribute(name = "messageCrypto", type = "org.apache.pulsar.client.api.MessageCrypto", direction = ConnectorAttribute.Direction.OUTGOING, description = "")
@ConnectorAttribute(name = "blockIfQueueFull", type = "boolean", direction = ConnectorAttribute.Direction.OUTGOING, description = "")
@ConnectorAttribute(name = "batchingMaxBytes", type = "int", direction = ConnectorAttribute.Direction.OUTGOING, description = "")
@ConnectorAttribute(name = "batchingMaxPublishDelayMicros", type = "long", direction = ConnectorAttribute.Direction.OUTGOING, description = "")
@ConnectorAttribute(name = "producerName", type = "string", direction = ConnectorAttribute.Direction.OUTGOING, description = "")
@ConnectorAttribute(name = "batchingPartitionSwitchFrequencyByPublishDelay", type = "int", direction = ConnectorAttribute.Direction.OUTGOING, description = "")
@ConnectorAttribute(name = "maxPendingMessages", type = "int", direction = ConnectorAttribute.Direction.OUTGOING, description = "")
@ConnectorAttribute(name = "messageRoutingMode", type = "org.apache.pulsar.client.api.MessageRoutingMode", direction = ConnectorAttribute.Direction.OUTGOING, description = "")
@ConnectorAttribute(name = "cryptoFailureAction", type = "org.apache.pulsar.client.api.ProducerCryptoFailureAction", direction = ConnectorAttribute.Direction.OUTGOING, description = "")
@ConnectorAttribute(name = "multiSchema", type = "boolean", direction = ConnectorAttribute.Direction.OUTGOING, description = "")
@ConnectorAttribute(name = "topicName", type = "string", direction = ConnectorAttribute.Direction.OUTGOING, description = "")
@ConnectorAttribute(name = "properties", type = "java.util.SortedMap", direction = ConnectorAttribute.Direction.OUTGOING, description = "")
public class PulsarConnector implements IncomingConnectorFactory, OutgoingConnectorFactory {
    static final String CONNECTOR_NAME = "smallrye-pulsar";

    @Override
    public PublisherBuilder<? extends Message<?>> getPublisherBuilder(Config config) {
        PulsarConnectorIncomingConfiguration pcic = new PulsarConnectorIncomingConfiguration(config);
        PulsarClient pulsarClient = PulsarClientManager.getInstance().getPulsarClient(pcic);
        PulsarSource<? extends Message<?>> pulsarSource = new PulsarSource<IncomingPulsarMessage<?>>(pulsarClient, pcic);
        return pulsarSource.createPublisher();
    }

    @Override
    public SubscriberBuilder<? extends Message<?>, Void> getSubscriberBuilder(Config config) {
        PulsarConnectorOutgoingConfiguration pcoc = new PulsarConnectorOutgoingConfiguration(config);
        PulsarClient pulsarClient = PulsarClientManager.getInstance().getPulsarClient(pcoc);
        PulsarSink<? extends Message<?>> pulsarSink = new PulsarSink<>(pulsarClient, pcoc);
        return pulsarSink.sink();
    }
}
