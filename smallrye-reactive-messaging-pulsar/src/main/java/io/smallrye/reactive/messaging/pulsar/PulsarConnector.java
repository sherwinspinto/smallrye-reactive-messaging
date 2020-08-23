package io.smallrye.reactive.messaging.pulsar;

import javax.enterprise.context.ApplicationScoped;

import org.apache.pulsar.client.api.*;
import org.eclipse.microprofile.config.Config;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.spi.Connector;
import org.eclipse.microprofile.reactive.messaging.spi.IncomingConnectorFactory;
import org.eclipse.microprofile.reactive.messaging.spi.OutgoingConnectorFactory;
import org.eclipse.microprofile.reactive.streams.operators.PublisherBuilder;
import org.eclipse.microprofile.reactive.streams.operators.ReactiveStreams;
import org.eclipse.microprofile.reactive.streams.operators.SubscriberBuilder;

import io.smallrye.reactive.messaging.annotations.ConnectorAttribute;

/**
 * @author Sherwin Pinto
 */
@ApplicationScoped
@Connector(PulsarConnector.CONNECTOR_NAME)
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
public class PulsarConnector implements IncomingConnectorFactory, OutgoingConnectorFactory {
    static final String CONNECTOR_NAME = "smallrye-pulsar";

    @Override
    public PublisherBuilder<? extends Message<?>> getPublisherBuilder(Config config) {
        PulsarConnectorIncomingConfiguration pcic = new PulsarConnectorIncomingConfiguration(config);
        PulsarClient pulsarClient = PulsarClientManager.getInstance().getPulsarClient(pcic);
        PulsarSource<? extends Message<?>> pulsarSource = new PulsarSource<IncomingPulsarMessage<?>>(pulsarClient, pcic);
        return ReactiveStreams.fromPublisher(pulsarSource.sourceUsingMessageListener());
    }

    @Override
    public SubscriberBuilder<? extends Message<?>, Void> getSubscriberBuilder(Config config) {
        return null;
    }
}
