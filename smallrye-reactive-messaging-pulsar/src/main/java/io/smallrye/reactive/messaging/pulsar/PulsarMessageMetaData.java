package io.smallrye.reactive.messaging.pulsar;

import java.util.Map;
import java.util.Optional;

import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.common.api.EncryptionContext;

/**
 * @author sherwinpinto
 */
public interface PulsarMessageMetaData {
    byte[] getData();

    Optional<EncryptionContext> getEncryptionCtx();

    long getEventTime();

    String getKey();

    byte[] getKeyBytes();

    MessageId getMessageId();

    String getProducerName();

    Map<String, String> getProperties();

    String getProperty(String name);

    long getPublishTime();

    int getRedeliveryCount();

    long getSequenceId();

    String getTopicName();

    boolean hasBase64EncodedKey();

    boolean hasKey();

    boolean hasProperty(String name);
}
