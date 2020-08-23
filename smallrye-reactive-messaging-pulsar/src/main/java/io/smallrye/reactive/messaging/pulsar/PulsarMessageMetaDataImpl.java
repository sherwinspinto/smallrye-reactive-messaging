package io.smallrye.reactive.messaging.pulsar;

import java.util.Map;
import java.util.Optional;

import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.common.api.EncryptionContext;

/**
 * @author sherwinpinto
 */
public class PulsarMessageMetaDataImpl implements PulsarMessageMetaData {
    private Message pulsarMessage;

    PulsarMessageMetaDataImpl() {

    }

    protected PulsarMessageMetaDataImpl(Message pulsarMessage) {
        this.pulsarMessage = pulsarMessage;
    }

    @Override
    public byte[] getData() {
        return pulsarMessage.getData();
    }

    @Override
    public Optional<EncryptionContext> getEncryptionCtx() {
        return pulsarMessage.getEncryptionCtx();
    }

    @Override
    public long getEventTime() {
        return pulsarMessage.getEventTime();
    }

    @Override
    public String getKey() {
        return pulsarMessage.getKey();
    }

    @Override
    public byte[] getKeyBytes() {
        return pulsarMessage.getKeyBytes();
    }

    @Override
    public MessageId getMessageId() {
        return pulsarMessage.getMessageId();
    }

    @Override
    public String getProducerName() {
        return pulsarMessage.getProducerName();
    }

    @Override
    public Map<String, String> getProperties() {
        return pulsarMessage.getProperties();
    }

    @Override
    public String getProperty(String name) {
        return pulsarMessage.getProperty(name);
    }

    @Override
    public long getPublishTime() {
        return pulsarMessage.getPublishTime();
    }

    @Override
    public int getRedeliveryCount() {
        return pulsarMessage.getRedeliveryCount();
    }

    @Override
    public long getSequenceId() {
        return pulsarMessage.getSequenceId();
    }

    @Override
    public String getTopicName() {
        return pulsarMessage.getTopicName();
    }

    @Override
    public boolean hasBase64EncodedKey() {
        return pulsarMessage.hasBase64EncodedKey();
    }

    @Override
    public boolean hasKey() {
        return pulsarMessage.hasKey();
    }

    @Override
    public boolean hasProperty(String name) {
        return pulsarMessage.hasProperty(name);
    }
}
