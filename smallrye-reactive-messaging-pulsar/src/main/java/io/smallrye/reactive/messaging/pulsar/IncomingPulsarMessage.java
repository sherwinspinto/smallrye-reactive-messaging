package io.smallrye.reactive.messaging.pulsar;

import java.util.concurrent.CompletionStage;

import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.eclipse.microprofile.reactive.messaging.Metadata;

/**
 *
 * @author sherwinpinto
 */
public class IncomingPulsarMessage<T> implements org.eclipse.microprofile.reactive.messaging.Message<T> {
    private Message<T> pulsarMessage;
    private Metadata metadata;
    private Consumer<T> conusmer;

    protected IncomingPulsarMessage() {

    }

    /**
     *
     * @param message This is the message read by the pulsar consumer @link Message
     */
    protected IncomingPulsarMessage(Consumer<T> consumer, Message<T> message) {
        this.pulsarMessage = message;
        this.metadata = Metadata.of(new PulsarMessageMetaDataImpl(message));
        this.conusmer = consumer;
    }

    @Override
    public T getPayload() {
        return pulsarMessage.getValue();
    }

    @Override
    public Metadata getMetadata() {
        return metadata;
    }

    @Override
    public CompletionStage<Void> ack() {
        return conusmer.acknowledgeAsync(pulsarMessage);
    }
}
