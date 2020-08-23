package io.smallrye.reactive.messaging.pulsar;

import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageListener;

import io.smallrye.mutiny.subscription.MultiEmitter;

/**
 * @author Sherwin Pinto
 */
public class GenericMessageListener<T> implements MessageListener<T> {
    private final MultiEmitter<IncomingPulsarMessage<T>> multiEmitter;

    protected GenericMessageListener(MultiEmitter<IncomingPulsarMessage<T>> multiEmitter) {
        this.multiEmitter = multiEmitter;
    }

    @Override
    public void received(Consumer<T> consumer, Message<T> message) {
        IncomingPulsarMessage<T> incomingPulsarMessage = new IncomingPulsarMessage<>(consumer, message);
        multiEmitter.emit(incomingPulsarMessage);
    }
}
