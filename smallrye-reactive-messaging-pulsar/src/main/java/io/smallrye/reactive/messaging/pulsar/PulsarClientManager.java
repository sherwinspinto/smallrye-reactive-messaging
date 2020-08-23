package io.smallrye.reactive.messaging.pulsar;

import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;

public class PulsarClientManager {
    private static final PulsarClientManager INSTANCE = new PulsarClientManager();

    private PulsarClient pulsarClient;

    private PulsarClientManager() {
    }

    static PulsarClientManager getInstance() {
        return INSTANCE;
    }

    synchronized PulsarClient getPulsarClient(PulsarConnectorCommonConfiguration config) {
        if (pulsarClient != null)
            return pulsarClient;

        try {
            pulsarClient = PulsarClient.builder()
                    .serviceUrl("pulsar://localhost:6650")
                    .build();
        } catch (PulsarClientException pce) {
            throw new RuntimeException(pce);
        }
        return pulsarClient;
    }
}
