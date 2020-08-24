package io.smallrye.reactive.messaging.pulsar;

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.pulsar.client.api.ClientBuilder;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.impl.ClientBuilderImpl;
import org.apache.pulsar.client.impl.conf.ClientConfigurationData;
import org.eclipse.microprofile.config.Config;

public class PulsarClientManager {
    private static final PulsarClientManager INSTANCE = new PulsarClientManager();
    private static final Set<String> ALLOWABLE_PULSAR_CLIENT_PROPERTIES = new HashSet<>();
    static {
        ALLOWABLE_PULSAR_CLIENT_PROPERTIES.add("serviceUrl");
        ALLOWABLE_PULSAR_CLIENT_PROPERTIES.add("serviceUrlProvider");
        ALLOWABLE_PULSAR_CLIENT_PROPERTIES.add("authentication");
        ALLOWABLE_PULSAR_CLIENT_PROPERTIES.add("authPluginClassName");
        ALLOWABLE_PULSAR_CLIENT_PROPERTIES.add("authParams");
        ALLOWABLE_PULSAR_CLIENT_PROPERTIES.add("authParamMap");
        ALLOWABLE_PULSAR_CLIENT_PROPERTIES.add("operationTimeoutMs");
        ALLOWABLE_PULSAR_CLIENT_PROPERTIES.add("statsIntervalSeconds");
        ALLOWABLE_PULSAR_CLIENT_PROPERTIES.add("numIoThreads");
        ALLOWABLE_PULSAR_CLIENT_PROPERTIES.add("numListenerThreads");
        ALLOWABLE_PULSAR_CLIENT_PROPERTIES.add("connectionsPerBroker");
        ALLOWABLE_PULSAR_CLIENT_PROPERTIES.add("useTcpNoDelay");
        ALLOWABLE_PULSAR_CLIENT_PROPERTIES.add("useTls");
        ALLOWABLE_PULSAR_CLIENT_PROPERTIES.add("tlsTrustCertsFilePath");
        ALLOWABLE_PULSAR_CLIENT_PROPERTIES.add("tlsAllowInsecureConnection");
        ALLOWABLE_PULSAR_CLIENT_PROPERTIES.add("tlsHostnameVerificationEnable");
        ALLOWABLE_PULSAR_CLIENT_PROPERTIES.add("concurrentLookupRequest");
        ALLOWABLE_PULSAR_CLIENT_PROPERTIES.add("maxLookupRequest");
        ALLOWABLE_PULSAR_CLIENT_PROPERTIES.add("maxLookupRedirects");
        ALLOWABLE_PULSAR_CLIENT_PROPERTIES.add("maxNumberOfRejectedRequestPerConnection");
        ALLOWABLE_PULSAR_CLIENT_PROPERTIES.add("keepAliveIntervalSeconds");
        ALLOWABLE_PULSAR_CLIENT_PROPERTIES.add("connectionTimeoutMs");
        ALLOWABLE_PULSAR_CLIENT_PROPERTIES.add("requestTimeoutMs");
        ALLOWABLE_PULSAR_CLIENT_PROPERTIES.add("initialBackoffIntervalNanos");
        ALLOWABLE_PULSAR_CLIENT_PROPERTIES.add("maxBackoffIntervalNanos");
        ALLOWABLE_PULSAR_CLIENT_PROPERTIES.add("listenerName");
        ALLOWABLE_PULSAR_CLIENT_PROPERTIES.add("useKeyStoreTls");
        ALLOWABLE_PULSAR_CLIENT_PROPERTIES.add("sslProvider");
        ALLOWABLE_PULSAR_CLIENT_PROPERTIES.add("tlsTrustStoreType");
        ALLOWABLE_PULSAR_CLIENT_PROPERTIES.add("tlsTrustStorePath");
        ALLOWABLE_PULSAR_CLIENT_PROPERTIES.add("tlsTrustStorePassword");
        ALLOWABLE_PULSAR_CLIENT_PROPERTIES.add("tlsCiphers");
        ALLOWABLE_PULSAR_CLIENT_PROPERTIES.add("tlsProtocols");
        ALLOWABLE_PULSAR_CLIENT_PROPERTIES.add("proxyServiceUrl");
        ALLOWABLE_PULSAR_CLIENT_PROPERTIES.add("proxyProtocol");
        ALLOWABLE_PULSAR_CLIENT_PROPERTIES.add("clock");
    }

    private PulsarClient pulsarClient;

    private PulsarClientManager() {
    }

    static PulsarClientManager getInstance() {
        return INSTANCE;
    }

    synchronized PulsarClient getPulsarClient(PulsarConnectorCommonConfiguration pccc) {
        if (pulsarClient != null)
            return pulsarClient;

        try {
            pulsarClient = createPulsarClientBuilder(pccc)
                    .build();
        } catch (PulsarClientException pce) {
            throw new RuntimeException(pce);
        }
        return pulsarClient;
    }

    private ClientBuilder createPulsarClientBuilder(PulsarConnectorCommonConfiguration pccc) {
        final Map<String, Object> configMap = new HashMap<>();
        Config config = pccc.config();
        config.getPropertyNames()
                .forEach(e -> {
                    if (ALLOWABLE_PULSAR_CLIENT_PROPERTIES.contains(e)) {
                        Object value = config.getValue(e, e.getClass());
                        configMap.put(e, value);
                    }
                });

        ClientBuilder clientBuilder = new ClientBuilderImpl();
        return clientBuilder.loadConf(configMap);
    }

    public static void main(String[] args) {
        Field[] fields = ClientConfigurationData.class.getDeclaredFields();
        for (Field field : fields) {
            System.out.println(String.format("ALLOWABLE_PULSAR_CLIENT_PROPERTIES.add(\"%s\");", field.getName()));
        }
    }
}
