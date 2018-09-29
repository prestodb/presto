/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.facebook.presto.kafkastream;

import com.facebook.presto.spi.NodeManager;
import com.google.common.base.Throwables;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import io.airlift.log.Logger;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.InvalidStateStoreException;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Reducer;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;

import javax.inject.Inject;

import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;

/**
 *
 */
public class KafkaKStreamConsumerManager
{
    private static final Logger log = Logger.get(KafkaKStreamConsumerManager.class);
    private static final Long WAIT_TIME = 3000L;
    private final LoadingCache<KafkaTable, KafkaClient> consumerCache;
    private final String connectorId;
    private final KafkaMetadataClient kafkaMetadataClient;
    private final NodeManager nodeManager;
    private final int connectTimeoutMillis;
    private final int bufferSizeBytes;

    @Inject
    public KafkaKStreamConsumerManager(
            KafkaConnectorId connectorId,
            KafkaMetadataClient kafkaMetadataClient)
    {
        log.debug("Creating KafkaKStreamConsumerManager");
        this.connectorId = requireNonNull(connectorId, "connectorId is null").toString();
        this.kafkaMetadataClient =
                requireNonNull(kafkaMetadataClient, "Kafka Metadata is null ");
        //FIXME:Check the usage of nodeManager. delete if not required
        this.nodeManager = null;
        // requireNonNull(nodeManager, "nodeManager is
        // null");
        //FIXME this should be part of config.
        this.connectTimeoutMillis =
                toIntExact(1000L);
        this.bufferSizeBytes =
                toIntExact(300);
        this.consumerCache =
                CacheBuilder.newBuilder().build(CacheLoader.from(this::createConsumer));

        kafkaMetadataClient.getKafkaConfigBeanMap().values().stream().flatMap(
                configBean -> configBean.getKafkaTableMap().values().stream()).map(
                this::getConsumer).collect(Collectors.toList());

        log.debug("KafkaKStreamConsumerManager Completed");
    }

    public KafkaClient getConsumer(KafkaTable kafkaTable)
    {
        requireNonNull(kafkaTable, "prop is null");
        try {
            return consumerCache.get(kafkaTable);
        }
        catch (ExecutionException e) {
            throw Throwables.propagate(e.getCause());
        }
    }

    private KafkaClient createConsumer(KafkaTable kafkaTable)
    {
        log.debug("Creating new Consumer for %s", kafkaTable);
        Properties config = new Properties();

        config.put(StreamsConfig.APPLICATION_ID_CONFIG,
                kafkaTable.getApplicationId());
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,
                kafkaTable.getSource());
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG,
                kafkaTable.getKeyDeserializer());
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG,
                kafkaTable.getValueDeserializer());

        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, String> inputDataStream =
                builder.stream(kafkaTable.getEntityName());
        @SuppressWarnings("static-access")
        Materialized materialized =
                Materialized.with(Serdes.String(), Serdes.String()).withLoggingDisabled().as(
                        kafkaTable.getStoreName());

        inputDataStream.transform(KeyTransformer::new).groupByKey().reduce(
                new Reducer<String>()
                {
                    @Override
                    public String apply(String aggValue, String currValue)
                    {
                        return currValue;
                    }
                }, materialized);
        KafkaStreams streams = new KafkaStreams(builder.build(), config);
        streams.cleanUp();
        streams.start();
        ReadOnlyKeyValueStore<String, String> store = null;
        while (true) {
            try {
                store = streams.store(kafkaTable.getStoreName(), QueryableStoreTypes.<String, String>keyValueStore());
                break;
            }
            catch (InvalidStateStoreException ignored) {
                // store not yet ready for querying
                try {
                    Thread.sleep(100);
                }
                catch (InterruptedException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
            }
        }
        KafkaClient client =
                new KafkaClient(store, kafkaTable);
        return client;
    }
}
