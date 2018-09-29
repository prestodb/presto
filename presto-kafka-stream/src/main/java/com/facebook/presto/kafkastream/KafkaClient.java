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

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import static java.util.Objects.requireNonNull;

/**
 * class to get data from kafka based on state store offset
 */
public class KafkaClient
{
    private static final int POLL_VALUE = 1;
    // FIXME :Check whether we can keep this in meta.json.
    private static final long POLL_TIMEOUT = 5000;
    private static final int PARTITION_NO = 0;
    private final TopicPartition tp;
    private final List<TopicPartition> tps = new ArrayList<>();
    private ReadOnlyKeyValueStore<String, String> viewStore;
    private Consumer<String, String> consumer;

    public KafkaClient(ReadOnlyKeyValueStore<String, String> store, KafkaTable kafkaTable)
    {
        this.viewStore = requireNonNull(store, "state store can not be null");
        requireNonNull(kafkaTable, "kafka table can not be null");
        tp = new TopicPartition(kafkaTable.getEntityName(), PARTITION_NO);
        initKafkaConsumer(kafkaTable);
        tps.add(tp);
    }

    private Long getOffsetById(String id)
    {
        String value = viewStore.get(id);
        return Long.valueOf(value);
    }

    public String getDataById(String id)
    {
        Long offset = getOffsetById(id);

        consumer.assign(tps);
        consumer.poll(POLL_VALUE);
        consumer.seek(tp, offset);
        ConsumerRecords<String, String> record = consumer.poll(POLL_TIMEOUT);
        if (record != null && record.iterator() != null && record.iterator().hasNext()) {
            return record.iterator().next().value();
        }
        return null;
    }

    void initKafkaConsumer(KafkaTable kafkaTable)
    {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaTable.getSource());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, kafkaTable.getGroupId());
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, kafkaTable.isEnableAutoCommit());
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumer = new KafkaConsumer<>(props);
    }
}
