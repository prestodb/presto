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
package com.facebook.presto.kafka;

import com.facebook.presto.kafka.decoder.dummy.DummyKafkaRowDecoder;
import com.facebook.presto.spi.SchemaTableName;
import com.google.common.base.Objects;
import com.google.common.base.Supplier;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.Files;
import io.airlift.json.JsonCodec;
import io.airlift.log.Logger;

import javax.inject.Inject;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static com.google.common.base.Preconditions.checkNotNull;

public class KafkaTableDescriptionSupplier
        implements Supplier<Map<SchemaTableName, KafkaTopicDescription>>
{
    private static final Logger LOG = Logger.get(KafkaTableDescriptionSupplier.class);

    private final KafkaConnectorConfig kafkaConnectorConfig;
    private final JsonCodec<KafkaTopicDescription> topicDescriptionCodec;

    @Inject
    KafkaTableDescriptionSupplier(KafkaConnectorConfig kafkaConnectorConfig,
            JsonCodec<KafkaTopicDescription> topicDescriptionCodec)
    {
        this.kafkaConnectorConfig = checkNotNull(kafkaConnectorConfig, "kafkaConnectorConfig is null");
        this.topicDescriptionCodec = checkNotNull(topicDescriptionCodec, "topicDescriptionCodec is null");
    }

    @Override
    public Map<SchemaTableName, KafkaTopicDescription> get()
    {
        ImmutableMap.Builder<SchemaTableName, KafkaTopicDescription> builder = ImmutableMap.builder();

        try {
            for (File file : listFiles(kafkaConnectorConfig.getTableDescriptionDir())) {
                if (file.isFile() && file.getName().endsWith(".json")) {
                    KafkaTopicDescription table = topicDescriptionCodec.fromJson(Files.toByteArray(file));
                    String schemaName = Objects.firstNonNull(table.getSchemaName(), kafkaConnectorConfig.getDefaultSchema());
                    LOG.debug("Kafka table %s.%s: %s", schemaName, table.getTableName(), table);
                    builder.put(new SchemaTableName(schemaName, table.getTableName()), table);
                }
            }

            Map<SchemaTableName, KafkaTopicDescription> tableDefinitions = builder.build();

            LOG.debug("Loaded Table definitions: %s", tableDefinitions.keySet());

            builder = ImmutableMap.builder();
            for (String definedTable : kafkaConnectorConfig.getTableNames()) {
                SchemaTableName tableName;
                try {
                    tableName = SchemaTableName.valueOf(definedTable);
                }
                catch (IllegalArgumentException iae) {
                    tableName = new SchemaTableName(kafkaConnectorConfig.getDefaultSchema(), definedTable);
                }

                if (tableDefinitions.containsKey(tableName)) {
                    KafkaTopicDescription kafkaTable = tableDefinitions.get(tableName);
                    LOG.debug("Found Table definition for %s: %s", tableName, kafkaTable);
                    builder.put(tableName, kafkaTable);
                }
                else {
                    // A dummy table definition only supports the internal columns.
                    LOG.debug("Created dummy Table definition for %s", tableName);
                    builder.put(tableName, new KafkaTopicDescription(tableName.getTableName(),
                            tableName.getSchemaName(),
                            definedTable,
                            new KafkaTopicFieldGroup(DummyKafkaRowDecoder.NAME, ImmutableList.<KafkaTopicFieldDescription>of()),
                            new KafkaTopicFieldGroup(DummyKafkaRowDecoder.NAME, ImmutableList.<KafkaTopicFieldDescription>of())));
                }
            }

            return builder.build();
        }
        catch (IOException e) {
            LOG.warn(e, "Error: ");
            throw Throwables.propagate(e);
        }
    }

    private List<File> listFiles(File dir)
    {
        if (dir != null && dir.isDirectory()) {
            File[] files = dir.listFiles();
            if (files != null) {
                LOG.debug("Considering files: %s", Arrays.asList(files));
                return ImmutableList.copyOf(files);
            }
        }
        return ImmutableList.of();
    }
}
