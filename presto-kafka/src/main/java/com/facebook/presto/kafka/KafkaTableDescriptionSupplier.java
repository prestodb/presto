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

import com.facebook.presto.decoder.dummy.DummyRowDecoder;
import com.facebook.presto.spi.SchemaTableName;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.airlift.json.JsonCodec;
import io.airlift.log.Logger;

import javax.inject.Inject;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Supplier;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Strings.isNullOrEmpty;
import static java.nio.file.Files.readAllBytes;
import static java.util.Arrays.asList;
import static java.util.Objects.requireNonNull;

public class KafkaTableDescriptionSupplier
        implements Supplier<Map<SchemaTableName, KafkaTopicDescription>>
{
    private static final Logger log = Logger.get(KafkaTableDescriptionSupplier.class);

    private final JsonCodec<KafkaTopicDescription> topicDescriptionCodec;
    private final File tableDescriptionDir;
    private final String defaultSchema;
    private final Set<String> tableNames;

    @Inject
    KafkaTableDescriptionSupplier(KafkaConnectorConfig kafkaConnectorConfig,
            JsonCodec<KafkaTopicDescription> topicDescriptionCodec)
    {
        this.topicDescriptionCodec = requireNonNull(topicDescriptionCodec, "topicDescriptionCodec is null");

        requireNonNull(kafkaConnectorConfig, "kafkaConfig is null");
        this.tableDescriptionDir = kafkaConnectorConfig.getTableDescriptionDir();
        this.defaultSchema = kafkaConnectorConfig.getDefaultSchema();
        this.tableNames = ImmutableSet.copyOf(kafkaConnectorConfig.getTableNames());
    }

    @Override
    public Map<SchemaTableName, KafkaTopicDescription> get()
    {
        ImmutableMap.Builder<SchemaTableName, KafkaTopicDescription> tableDefinitionBuilder = ImmutableMap.builder();

        log.debug("Loading kafka table definitions from %s", tableDescriptionDir.getAbsolutePath());

        try {
            for (File file : listFiles(tableDescriptionDir)) {
                if (file.isFile() && file.getName().endsWith(".json")) {
                    KafkaTopicDescription table = topicDescriptionCodec.fromJson(readAllBytes(file.toPath()));
                    String schemaName = table.getSchemaName().orElse(defaultSchema);
                    log.debug("Found Kafka Table definition for %s.%s: %s", schemaName, table.getTableName(), table);
                    tableDefinitionBuilder.put(new SchemaTableName(schemaName, table.getTableName()), table);
                }
            }
            Map<SchemaTableName, KafkaTopicDescription> tableDefinitions = tableDefinitionBuilder.build();
            log.debug("Loaded Table definitions: %s", tableDefinitions.keySet());

            Map<SchemaTableName, KafkaTopicDescription> builder = new HashMap<>();

            // A dummy table definition only supports the internal columns.
            for (String definedTable : tableNames) {
                SchemaTableName tableName = parseTableName(definedTable);

                log.debug("Created dummy Table definition for %s", tableName);
                builder.put(tableName, new KafkaTopicDescription(
                        tableName.getTableName(),
                        Optional.ofNullable(tableName.getSchemaName()),
                        definedTable,
                        Optional.of(new KafkaTopicFieldGroup(DummyRowDecoder.NAME, Optional.empty(), ImmutableList.of())),
                        Optional.of(new KafkaTopicFieldGroup(DummyRowDecoder.NAME, Optional.empty(), ImmutableList.of()))));
            }

            //Add tables from json definition schemas
            builder.putAll(tableDefinitions);
            return builder;
        }
        catch (IOException e) {
            log.warn(e, "Error: ");
            throw new UncheckedIOException(e);
        }
    }

    private static List<File> listFiles(File dir)
    {
        if ((dir != null) && dir.isDirectory()) {
            File[] files = dir.listFiles();
            if (files != null) {
                log.debug("Considering files: %s", asList(files));
                return ImmutableList.copyOf(files);
            }
        }
        return ImmutableList.of();
    }

    private SchemaTableName parseTableName(String schemaTableName)
    {
        try {
            checkArgument(!isNullOrEmpty(schemaTableName), "schemaTableName is null or is empty");
            List<String> parts = Splitter.on('.').splitToList(schemaTableName);
            checkArgument(parts.size() == 2, "Invalid schemaTableName: %s", schemaTableName);
            return new SchemaTableName(parts.get(0), parts.get(1));
        }
        catch (IllegalArgumentException ignored) {
            return new SchemaTableName(defaultSchema, schemaTableName);
        }
    }
}
