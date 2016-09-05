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
package com.facebook.presto.hbase;

import com.facebook.presto.decoder.dummy.DummyRowDecoder;
import com.facebook.presto.spi.SchemaTableName;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.airlift.json.JsonCodec;
import io.airlift.log.Logger;

import javax.inject.Inject;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;

import static com.google.common.base.MoreObjects.firstNonNull;
import static java.nio.file.Files.readAllBytes;
import static java.util.Arrays.asList;
import static java.util.Objects.requireNonNull;

public class HBaseTableDescriptionSupplier
        implements Supplier<Map<SchemaTableName, HBaseTableDescription>>
{
    private static final Logger log = Logger.get(HBaseTableDescriptionSupplier.class);

    private final JsonCodec<HBaseTableDescription> topicDescriptionCodec;
    private final File tableDescriptionDir;
    private final String defaultSchema;
    private final Set<String> tableNames;

    @Inject
    HBaseTableDescriptionSupplier(HBaseConnectorConfig kafkaConnectorConfig,
            JsonCodec<HBaseTableDescription> topicDescriptionCodec)
    {
        this.topicDescriptionCodec = requireNonNull(topicDescriptionCodec, "topicDescriptionCodec is null");

        requireNonNull(kafkaConnectorConfig, "kafkaConfig is null");
        this.tableDescriptionDir = kafkaConnectorConfig.getTableDescriptionDir();
        this.defaultSchema = kafkaConnectorConfig.getDefaultSchema();
        this.tableNames = ImmutableSet.copyOf(kafkaConnectorConfig.getTableNames());
    }

    @Override
    public Map<SchemaTableName, HBaseTableDescription> get()
    {
        ImmutableMap.Builder<SchemaTableName, HBaseTableDescription> builder = ImmutableMap.builder();

        log.debug("Loading hbase table definitions from %s", tableDescriptionDir.getAbsolutePath());

        try {
            for (File file : listFiles(tableDescriptionDir)) {
                if (file.isFile() && file.getName().endsWith(".json")) {
                    HBaseTableDescription table = topicDescriptionCodec.fromJson(readAllBytes(file.toPath()));
                    String schemaName = firstNonNull(table.getSchemaName(), defaultSchema);
                    log.debug("HBase table %s.%s: %s", schemaName, table.getTableName(), table);
                    builder.put(new SchemaTableName(schemaName, table.getTableName()), table);
                }
            }

            Map<SchemaTableName, HBaseTableDescription> tableDefinitions = builder.build();

            log.debug("Loaded Table definitions: %s", tableDefinitions.keySet());

            builder = ImmutableMap.builder();
            for (String definedTable : tableNames) {
                SchemaTableName tableName;
                try {
                    tableName = SchemaTableName.valueOf(definedTable);
                }
                catch (IllegalArgumentException iae) {
                    tableName = new SchemaTableName(defaultSchema, definedTable);
                }

                if (tableDefinitions.containsKey(tableName)) {
                    HBaseTableDescription kafkaTable = tableDefinitions.get(tableName);
                    log.debug("Found Table definition for %s: %s", tableName, kafkaTable);
                    builder.put(tableName, kafkaTable);
                }
                else {
                    // A dummy table definition only supports the internal columns.
                    log.debug("Created dummy Table definition for %s", tableName);
                    builder.put(tableName, new HBaseTableDescription(tableName.getTableName(),
                            tableName.getSchemaName(),
                            definedTable,
                            new HBaseTableFieldGroup(DummyRowDecoder.NAME, ImmutableList.<HBaseTableFieldDescription>of()),
                            new HBaseTableFieldGroup(DummyRowDecoder.NAME, ImmutableList.<HBaseTableFieldDescription>of())));
                }
            }

            return builder.build();
        }
        catch (IOException e) {
            log.warn(e, "Error: ");
            throw Throwables.propagate(e);
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
}
