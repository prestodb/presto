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
package com.facebook.presto.kinesis;

import io.airlift.json.JsonCodec;
import io.airlift.log.Logger;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.weakref.jmx.internal.guava.base.Objects;

import com.facebook.presto.kinesis.decoder.dummy.DummyKinesisRowDecoder;
import com.facebook.presto.spi.SchemaTableName;
import com.google.common.base.Supplier;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.Files;
import com.google.inject.Inject;

import static com.google.common.base.Preconditions.checkNotNull;
import static java.util.Arrays.asList;

/**
 *
 * This class get() method reads the table description file stored in Kinesis directory
 * and then creates user defined field for Presto Table.
 *
 */
public class KinesisTableDescriptionSupplier
        implements Supplier<Map<SchemaTableName, KinesisStreamDescription>>
{
    private static final Logger log = Logger.get(KinesisTableDescriptionSupplier.class);

    public final KinesisConnectorConfig kinesisConnectorConfig;
    public final JsonCodec<KinesisStreamDescription> streamDescriptionCodec;

    @Inject
    KinesisTableDescriptionSupplier(KinesisConnectorConfig kinesisConnectorConfig,
            JsonCodec<KinesisStreamDescription> streamDescriptionCodec)
    {
        this.kinesisConnectorConfig = checkNotNull(kinesisConnectorConfig, "kinesisConnectorConfig is null");
        this.streamDescriptionCodec = checkNotNull(streamDescriptionCodec, "streamDescriptionCodec is null");
    }

    @Override
    public Map<SchemaTableName, KinesisStreamDescription> get()
    {
        ImmutableMap.Builder<SchemaTableName, KinesisStreamDescription> builder = ImmutableMap.builder();
        try {
            for (File file : listFiles(kinesisConnectorConfig.getTableDescriptionDir())) {
                if (file.isFile() && file.getName().endsWith(".json")) {
                    KinesisStreamDescription table = streamDescriptionCodec.fromJson(Files.toByteArray(file));
                    String schemaName = Objects.firstNonNull(table.getSchemaName(), kinesisConnectorConfig.getDefaultSchema());
                    log.debug("Kinesis table %s %s %s", schemaName, table.getTableName(), table);
                    builder.put(new SchemaTableName(schemaName, table.getTableName()), table);
                }
            }

            Map<SchemaTableName, KinesisStreamDescription> tableDefinitions = builder.build();

            log.debug("Loaded table definitions: %s", tableDefinitions.keySet());

            builder = ImmutableMap.builder();
            for (String definedTable : kinesisConnectorConfig.getTableNames()) {
                SchemaTableName tableName;
                try {
                    tableName = SchemaTableName.valueOf(definedTable);
                }
                catch (IllegalArgumentException iae) {
                    tableName = new SchemaTableName(kinesisConnectorConfig.getDefaultSchema(), definedTable);
                }

                if (tableDefinitions.containsKey(tableName)) {
                    KinesisStreamDescription kinesisTable = tableDefinitions.get(tableName);
                    log.debug("Found Table definition fo %s %s", tableName, kinesisTable);
                    builder.put(tableName, kinesisTable);
                }
                else {
                    // A dummy table definition only supports the internal columns
                    log.debug("Created dummy Table definition for %s", tableName);
                    builder.put(tableName, new KinesisStreamDescription(tableName.getTableName(),
                            tableName.getSchemaName(),
                            definedTable,
                            new KinesisStreamFieldGroup(DummyKinesisRowDecoder.NAME, ImmutableList.<KinesisStreamFieldDescription>of())));
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
