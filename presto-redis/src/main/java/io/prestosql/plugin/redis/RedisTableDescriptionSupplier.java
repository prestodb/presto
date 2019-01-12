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
package io.prestosql.plugin.redis;

import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.json.JsonCodec;
import io.airlift.log.Logger;
import io.prestosql.decoder.dummy.DummyRowDecoder;
import io.prestosql.spi.connector.SchemaTableName;

import javax.inject.Inject;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

import static com.google.common.base.MoreObjects.firstNonNull;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Strings.isNullOrEmpty;
import static java.nio.file.Files.readAllBytes;
import static java.util.Arrays.asList;
import static java.util.Objects.requireNonNull;

public class RedisTableDescriptionSupplier
        implements Supplier<Map<SchemaTableName, RedisTableDescription>>
{
    private static final Logger log = Logger.get(RedisTableDescriptionSupplier.class);

    private final RedisConnectorConfig redisConnectorConfig;
    private final JsonCodec<RedisTableDescription> tableDescriptionCodec;

    @Inject
    RedisTableDescriptionSupplier(RedisConnectorConfig redisConnectorConfig, JsonCodec<RedisTableDescription> tableDescriptionCodec)
    {
        this.redisConnectorConfig = requireNonNull(redisConnectorConfig, "redisConnectorConfig is null");
        this.tableDescriptionCodec = requireNonNull(tableDescriptionCodec, "tableDescriptionCodec is null");
    }

    @Override
    public Map<SchemaTableName, RedisTableDescription> get()
    {
        ImmutableMap.Builder<SchemaTableName, RedisTableDescription> builder = ImmutableMap.builder();

        try {
            for (File file : listFiles(redisConnectorConfig.getTableDescriptionDir())) {
                if (file.isFile() && file.getName().endsWith(".json")) {
                    RedisTableDescription table = tableDescriptionCodec.fromJson(readAllBytes(file.toPath()));
                    String schemaName = firstNonNull(table.getSchemaName(), redisConnectorConfig.getDefaultSchema());
                    log.debug("Redis table %s.%s: %s", schemaName, table.getTableName(), table);
                    builder.put(new SchemaTableName(schemaName, table.getTableName()), table);
                }
            }

            Map<SchemaTableName, RedisTableDescription> tableDefinitions = builder.build();

            log.debug("Loaded table definitions: %s", tableDefinitions.keySet());

            builder = ImmutableMap.builder();
            for (String definedTable : redisConnectorConfig.getTableNames()) {
                SchemaTableName tableName;
                try {
                    tableName = parseTableName(definedTable);
                }
                catch (IllegalArgumentException iae) {
                    tableName = new SchemaTableName(redisConnectorConfig.getDefaultSchema(), definedTable);
                }

                if (tableDefinitions.containsKey(tableName)) {
                    RedisTableDescription redisTable = tableDefinitions.get(tableName);
                    log.debug("Found Table definition for %s: %s", tableName, redisTable);
                    builder.put(tableName, redisTable);
                }
                else {
                    // A dummy table definition only supports the internal columns.
                    log.debug("Created dummy Table definition for %s", tableName);
                    builder.put(tableName, new RedisTableDescription(tableName.getTableName(),
                            tableName.getSchemaName(),
                            new RedisTableFieldGroup(DummyRowDecoder.NAME, null, ImmutableList.of()),
                            new RedisTableFieldGroup(DummyRowDecoder.NAME, null, ImmutableList.of())));
                }
            }

            return builder.build();
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

    private static SchemaTableName parseTableName(String schemaTableName)
    {
        checkArgument(!isNullOrEmpty(schemaTableName), "schemaTableName is null or is empty");
        List<String> parts = Splitter.on('.').splitToList(schemaTableName);
        checkArgument(parts.size() == 2, "Invalid schemaTableName: %s", schemaTableName);
        return new SchemaTableName(parts.get(0), parts.get(1));
    }
}
