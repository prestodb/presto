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
package com.facebook.presto.elasticsearch;

import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.SchemaTableName;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.airlift.json.JsonCodec;

import javax.inject.Inject;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;

import static com.facebook.presto.elasticsearch.ElasticsearchErrorCode.ELASTIC_SEARCH_TABLE_DEFINITION_ERROR;
import static com.google.common.base.MoreObjects.firstNonNull;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Strings.isNullOrEmpty;
import static java.nio.file.Files.readAllBytes;
import static java.util.Objects.requireNonNull;

public class ElasticsearchTableDescriptionSupplier
        implements Supplier<Map<SchemaTableName, ElasticsearchTableDescription>>
{
    private final ElasticsearchConnectorConfig config;
    private final JsonCodec<ElasticsearchTableDescription> codec;

    @Inject
    ElasticsearchTableDescriptionSupplier(ElasticsearchConnectorConfig config, JsonCodec<ElasticsearchTableDescription> codec)
    {
        this.config = requireNonNull(config, "config is null");
        this.codec = requireNonNull(codec, "codec is null");
    }

    @Override
    public Map<SchemaTableName, ElasticsearchTableDescription> get()
    {
        return createElasticsearchTableDescriptions(config.getTableDescriptionDir(), config.getDefaultSchema(), ImmutableSet.copyOf(config.getTableNames()), codec);
    }

    public static Map<SchemaTableName, ElasticsearchTableDescription> createElasticsearchTableDescriptions(
            File tableDescriptionDir,
            String defaultSchema,
            Set<String> tableNames,
            JsonCodec<ElasticsearchTableDescription> tableDescriptionCodec)
    {
        ImmutableMap.Builder<SchemaTableName, ElasticsearchTableDescription> builder = ImmutableMap.builder();

        try {
            for (File file : listFiles(tableDescriptionDir)) {
                if (file.isFile() && file.getName().endsWith(".json")) {
                    ElasticsearchTableDescription table = tableDescriptionCodec.fromJson(readAllBytes(file.toPath()));
                    String schemaName = firstNonNull(table.getSchemaName(), defaultSchema);
                    builder.put(new SchemaTableName(schemaName, table.getTableName()), table);
                }
            }

            Map<SchemaTableName, ElasticsearchTableDescription> tableDefinitions = builder.build();

            builder = ImmutableMap.builder();
            for (String definedTable : tableNames) {
                SchemaTableName tableName = parseTableName(definedTable);

                if (!tableDefinitions.containsKey(tableName)) {
                    throw new PrestoException(ELASTIC_SEARCH_TABLE_DEFINITION_ERROR, "Missing table definition for: " + tableName);
                }
                ElasticsearchTableDescription elasticsearchTable = tableDefinitions.get(tableName);
                builder.put(tableName, elasticsearchTable);
            }
            return builder.build();
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static List<File> listFiles(File dir)
    {
        if (dir != null && dir.isDirectory()) {
            File[] files = dir.listFiles();
            if (files != null) {
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
