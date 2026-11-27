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
package com.facebook.presto.hudi.common;

import com.facebook.presto.common.predicate.Domain;
import com.facebook.presto.common.type.Type;
import org.apache.hudi.avro.model.HoodieMetadataColumnStats;
import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.engine.HoodieLocalEngineContext;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.TableSchemaResolver;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.metadata.HoodieBackedTableMetadata;
import org.apache.hudi.org.apache.avro.Schema;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.BooleanType.BOOLEAN;
import static com.facebook.presto.common.type.IntegerType.INTEGER;
import static com.facebook.presto.hudi.common.HudiColumnStatsConverter.toDomain;
import static java.util.Objects.requireNonNull;
import static org.apache.hudi.metadata.MetadataPartitionType.COLUMN_STATS;

/**
 * Reads the latest MDT view. It is deliberately not a snapshot-bound reader.
 * Each bounded batch owns and closes its readers, including when a scan stops
 * before exhausting its file iterator. No reader state is shared between threads.
 */
public final class LatestHudiColumnStatsReader
        implements HudiColumnStatsReader
{
    private final HoodieTableMetaClient metaClient;

    public LatestHudiColumnStatsReader(HoodieTableMetaClient metaClient)
    {
        this.metaClient = requireNonNull(metaClient, "metaClient is null");
    }

    @Override
    public Map<Pair<String, String>, Map<String, Domain>> readColumnStats(
            List<Pair<String, String>> files,
            Map<String, Type> columns)
            throws IOException
    {
        if (files.isEmpty() || columns.isEmpty() || !metaClient.getTableConfig().isMetadataPartitionAvailable(COLUMN_STATS)) {
            return Map.of();
        }
        Schema schema;
        try {
            schema = new TableSchemaResolver(metaClient).getTableAvroSchema();
        }
        catch (Exception e) {
            throw new IOException("Cannot resolve the Hudi schema for statistics", e);
        }
        Map<String, Type> supportedColumns = new HashMap<>();
        columns.forEach((name, type) -> {
            Schema.Field field = schema.getField(name);
            if (field != null && matchesType(field.schema(), type)) {
                supportedColumns.put(name, type);
            }
        });
        if (supportedColumns.isEmpty()) {
            return Map.of();
        }

        // A reader per batch avoids leaking MDT handles when an iterator is abandoned.
        // The connector's storage timeouts also apply to this synchronous lookup.
        try (HoodieBackedTableMetadata metadata = new HoodieBackedTableMetadata(
                new HoodieLocalEngineContext(metaClient.getStorageConf()),
                metaClient.getStorage(),
                HoodieMetadataConfig.newBuilder().enable(true).build(),
                metaClient.getBasePath().toString())) {
            Map<Pair<String, String>, List<HoodieMetadataColumnStats>> statistics =
                    metadata.getColumnStats(files, List.copyOf(supportedColumns.keySet()));
            Map<Pair<String, String>, Map<String, Domain>> result = new HashMap<>();
            for (Pair<String, String> file : files) {
                Map<String, Domain> domains = new HashMap<>();
                for (HoodieMetadataColumnStats statistic : statistics.getOrDefault(file, List.of())) {
                    Type type = supportedColumns.get(statistic.getColumnName());
                    if (type != null && file.getRight().equals(statistic.getFileName())) {
                        // Duplicate entries are ambiguous, so their combined domain is unconstrained.
                        domains.merge(statistic.getColumnName(), toDomain(type, statistic), (left, right) -> Domain.all(type));
                    }
                }
                result.put(file, Map.copyOf(domains));
            }
            return Map.copyOf(result);
        }
    }

    private static boolean matchesType(Schema schema, Type type)
    {
        if (schema.getType() == Schema.Type.UNION) {
            List<Schema> nonNullTypes = schema.getTypes().stream()
                    .filter(member -> member.getType() != Schema.Type.NULL)
                    .toList();
            return nonNullTypes.size() == 1 && matchesType(nonNullTypes.get(0), type);
        }
        if (schema.getLogicalType() != null) {
            return false;
        }
        // Reject schema coercions. Narrow integer metastore types are also left unpruned.
        return (type.equals(BOOLEAN) && schema.getType() == Schema.Type.BOOLEAN) ||
                (type.equals(INTEGER) && schema.getType() == Schema.Type.INT) ||
                (type.equals(BIGINT) && schema.getType() == Schema.Type.LONG);
    }
}
