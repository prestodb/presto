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

package com.facebook.presto.hudi;

import com.facebook.airlift.log.Logger;
import com.facebook.presto.common.predicate.Domain;
import com.facebook.presto.common.predicate.TupleDomain;
import com.facebook.presto.common.predicate.ValueSet;
import com.facebook.presto.common.type.StandardTypes;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.TypeManager;
import com.facebook.presto.hive.metastore.Column;
import com.facebook.presto.hive.metastore.ExtendedHiveMetastore;
import com.facebook.presto.hive.metastore.MetastoreContext;
import com.facebook.presto.hive.metastore.Table;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.PrestoException;
import com.google.common.base.Verify;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.slice.Slice;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.engine.HoodieLocalEngineContext;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.table.HoodieTableMetaClient;

import javax.inject.Inject;

import java.sql.Timestamp;
import java.time.LocalDate;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static com.facebook.presto.hudi.HudiErrorCode.HUDI_INVALID_PARTITION_VALUE;
import static com.facebook.presto.hudi.HudiMetadata.fromPartitionColumns;
import static com.facebook.presto.hudi.HudiMetadata.toMetastoreContext;
import static com.facebook.presto.hudi.HudiSessionProperties.isHudiMetadataTableEnabled;
import static io.airlift.slice.Slices.utf8Slice;
import static java.lang.Double.doubleToRawLongBits;
import static java.lang.Double.parseDouble;
import static java.lang.Float.floatToRawIntBits;
import static java.lang.Float.parseFloat;
import static java.lang.Long.parseLong;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class HudiPartitionManager
{
    private static final Logger log = Logger.get(HudiPartitionManager.class);

    private final TypeManager typeManager;

    private static final Pattern HIVE_PARTITION_NAME_PATTERN = Pattern.compile("([^/]+)=([^/]+)");

    @Inject
    public HudiPartitionManager(TypeManager typeManager)
    {
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
    }

    public List<String> getEffectivePartitions(
            ConnectorSession connectorSession,
            ExtendedHiveMetastore metastore,
            HoodieTableMetaClient metaClient,
            String schemaName,
            String tableName,
            TupleDomain<ColumnHandle> tupleDomain)
    {
        MetastoreContext metastoreContext = toMetastoreContext(connectorSession);
        Optional<Table> table = metastore.getTable(metastoreContext, schemaName, tableName);
        Verify.verify(table.isPresent());
        List<Column> partitionColumns = table.get().getPartitionColumns();

        if (partitionColumns.isEmpty()) {
            return ImmutableList.of("");
        }

        boolean metaTableEnabled = isHudiMetadataTableEnabled(connectorSession);

        return metaTableEnabled ? prunePartitionByMetaDataTable(connectorSession, metaClient, partitionColumns, tupleDomain) :
                prunePartitionByMetaStore(metastore, metastoreContext, schemaName, tableName, partitionColumns, tupleDomain);
    }

    private List<String> prunePartitionByMetaDataTable(
            ConnectorSession connectorSession,
            HoodieTableMetaClient metaClient,
            List<Column> partitionColumns,
            TupleDomain<ColumnHandle> tupleDomain)
    {
        // non-partition table
        if (partitionColumns.isEmpty()) {
            return ImmutableList.of("");
        }
        Configuration conf = metaClient.getHadoopConf();
        HoodieLocalEngineContext engineContext = new HoodieLocalEngineContext(conf);
        HoodieMetadataConfig metadataConfig = HoodieMetadataConfig.newBuilder().enable(true).build();

        // Load all the partition path from the basePath
        List<String> allPartitions = FSUtils.getAllPartitionPaths(engineContext, metadataConfig, metaClient.getBasePathV2().toString());

        // Extract partition columns predicate
        TupleDomain<String> partitionPredicate = tupleDomain.transform(hudiColumnHandle -> {
            if (((HudiColumnHandle) hudiColumnHandle).getColumnType() != HudiColumnHandle.ColumnType.PARTITION_KEY) {
                return null;
            }
            return ((HudiColumnHandle) hudiColumnHandle).getName();
        });

        if (partitionPredicate.isAll()) {
            return allPartitions;
        }

        if (partitionPredicate.isNone()) {
            return ImmutableList.of("");
        }

        List<HudiColumnHandle> partitionColumnHandles = fromPartitionColumns(partitionColumns);

        List<String> matchedPartitionPaths = prunePartitions(partitionPredicate, partitionColumnHandles, getPartitions(partitionColumns.stream().map(f -> f.getName()).collect(Collectors.toList()), allPartitions));
        log.info(format("Total partition size is %s, after partition prune size is %s.", allPartitions.size(), matchedPartitionPaths.size()));
        return matchedPartitionPaths;
    }

    /**
     * Returns the partition path key and values as a list of map.
     * For example:
     * partition keys: [p1, p2, p3],
     * partition paths:
     *      p1=val1/p2=val2/p3=val3  (hive style partition)
     *      p1=val4/p2=val5/p3=val6  (hive style partition)
     * return values {p1=val1/p2=val2/p3=val3 -> {p1 -> val1, p2 -> value2, p3 -> value3}}, {p1=val4/p2=val5/p3=val6 -> {p1 -> val4, p2 -> value5, p3 -> value6}}
     *
     * @param partitionKey  The partition key list
     * @param partitionPaths partition path list
     */
    public static Map<String, Map<String, String>> getPartitions(List<String> partitionKey, List<String> partitionPaths)
    {
        Map<String, Map<String, String>> result = new HashMap<>();
        if (partitionPaths.isEmpty() || partitionKey.isEmpty()) {
            return result;
        }
        // try to infer hive style
        boolean hiveStylePartition = HIVE_PARTITION_NAME_PATTERN.matcher(partitionPaths.get(0).split(Path.SEPARATOR)[0]).matches();
        for (String partitionPath : partitionPaths) {
            String[] pathParts = partitionPath.split(Path.SEPARATOR);
            Map<String, String> partitionMapping = new LinkedHashMap<>();
            if (hiveStylePartition) {
                Arrays.stream(pathParts).forEach(p -> {
                    String[] keyValue = p.split("=");
                    if (keyValue.length == 2) {
                        partitionMapping.put(keyValue[0], keyValue[1]);
                    }
                });
            }
            else {
                for (int i = 0; i < partitionKey.size(); i++) {
                    partitionMapping.put(partitionKey.get(i), pathParts[i]);
                }
            }
            result.put(partitionPath, partitionMapping);
        }
        return result;
    }

    private List<String> prunePartitions(
            TupleDomain<String> partitionPredicate,
            List<HudiColumnHandle> partitionColumnHandles,
            Map<String, Map<String, String>> candidatePartitionPaths)
    {
        return candidatePartitionPaths.entrySet().stream().filter(f -> {
            Map<String, String> partitionMapping = f.getValue();
            return partitionMapping.entrySet().stream().allMatch(p -> evaluatePartitionPredicate(partitionPredicate, partitionColumnHandles, p.getValue(), p.getKey()));
        }).map(entry -> entry.getKey()).collect(Collectors.toList());
    }

    private boolean evaluatePartitionPredicate(TupleDomain<String> partitionPredicate, List<HudiColumnHandle> partitionColumnHandles, String partitionPathValue, String partitionName)
    {
        Optional<HudiColumnHandle> columnHandleOpt = partitionColumnHandles.stream().filter(f -> f.getName().equals(partitionName)).findFirst();
        if (columnHandleOpt.isPresent()) {
            Domain domain = getDomain(columnHandleOpt.get(), partitionPathValue);
            if (!partitionPredicate.getDomains().isPresent()) {
                return true;
            }
            Domain columnPredicate = partitionPredicate.getDomains().get().get(partitionName);
            // no predicate on current partitionName
            if (columnPredicate == null) {
                return true;
            }

            // For null partition, hive will produce a default value for current partition.
            if (partitionPathValue.equals("default")) {
                return true;
            }

            if (columnPredicate.intersect(domain).isNone()) {
                return false;
            }
            return true;
        }
        else {
            // Should not happen
            throw new IllegalArgumentException(format("Mismatched partition information found,"
                            + " partition: %s from Hudi metadataTable is not included by the partitions from HMS: %s",
                    partitionName, partitionColumnHandles.stream().map(f -> f.getName()).collect(Collectors.joining(","))));
        }
    }

    private Domain getDomain(HudiColumnHandle columnHandle, String partitionValue)
    {
        Type type = columnHandle.getHiveType().getType(typeManager);
        if (partitionValue == null) {
            return Domain.onlyNull(type);
        }
        try {
            switch (columnHandle.getHiveType().getTypeSignature().getBase()) {
                case StandardTypes.TINYINT:
                case StandardTypes.SMALLINT:
                case StandardTypes.INTEGER:
                case StandardTypes.BIGINT:
                    Long intValue = parseLong(partitionValue);
                    return Domain.create(ValueSet.of(type, intValue), false);
                case StandardTypes.REAL:
                    Long realValue = (long) floatToRawIntBits(parseFloat(partitionValue));
                    return Domain.create(ValueSet.of(type, realValue), false);
                case StandardTypes.DOUBLE:
                    Long doubleValue = doubleToRawLongBits(parseDouble(partitionValue));
                    return Domain.create(ValueSet.of(type, doubleValue), false);
                case StandardTypes.VARCHAR:
                case StandardTypes.VARBINARY:
                    Slice sliceValue = utf8Slice(partitionValue);
                    return Domain.create(ValueSet.of(type, sliceValue), false);
                case StandardTypes.DATE:
                    Long dateValue = LocalDate.parse(partitionValue, java.time.format.DateTimeFormatter.ISO_LOCAL_DATE).toEpochDay();
                    return Domain.create(ValueSet.of(type, dateValue), false);
                case StandardTypes.TIMESTAMP:
                    Long timestampValue = Timestamp.valueOf(partitionValue).getTime();
                    return Domain.create(ValueSet.of(type, timestampValue), false);
                case StandardTypes.BOOLEAN:
                    Boolean booleanValue = Boolean.valueOf(partitionValue);
                    return Domain.create(ValueSet.of(type, booleanValue), false);
                default:
                    throw new PrestoException(HUDI_INVALID_PARTITION_VALUE, format(
                            "partition data type '%s' is unsupported for partition key: %s",
                            columnHandle.getHiveType(),
                            columnHandle.getName()));
            }
        }
        catch (IllegalArgumentException e) {
            throw new PrestoException(HUDI_INVALID_PARTITION_VALUE, format(
                    "Invalid partition value '%s' for %s partition key: %s",
                    partitionValue,
                    type.getDisplayName(),
                    columnHandle.getName()));
        }
    }

    private List<String> prunePartitionByMetaStore(
            ExtendedHiveMetastore metastore,
            MetastoreContext metastoreContext,
            String schemaName,
            String tableName,
            List<Column> partitionColumns,
            TupleDomain<ColumnHandle> tupleDomain)
    {
        Map<Column, Domain> partitionPredicate = new HashMap<>();
        Map<ColumnHandle, Domain> domains = tupleDomain.getDomains().orElse(ImmutableMap.of());

        List<HudiColumnHandle> hudiColumnHandles = fromPartitionColumns(partitionColumns);

        for (int i = 0; i < hudiColumnHandles.size(); i++) {
            HudiColumnHandle column = hudiColumnHandles.get(i);
            Column partitionColumn = partitionColumns.get(i);
            if (domains.containsKey(column)) {
                partitionPredicate.put(partitionColumn, domains.get(column));
            }
            else {
                partitionPredicate.put(partitionColumn, Domain.all(column.getHiveType().getType(typeManager)));
            }
        }
        return metastore.getPartitionNamesByFilter(metastoreContext, schemaName, tableName, partitionPredicate);
    }
}
