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
package com.facebook.presto.maxcompute;

import com.aliyun.odps.NoSuchObjectException;
import com.aliyun.odps.Odps;
import com.aliyun.odps.OdpsException;
import com.aliyun.odps.Partition;
import com.aliyun.odps.PartitionSpec;
import com.aliyun.odps.Table;
import com.aliyun.odps.account.Account;
import com.aliyun.odps.account.AliyunAccount;
import com.aliyun.odps.tunnel.TableTunnel;
import com.aliyun.odps.tunnel.TableTunnel.DownloadSession;
import com.facebook.airlift.log.Logger;
import com.facebook.airlift.stats.CounterStat;
import com.facebook.presto.common.predicate.NullableValue;
import com.facebook.presto.common.predicate.TupleDomain;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.maxcompute.util.MaxComputePredicateUtils;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorInsertTableHandle;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorTableMetadata;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.SchemaTableName;
import com.google.common.base.Predicates;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import org.apache.commons.lang3.tuple.Pair;
import org.weakref.jmx.Managed;
import org.weakref.jmx.Nested;

import javax.inject.Inject;

import java.io.File;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Predicate;

import static com.facebook.airlift.concurrent.Threads.daemonThreadsNamed;
import static com.facebook.presto.maxcompute.util.MaxComputePredicateUtils.convertToPartitionSpecRange;
import static com.facebook.presto.maxcompute.util.MaxComputePredicateUtils.convertToPredicate;
import static com.facebook.presto.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.Executors.newFixedThreadPool;

public class MaxComputeClient
{
    private static final Logger log = Logger.get(MaxComputeRecordCursor.class);
    private final String connectorId;
    private final TableTunnel tableTunnel;
    private final MaxComputeConfig maxComputeConfig;
    private final Cache<String, Long> recordCountCache;
    private final Cache<MaxComputeTable, List<Partition>> tablePartitionCache;
    private final AtomicInteger downloadingThreads = new AtomicInteger();
    private final ExecutorService executorService;
    private final CounterStat recordCountCacheHit = new CounterStat();
    private final CounterStat recordCountCacheMiss = new CounterStat();
    private final CounterStat tablePartitionCacheHit = new CounterStat();
    private final CounterStat tablePartitionCacheMiss = new CounterStat();

    @Inject
    MaxComputeClient(MaxComputeConnectorId connectorId, MaxComputeConfig config)
    {
        this.connectorId = requireNonNull(connectorId, "connectorId is null").toString();
        this.maxComputeConfig = config;
        this.tableTunnel = createTableTunnel();
        recordCountCache = CacheBuilder
                .newBuilder()
                .expireAfterAccess(15, TimeUnit.DAYS)
                .build();
        tablePartitionCache = CacheBuilder
                .newBuilder()
                .maximumSize(200_000)
                .expireAfterAccess(24, TimeUnit.HOURS)
                .build();
        executorService = newFixedThreadPool(maxComputeConfig.getPartitionCacheThreads(), daemonThreadsNamed("partition-cache-loader-%s"));
    }

    public static Odps createOdps(MaxComputeConfig config)
    {
        return createOdps(config, config.getDefaultProject());
    }

    public static Odps createOdps(MaxComputeConfig config, String defaultProject)
    {
        Account account = new AliyunAccount(config.getAccessKeyId(), config.getAccessKeySecret());
        Odps odps = new Odps(account);
        odps.setEndpoint(config.getEndpoint());
        odps.setDefaultProject(defaultProject == null ? config.getDefaultProject() : defaultProject);
        odps.setUserAgent("presto");
        log.info("Created an Odps for project: %s", defaultProject);
        return odps;
    }

    public ConnectorInsertTableHandle beginInsertTable(
            ConnectorSession session,
            MaxComputeMetadata maxComputeMeta,
            ConnectorTableMetadata tableMetadata)
    {
        SchemaTableName schemaTableName = tableMetadata.getTable();
        String schema = schemaTableName.getSchemaName();
        String table = schemaTableName.getTableName();

        List<MaxComputeColumnHandle> columns = maxComputeMeta.getOrderedColumns(session, schemaTableName);
        ImmutableList.Builder<Type> typeBuilder = ImmutableList.builder();

        for (MaxComputeColumnHandle col : columns) {
            typeBuilder.add(col.getColumnType());
        }

        return new MaxComputeInsertTableHandle(
                connectorId,
                schema,
                table,
                columns,
                typeBuilder.build());
    }

    public TableTunnel getTableTunnel(ConnectorSession session, String project)
    {
        return tableTunnel;
    }

    public Table getTableMeta(String project, String tableName)
    {
        Odps odps = createOdps(maxComputeConfig, project);

        Table targetTable;
        try {
            targetTable = odps.tables().get(tableName);
            targetTable.reload();
        }
        catch (NoSuchObjectException e) {
            throw new RuntimeException("Target table was not found: " + tableName, e);
        }
        catch (OdpsException e) {
            throw new RuntimeException("Finding table error: " + tableName + ", due to: " + e.getMessage(), e);
        }

        return targetTable;
    }

    public DownloadSession getDownloadSession(TableTunnel tunnel,
            String projectName,
            String tableName)
    {
        for (int retry = 3; retry > 0; retry--) {
            try {
                return tunnel.createDownloadSession(projectName, tableName);
            }
            catch (Exception e) {
                if (retry <= 1) {
                    throw new RuntimeException(e);
                }
            }
        }
        return null;
    }

    @Managed
    @Nested
    public CounterStat getRecordCountCacheHit()
    {
        return recordCountCacheHit;
    }

    @Managed
    @Nested
    public CounterStat getRecordCountCacheMiss()
    {
        return recordCountCacheMiss;
    }

    @Managed
    @Nested
    public CounterStat getTablePartitionCacheHit()
    {
        return tablePartitionCacheHit;
    }

    @Managed
    @Nested
    public CounterStat getTablePartitionCacheMiss()
    {
        return tablePartitionCacheMiss;
    }

    public Long getRecordCount(String projectName, String tableName, PartitionSpec partSpec, Date lastDataModifiedTime)
    {
        Long l = recordCountCache.getIfPresent(getPartitionString(projectName, tableName, partSpec, lastDataModifiedTime));
        if (l == null) {
            recordCountCacheMiss.update(1);
        }
        else {
            recordCountCacheHit.update(1);
        }
        return l;
    }

    public void cacheRecordCount(String projectName, String tableName, PartitionSpec partSpec, Date lastDataModifiedTime, Long count)
    {
        recordCountCache.put(getPartitionString(projectName, tableName, partSpec, lastDataModifiedTime), count);
    }

    public List<Partition> getTablePartitions(Table table, Optional<TupleDomain<ColumnHandle>> predicates, List<MaxComputeColumnHandle> partitionColumns, boolean testIgnorePartitionCache)
    {
        if ((!predicates.isPresent()) || predicates.get().isAll()) {
            return getTablePartitions(table);
        }
        else {
            TupleDomain<ColumnHandle> partitionPredicates = TupleDomain.withColumnDomains(
                    Maps.filterKeys(predicates.get().getDomains().get(), Predicates.in(partitionColumns)));
            Optional<Pair<PartitionSpec, PartitionSpec>> partitionSpecRange = convertToPartitionSpecRange(partitionPredicates, partitionColumns);
            List<Partition> tableParts = getTablePartitions(table, partitionSpecRange, testIgnorePartitionCache);
            List<Partition> matchedPartitions = new ArrayList<>();

            Predicate<Map<ColumnHandle, NullableValue>> predicate = convertToPredicate(partitionPredicates);
            // Have predicate, find the matched partition spec if any.
            for (Partition part : tableParts) {
                PartitionSpec partSpec = part.getPartitionSpec();
                if (MaxComputePredicateUtils.matchPartition(partSpec, partitionPredicates, partitionColumns, predicate)) {
                    matchedPartitions.add(part);
                }
            }
            return matchedPartitions;
        }
    }

    private List<Partition> getTablePartitions(Table table, Optional<Pair<PartitionSpec, PartitionSpec>> partitionSpecRange, boolean testIgnorePartitionCache)
    {
        if (!partitionSpecRange.isPresent()) {
            return getTablePartitions(table);
        }
        else {
            MaxComputeTable maxComputeTable = new MaxComputeTable(table);
            List<Partition> allPartitions = tablePartitionCache.getIfPresent(maxComputeTable);
            if (allPartitions != null && !testIgnorePartitionCache) {
                tablePartitionCacheHit.update(1);
                log.debug("Table %s getPartitions hit cache", maxComputeTable);
                return allPartitions; ///since the partitions will be filtered later, it's ok to return all partitions.
            }

            try {
                List<Partition> partitions = new ArrayList<>();
                PartitionSpec low = partitionSpecRange.get().getLeft();
                PartitionSpec high = partitionSpecRange.get().getRight();
                partitions.addAll(table.getPartitions(low, high));
                executorService.submit(() -> getTablePartitions(table));
                tablePartitionCacheMiss.update(1);
                return partitions;
            }
            catch (Exception e) {
                log.warn(e, "getPartitionIterator failed, partitionSpec = %s", partitionSpecRange.get());
                return getTablePartitions(table);
            }
        }
    }

    private List<Partition> getTablePartitions(Table table)
    {
        MaxComputeTable maxComputeTable = new MaxComputeTable(table);
        try {
            List<Partition> partitions = tablePartitionCache.getIfPresent(maxComputeTable);
            if (partitions != null) {
                tablePartitionCacheHit.update(1);
                log.debug("Table %s getPartitions hit cache", maxComputeTable);
                return partitions;
            }
            long startTime = System.currentTimeMillis();
            partitions = table.getPartitions();
            //preload to avoid concurrent problem
            partitions.forEach(partition -> partition.getPartitionSpec());
            tablePartitionCache.put(maxComputeTable, partitions);
            tablePartitionCacheMiss.update(1);
            log.debug("Table %s getPartitions with %d partitions cost %d ms", maxComputeTable, partitions.size(), System.currentTimeMillis() - startTime);
            return partitions;
        }
        catch (Exception e) {
            throw new PrestoException(GENERIC_INTERNAL_ERROR, String.format("getTablePartitions error: %s, table=%s", e.getMessage(), maxComputeTable), e);
        }
    }

    private String getPartitionString(String projectName, String tableName, PartitionSpec partSpec, Date lastDataModifiedTime)
    {
        return new StringBuilder(projectName)
                .append("/")
                .append(tableName)
                .append("/")
                .append(partSpec)
                .append("/")
                .append(lastDataModifiedTime)
                .toString();
    }

    public String getConnectorDataPath(String path)
    {
        if (path.endsWith(File.separator)) {
            return path + connectorId;
        }
        return path + File.separator + connectorId;
    }

    public void incrementDownloadingThreads()
    {
        downloadingThreads.incrementAndGet();
    }

    public void decrementDownloadingThreads()
    {
        downloadingThreads.decrementAndGet();
    }

    public DownloadSession getDownloadSession(TableTunnel tunnel, String projectName,
            String tableName, PartitionSpec partSpec)
    {
        for (int retry = 3; retry > 0; retry--) {
            try {
                return tunnel.createDownloadSession(projectName, tableName, partSpec);
            }
            catch (Exception e) {
                if (retry <= 1) {
                    throw new RuntimeException(e);
                }
            }
        }
        return null;
    }

    public Odps createOdps()
    {
        return createOdps(maxComputeConfig);
    }

    public Odps createOdps(String project)
    {
        return createOdps(maxComputeConfig, project);
    }

    private TableTunnel createTableTunnel()
    {
        Odps odps = createOdps(maxComputeConfig);
        TableTunnel tunnel = new TableTunnel(odps);
        tunnel.setEndpoint(maxComputeConfig.getTunnelURL());
        log.info("TableTunnel created. ");
        return tunnel;
    }
}
