package com.facebook.presto.hudi.query.index;

import com.facebook.airlift.log.Logger;
import com.facebook.presto.common.predicate.Domain;
import com.facebook.presto.common.predicate.TupleDomain;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.SchemaTableName;
import org.apache.hudi.avro.model.HoodieMetadataColumnStats;
import org.apache.hudi.common.model.HoodieIndexDefinition;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.util.HoodieTimer;
import org.apache.hudi.common.util.hash.ColumnIndexID;
import org.apache.hudi.metadata.HoodieTableMetadata;
import org.apache.hudi.metadata.HoodieTableMetadataUtil;
import org.apache.hudi.util.Lazy;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.facebook.presto.hudi.util.TupleDomainUtils.areSomeFieldsReferenced;
import static com.facebook.presto.hudi.util.TupleDomainUtils.filter;
import static com.facebook.presto.hudi.util.TupleDomainUtils.hasSimpleNullCheck;

public class HudiPartitionStatsIndexSupport
        extends HudiColumnStatsIndexSupport
{
    private static final Logger log = Logger.get(HudiColumnStatsIndexSupport.class);
    private final Lazy<HoodieTableMetadata> lazyMetadataTable;

    public HudiPartitionStatsIndexSupport(ConnectorSession session, SchemaTableName schemaTableName, Lazy<HoodieTableMetaClient> lazyMetaClient, Lazy<HoodieTableMetadata> lazyTableMetadata, TupleDomain<String> regularColumnPredicates)
    {
        super(log, session, schemaTableName, lazyMetaClient, lazyTableMetadata, regularColumnPredicates);
        this.lazyMetadataTable = lazyTableMetadata;
    }

    public Optional<Stream<String>> prunePartitions(
            Collection<String> allPartitions)
    {
        HoodieTimer timer = HoodieTimer.start();

        // Filter out predicates containing simple null checks (`IS NULL` or `IS NOT NULL`)
        TupleDomain<String> filteredRegularPredicates = filter(regularColumnPredicates, (ignored, domain) -> !hasSimpleNullCheck(domain));

        // Sanity check, if no regular domains, return immediately
        if (filteredRegularPredicates.getDomains().isEmpty()) {
            timer.endTimer();
            return Optional.empty();
        }

        List<String> regularColumns = new ArrayList<>(filteredRegularPredicates.getDomains().get().keySet());

        // Get columns to filter on
        List<String> encodedTargetColumnNames = regularColumns.stream()
                .map(col -> new ColumnIndexID(col).asBase64EncodedString()).toList();

        Map<String, Type> columnTypes = regularColumnPredicates.getDomains().get().entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getKey, entry -> entry.getValue().getType()));

        // Map of domains with partition stats keyed by partition name and column name
        Map<String, Map<String, Domain>> domainsWithStats = lazyMetadataTable.get().getRecordsByKeyPrefixes(
                        encodedTargetColumnNames,
                        HoodieTableMetadataUtil.PARTITION_NAME_PARTITION_STATS, true)
                .collectAsList()
                .stream()
                .filter(f -> f.getData().getColumnStatMetadata().isPresent())
                .map(f -> f.getData().getColumnStatMetadata().get())
                .collect(Collectors.groupingBy(
                        HoodieMetadataColumnStats::getFileName,
                        Collectors.toMap(
                                HoodieMetadataColumnStats::getColumnName,
                                // Pre-compute the Domain object for each HoodieMetadataColumnStats
                                stats -> getDomainFromColumnStats(stats.getColumnName(), columnTypes.get(stats.getColumnName()), stats))));

        // For each partition, determine if it should be kept based on stats availability and predicate evaluation
        Stream<String> prunedPartitions = allPartitions.stream()
                .filter(partition -> {
                    // Check if stats exist for this partition
                    Map<String, Domain> partitionDomainsWithStats = domainsWithStats.get(partition);
                    if (partitionDomainsWithStats == null) {
                        // Partition has no stats in the index, keep it
                        return true;
                    }
                    else {
                        // Partition has stats, evaluate the predicate against them
                        // Keep the partition only if the predicate evaluates to true
                        // Important: If some columns in encodedTargetColumnNames is not available in partition stats, partition will not be pruned iff all available predicate
                        // evaluates to true. Since we cannot determine if the predicate will evaluate to true or not on the missing stat, adopt conservative measure to true,
                        // i.e. to not prune
                        return evaluateStatisticPredicate(filteredRegularPredicates, partitionDomainsWithStats, regularColumns);
                    }
                });

        log.info("Took %s ms to prune partitions using Partition Stats Index for table %s", timer.endTimer(), schemaTableName);
        return Optional.of(prunedPartitions);
    }

    @Override
    public boolean isIndexSupportAvailable()
    {
        return lazyMetaClient.get().getTableConfig().getMetadataPartitions()
                .contains(HoodieTableMetadataUtil.PARTITION_NAME_PARTITION_STATS);
    }

    @Override
    public boolean canApply(TupleDomain<String> tupleDomain)
    {
        // Important: has the same implementation as col stats superclass, only difference is that log messages are different
        if (!isIndexSupportAvailable()) {
            log.debug("Partition Stats Index partition is not enabled in metadata table.");
            return false;
        }

        Map<String, HoodieIndexDefinition> indexDefinitions = getAllIndexDefinitions();
        HoodieIndexDefinition partitionsStatsIndex = indexDefinitions.get(HoodieTableMetadataUtil.PARTITION_NAME_COLUMN_STATS);
        if (partitionsStatsIndex == null || partitionsStatsIndex.getSourceFields() == null || partitionsStatsIndex.getSourceFields().isEmpty()) {
            log.warn("Partition stats index definition is missing or has no source fields defined");
            return false;
        }

        // Optimization applied: Only consider applicable if predicates reference indexed columns
        List<String> sourceFields = partitionsStatsIndex.getSourceFields();
        boolean applicable = areSomeFieldsReferenced(tupleDomain, sourceFields);

        if (applicable) {
            log.debug("Partition Stats Index is available and applicable (predicates reference indexed columns).");
        }
        else {
            log.debug("Partition Stats Index is available, but predicates do not reference any indexed columns.");
        }
        return applicable;
    }
}