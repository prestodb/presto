package com.facebook.presto.hudi.query.index;

import com.facebook.airlift.log.Logger;
import com.facebook.airlift.units.Duration;
import com.facebook.presto.common.predicate.TupleDomain;
import com.facebook.presto.hudi.util.TupleDomainUtils;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.SchemaTableName;
import org.apache.hudi.common.data.HoodieListData;
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.model.HoodieIndexDefinition;
import org.apache.hudi.common.model.HoodieRecordGlobalLocation;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.util.HoodieDataUtils;
import org.apache.hudi.common.util.HoodieTimer;
import org.apache.hudi.metadata.HoodieTableMetadata;
import org.apache.hudi.metadata.HoodieTableMetadataUtil;
import org.apache.hudi.util.Lazy;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

import static com.facebook.presto.hudi.HudiSessionProperties.getSecondaryIndexWaitTimeout;
import static com.facebook.presto.hudi.query.index.HudiRecordLevelIndexSupport.constructRecordKeys;
import static com.facebook.presto.hudi.query.index.HudiRecordLevelIndexSupport.extractPredicatesForColumns;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class HudiSecondaryIndexSupport
        extends HudiBaseIndexSupport
{
    private static final Logger log = Logger.get(HudiSecondaryIndexSupport.class);
    private final CompletableFuture<Optional<Set<String>>> relevantFileIdsFuture;
    private final Duration secondaryIndexWaitTimeout;
    private final long futureStartTimeMs;

    public HudiSecondaryIndexSupport(ConnectorSession session, SchemaTableName schemaTableName, Lazy<HoodieTableMetaClient> lazyMetaClient, Lazy<HoodieTableMetadata> lazyTableMetadata, TupleDomain<String> regularColumnPredicates)
    {
        super(log, schemaTableName, lazyMetaClient);
        this.secondaryIndexWaitTimeout = getSecondaryIndexWaitTimeout(session);
        this.relevantFileIdsFuture = CompletableFuture.supplyAsync(() -> {
            HoodieTimer timer = HoodieTimer.start();
            if (regularColumnPredicates.isAll() || lazyMetaClient.get().getIndexMetadata().isEmpty()) {
                log.debug("Predicates cover all data, skipping secondary index lookup.");
                return Optional.empty();
            }

            Optional<Map.Entry<String, HoodieIndexDefinition>> firstApplicableIndex = findFirstApplicableSecondaryIndex(regularColumnPredicates);
            if (firstApplicableIndex.isEmpty()) {
                log.debug("Took %s ms but no secondary index definition found matching the query's referenced columns for table %s",
                        timer.endTimer(), schemaTableName);
                return Optional.empty();
            }

            Map.Entry<String, HoodieIndexDefinition> applicableIndexEntry = firstApplicableIndex.get();
            String indexName = applicableIndexEntry.getKey();
            // `indexedColumns` should only contain one element as secondary indices only support one column
            List<String> indexedColumns = applicableIndexEntry.getValue().getSourceFields();
            log.debug(String.format("Using secondary index '%s' on columns %s for pruning.", indexName, indexedColumns));
            TupleDomain<String> indexPredicates = extractPredicatesForColumns(regularColumnPredicates, indexedColumns);

            List<String> secondaryKeys = constructRecordKeys(indexPredicates, indexedColumns);
            if (secondaryKeys.isEmpty()) {
                log.warn("Took %s ms, but could not construct secondary keys for index '%s' from predicates. Skipping pruning for table %s",
                        timer.endTimer(), indexName, schemaTableName);
                return Optional.empty();
            }
            log.debug(String.format("Constructed %d secondary keys for index lookup.", secondaryKeys.size()));

            // Perform index lookup in metadataTable
            // TODO: document here what this map is keyed by
            Map<String, HoodieRecordGlobalLocation> recordKeyLocationsMap =
                    HoodieDataUtils.dedupeAndCollectAsMap(lazyTableMetadata.get().readSecondaryIndexLocationsWithKeys(
                            HoodieListData.lazy(secondaryKeys), indexName));
            if (recordKeyLocationsMap.isEmpty()) {
                log.debug("Took %s ms, but secondary index lookup returned no locations for the given keys for table %s", timer.endTimer(), schemaTableName);
                // Return all original fileSlices
                return Optional.empty();
            }

            // Collect fileIds for pruning
            Set<String> relevantFileIds = recordKeyLocationsMap.values().stream()
                    .map(HoodieRecordGlobalLocation::getFileId)
                    .collect(Collectors.toSet());
            log.debug(String.format("Secondary index lookup identified %d relevant file IDs.", relevantFileIds.size()));

            return Optional.of(relevantFileIds);
        });

        this.futureStartTimeMs = System.currentTimeMillis();
    }

    @Override
    public boolean shouldSkipFileSlice(FileSlice slice)
    {
        try {
            if (relevantFileIdsFuture.isDone()) {
                Optional<Set<String>> relevantFileIds = relevantFileIdsFuture.get();
                return relevantFileIds.map(fileIds -> !fileIds.contains(slice.getFileId())).orElse(false);
            }

            long elapsedMs = System.currentTimeMillis() - futureStartTimeMs;
            if (elapsedMs > secondaryIndexWaitTimeout.toMillis()) {
                // Took too long; skip decision
                return false;
            }

            long remainingMs = Math.max(0, secondaryIndexWaitTimeout.toMillis() - elapsedMs);
            Optional<Set<String>> relevantFileIds = relevantFileIdsFuture.get(remainingMs, MILLISECONDS);
            return relevantFileIds.map(fileIds -> !fileIds.contains(slice.getFileId())).orElse(false);
        }
        catch (TimeoutException | InterruptedException | ExecutionException e) {
            return false;
        }
    }

    /**
     * Determines whether secondary index (SI) should be used based on the given tuple domain and index definitions.
     * <p>
     * This method first filters out the secondary index definitions from the provided map of index definitions.
     * It then checks if there are any secondary indices defined. If no secondary indices are found, it returns {@code false}.
     * <p>
     * For each secondary index definition, the method verifies two conditions:
     * <ol>
     * <li>All fields referenced in the tuple domain must be part of the source fields of the secondary index.</li>
     * <li>The predicates on these fields must be either of type IN or EQUAL.</li>
     * </ol>
     * <p>
     * If at least one secondary index definition meets these conditions, the method returns {@code true}; otherwise,
     * it returns {@code false}.
     *
     * @param tupleDomain the domain representing the constraints on the columns
     * HoodieIndexDefinition object
     * @return {@code true} if at least one secondary index can be used based on the given tuple domain; otherwise,
     * {@code false}
     */
    @Override
    public boolean canApply(TupleDomain<String> tupleDomain)
    {
        if (!isIndexSupportAvailable()) {
            log.debug("Secondary Index partition is not enabled in metadata.");
            return false;
        }

        Map<String, HoodieIndexDefinition> secondaryIndexDefinitions = getApplicableIndexDefinitions(tupleDomain, true);
        if (secondaryIndexDefinitions.isEmpty()) {
            log.debug("No applicable secondary index definitions found.");
            return false;
        }

        boolean atLeastOneIndexUsable = secondaryIndexDefinitions.values().stream()
                .anyMatch(indexDef -> {
                    List<String> sourceFields = indexDef.getSourceFields();
                    // Predicates referencing columns with secondary index needs to be IN or EQUAL only
                    boolean usable = TupleDomainUtils.areAllFieldsReferenced(tupleDomain, sourceFields)
                            && TupleDomainUtils.areDomainsInOrEqualOnly(tupleDomain, sourceFields);
                    if (log.isDebugEnabled() && usable) {
                        log.debug(String.format("Secondary index '%s' on fields '%s' is usable for the query.", indexDef.getIndexName(), sourceFields));
                    }
                    return usable;
                });
        if (!atLeastOneIndexUsable) {
            log.debug("Although secondary indexes exist, none match the required fields and predicate types (IN/EQUAL) for the query.");
        }
        return atLeastOneIndexUsable;
    }

    private boolean isIndexSupportAvailable()
    {
        // Filter out definitions that are secondary indices
        Map<String, HoodieIndexDefinition> secondaryIndexDefinitions = getAllIndexDefinitions()
                .entrySet().stream()
                .filter(e -> e.getKey().contains(HoodieTableMetadataUtil.PARTITION_NAME_SECONDARY_INDEX))
                .collect(Collectors.toMap(e -> e.getValue().getIndexName(),
                        Map.Entry::getValue));
        return !secondaryIndexDefinitions.isEmpty();
    }

    private Map<String, HoodieIndexDefinition> getApplicableIndexDefinitions(TupleDomain<String> tupleDomain, boolean checkPredicateCompatibility)
    {
        Map<String, HoodieIndexDefinition> allDefinitions = getAllIndexDefinitions();
        if (allDefinitions.isEmpty()) {
            return Map.of();
        }
        // Filter out definitions that are secondary indices
        return allDefinitions.entrySet().stream()
                .filter(entry -> entry.getKey().contains(HoodieTableMetadataUtil.PARTITION_NAME_SECONDARY_INDEX))
                .filter(entry -> {
                    if (!checkPredicateCompatibility) {
                        return true;
                    }
                    // Perform additional compatibility checks
                    List<String> sourceFields = entry.getValue().getSourceFields();
                    return TupleDomainUtils.areAllFieldsReferenced(tupleDomain, sourceFields)
                            && TupleDomainUtils.areDomainsInOrEqualOnly(tupleDomain, sourceFields);
                })
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    private Optional<Map.Entry<String, HoodieIndexDefinition>> findFirstApplicableSecondaryIndex(TupleDomain<String> queryPredicates)
    {
        // Predicate checks would have already been done, skip predicate checks here
        Map<String, HoodieIndexDefinition> secondaryIndexDefinitions = getApplicableIndexDefinitions(queryPredicates, false);
        if (queryPredicates.getDomains().isEmpty()) {
            return Optional.empty();
        }
        List<String> queryReferencedColumns = List.copyOf(queryPredicates.getDomains().get().keySet());
        return secondaryIndexDefinitions.entrySet().stream()
                .filter(entry -> {
                    List<String> sourceFields = entry.getValue().getSourceFields();
                    // Only filter for sourceFields that match the predicates
                    return !sourceFields.isEmpty() && queryReferencedColumns.contains(sourceFields.get(0));
                })
                .findFirst();
    }
}