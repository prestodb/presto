package com.facebook.presto.hudi.query.index;

import com.facebook.airlift.log.Logger;
import com.facebook.airlift.units.Duration;
import com.facebook.presto.common.predicate.Domain;
import com.facebook.presto.common.predicate.TupleDomain;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.SchemaTableName;
import io.airlift.slice.Slice;
import org.apache.hudi.common.data.HoodieListData;
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.model.HoodieRecordGlobalLocation;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.util.HoodieDataUtils;
import org.apache.hudi.common.util.HoodieTimer;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.metadata.HoodieTableMetadata;
import org.apache.hudi.metadata.HoodieTableMetadataUtil;
import org.apache.hudi.util.Lazy;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

import static com.facebook.presto.hudi.HudiErrorCode.HUDI_BAD_DATA;
import static com.facebook.presto.hudi.HudiSessionProperties.getRecordIndexWaitTimeout;
import static com.facebook.presto.hudi.util.TupleDomainUtils.areAllFieldsReferenced;
import static com.facebook.presto.hudi.util.TupleDomainUtils.areDomainsInOrEqualOnly;
import static com.facebook.presto.hudi.util.TupleDomainUtils.getDiscreteSet;
import static com.facebook.presto.hudi.util.TupleDomainUtils.isDiscreteSet;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class HudiRecordLevelIndexSupport
        extends HudiBaseIndexSupport
{
    private static final Logger log = Logger.get(HudiRecordLevelIndexSupport.class);

    public static final String DEFAULT_COLUMN_VALUE_SEPARATOR = ":";
    public static final String DEFAULT_RECORD_KEY_PARTS_SEPARATOR = ",";
    private final CompletableFuture<Optional<Set<String>>> relevantFileIdsFuture;
    private final Duration recordIndexWaitTimeout;
    private final long futureStartTimeMs;

    public HudiRecordLevelIndexSupport(ConnectorSession session, SchemaTableName schemaTableName, Lazy<HoodieTableMetaClient> lazyMetaClient, Lazy<HoodieTableMetadata> lazyTableMetadata, TupleDomain<String> regularColumnPredicates)
    {
        super(log, schemaTableName, lazyMetaClient);
        this.recordIndexWaitTimeout = getRecordIndexWaitTimeout(session);
        if (regularColumnPredicates.isAll()) {
            log.debug("Predicates cover all data, skipping record level index lookup.");
            this.relevantFileIdsFuture = CompletableFuture.completedFuture(Optional.empty());
        }
        else {
            this.relevantFileIdsFuture = CompletableFuture.supplyAsync(() -> {
                HoodieTimer timer = HoodieTimer.start();
                Option<String[]> recordKeyFieldsOpt = lazyMetaClient.get().getTableConfig().getRecordKeyFields();
                if (recordKeyFieldsOpt.isEmpty() || recordKeyFieldsOpt.get().length == 0) {
                    // Should not happen since canApply checks for this, include for safety
                    throw new PrestoException(HUDI_BAD_DATA, "Record key fields must be defined to use Record Level Index.");
                }
                List<String> recordKeyFields = Arrays.asList(recordKeyFieldsOpt.get());

                // Only extract the predicates relevant to the record key fields
                TupleDomain<String> filteredDomains = extractPredicatesForColumns(regularColumnPredicates, recordKeyFields);

                // Construct the actual record keys based on the filtered predicates using Hudi's encoding scheme
                List<String> recordKeys = constructRecordKeys(filteredDomains, recordKeyFields);

                if (recordKeys.isEmpty()) {
                    // If key construction fails (e.g., incompatible predicates not caught by canApply, or placeholder issue)
                    log.warn("Took %s ms, but could not construct record keys from predicates. Skipping record index pruning for table %s",
                            timer.endTimer(), schemaTableName);
                    return Optional.empty();
                }
                log.debug(String.format("Constructed %d record keys for index lookup.", recordKeys.size()));

                // Perform index lookup in metadataTable
                // TODO: document here what this map is keyed by
                Map<String, HoodieRecordGlobalLocation> recordIndex = HoodieDataUtils.dedupeAndCollectAsMap(lazyTableMetadata.get().readRecordIndexLocationsWithKeys(HoodieListData.lazy(recordKeys)));
                if (recordIndex.isEmpty()) {
                    log.debug("Record level index lookup took %s ms but returned no locations for the given keys %s for table %s",
                            timer.endTimer(), recordKeys, schemaTableName);
                    // Return all original fileSlices
                    return Optional.empty();
                }

                // Collect fileIds for pruning
                Set<String> relevantFiles = recordIndex.values().stream()
                        .map(HoodieRecordGlobalLocation::getFileId)
                        .collect(Collectors.toSet());
                log.debug("Record level index lookup took %s ms and identified %d relevant file IDs.", timer.endTimer(), relevantFiles.size());

                return Optional.of(relevantFiles);
            });
        }
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
            if (elapsedMs > recordIndexWaitTimeout.toMillis()) {
                // Took too long; skip decision
                return false;
            }

            long remainingMs = Math.max(0, recordIndexWaitTimeout.toMillis() - elapsedMs);
            Optional<Set<String>> relevantFileIds = relevantFileIdsFuture.get(remainingMs, MILLISECONDS);
            return relevantFileIds.map(fileIds -> !fileIds.contains(slice.getFileId())).orElse(false);
        }
        catch (TimeoutException | InterruptedException | ExecutionException e) {
            return false;
        }
    }

    /**
     * Checks if the Record Level Index is available and the query predicates
     * reference all record key fields with compatible (IN/EQUAL) constraints.
     */
    @Override
    public boolean canApply(TupleDomain<String> tupleDomain)
    {
        if (!isIndexSupportAvailable()) {
            log.debug("Record Level Index partition is not enabled in metadata.");
            return false;
        }

        Option<String[]> recordKeyFieldsOpt = lazyMetaClient.get().getTableConfig().getRecordKeyFields();
        if (recordKeyFieldsOpt.isEmpty() || recordKeyFieldsOpt.get().length == 0) {
            log.debug("Record key fields are not defined in table config.");
            return false;
        }
        List<String> recordKeyFields = Arrays.asList(recordKeyFieldsOpt.get());

        // Ensure that predicates reference all record key fields and use IN/EQUAL
        boolean applicable = areAllFieldsReferenced(tupleDomain, recordKeyFields)
                && areDomainsInOrEqualOnly(tupleDomain, recordKeyFields);

        if (!applicable) {
            log.debug("Predicates do not reference all record key fields or use non-compatible (non IN/EQUAL) constraints.");
        }
        else {
            log.debug("Record Level Index is available and applicable based on predicates.");
        }
        return applicable;
    }

    private boolean isIndexSupportAvailable()
    {
        return lazyMetaClient.get().getTableConfig().getMetadataPartitions()
                .contains(HoodieTableMetadataUtil.PARTITION_NAME_RECORD_INDEX);
    }

    /**
     * Extracts predicates from a TupleDomain that match a given set of columns.
     * Preserves all complex predicate properties including multi-value domains,
     * range-based predicates, and nullability.
     *
     * @param tupleDomain The source TupleDomain containing all predicates
     * @param columnFields The set of columns for which to extract predicates
     * @return A new TupleDomain containing only the predicates for the specified columns
     */
    public static TupleDomain<String> extractPredicatesForColumns(TupleDomain<String> tupleDomain, List<String> columnFields)
    {
        if (tupleDomain.isNone()) {
            return TupleDomain.none();
        }

        if (tupleDomain.isAll()) {
            return TupleDomain.all();
        }

        // Extract the domains matching the specified columns
        Map<String, Domain> allDomains = tupleDomain.getDomains().get();
        Map<String, Domain> filteredDomains = allDomains.entrySet().stream().filter(entry -> columnFields.contains(entry.getKey())) // Ensure key is in the column set
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

        // If no domains matched, but we had some columns to extract, return ALL
        if (filteredDomains.isEmpty() && !columnFields.isEmpty() && !allDomains.isEmpty()) {
            return TupleDomain.all();
        }

        return TupleDomain.withColumnDomains(filteredDomains);
    }

    /**
     * Constructs a record key from TupleDomain based on whether it's a complex key or not.
     * <p>
     * Construction of record keys will only be handled for domains generated from EQUALITY or IN predicates.
     * <p>
     * An empty list of record keys will be generated if the following conditions are not met:
     * <ol>
     * <li>recordKeysFields is empty</li>
     * <li>recordKeyDomains isAll</li>
     * <li>For the case of complex key, domains are not applied to all recordKeysFields</li>
     * <li>For the case of complex key, domains are applied to all recordKeyFields, but one of the domain is <b>NOT</b>
     * generated from an equality or IN predicate</li>
     * </ol>
     * <p>
     * Note: This function is O(m^n) where m is the average size of value literals and n is the number of record keys.
     * <p>
     * Optimization 1: If MDT enabled functions allows for streams to be passed in, we can implement an iterator to be more memory efficient.
     * <p>
     * Optimization 2: We should also consider limiting the number of recordKeys generated, if it is estimated to be more than a limit, RLI should just be skipped
     * as it may just be faster to read out all data and filer accordingly.
     *
     * @param recordKeyDomains The filtered TupleDomain containing column handles and values
     * @param recordKeyFields List of column names that represent the record keys
     * @return List of string values representing the record key(s)
     */
    public static List<String> constructRecordKeys(TupleDomain<String> recordKeyDomains, List<String> recordKeyFields)
    {
        // TODO: Move this to TupleDomainUtils
        // If no recordKeys or no recordKeyDomains, return empty list
        if (recordKeyFields == null || recordKeyFields.isEmpty() || recordKeyDomains.isAll()) {
            return Collections.emptyList();
        }

        // All recordKeys must have a domain else, return empty list (applicable to complexKeys)
        // If a one of the recordKey in the set of complexKeys does not have a domain, we are unable to construct
        // a complete complexKey
        if (!recordKeyDomains.getDomains().get().keySet().containsAll(recordKeyFields)) {
            return Collections.emptyList();
        }

        // Extract the domain mappings from the tuple domain
        Map<String, Domain> domains = recordKeyDomains.getDomains().get();

        // Case 1: Not a complex key (single record key)
        if (recordKeyFields.size() == 1) {
            String recordKey = recordKeyFields.get(0);

            // Extract value for this key
            Domain domain = domains.get(recordKey);
            return extractStringValues(domain);
        }
        // Case 2: Complex/Composite key (multiple record keys)
        else {
            // Create a queue to manage the Cartesian product generation
            Queue<String> results = new LinkedList<>();

            // For each key in the complex key
            for (String recordKeyField : recordKeyFields) {
                // Extract value for this key
                Domain domain = domains.get(recordKeyField);
                List<String> values = extractStringValues(domain);
                // First iteration: initialize the queue
                if (results.isEmpty()) {
                    values.forEach(v -> results.offer(recordKeyField + DEFAULT_COLUMN_VALUE_SEPARATOR + v));
                }
                else {
                    int size = results.size();
                    for (int j = 0; j < size; j++) {
                        String currentEntry = results.poll();

                        // Generate new combinations by appending keyParts to existing keyParts
                        for (String v : values) {
                            String newKeyPart = recordKeyField + DEFAULT_COLUMN_VALUE_SEPARATOR + v;
                            String newEntry = currentEntry + DEFAULT_RECORD_KEY_PARTS_SEPARATOR + newKeyPart;
                            results.offer(newEntry);
                        }
                    }
                }
            }
            return results.stream().toList();
        }
    }

    /**
     * Extract string values from a domain, handle EQUAL and IN domains only.
     * Note: Actual implementation depends on your Domain class structure.
     */
    private static List<String> extractStringValues(Domain domain)
    {
        List<String> values = new ArrayList<>();

        if (domain.isSingleValue()) {
            // Handle EQUAL condition (single value domain)
            Object value = domain.getSingleValue();
            values.add(convertToString(value));
        }
        else if (isDiscreteSet(domain.getValues())) {
            // Handle IN condition (set of discrete values)
            for (Object value : getDiscreteSet(domain.getValues())) {
                values.add(convertToString(value));
            }
        }
        return values;
    }

    private static String convertToString(Object value)
    {
        if (value instanceof Slice) {
            return ((Slice) value).toStringUtf8();
        }
        else {
            return value.toString();
        }
    }
}