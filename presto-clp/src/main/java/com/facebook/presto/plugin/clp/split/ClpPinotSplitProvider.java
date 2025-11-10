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
package com.facebook.presto.plugin.clp.split;

import com.facebook.airlift.log.Logger;
import com.facebook.presto.plugin.clp.ClpConfig;
import com.facebook.presto.plugin.clp.ClpSplit;
import com.facebook.presto.plugin.clp.ClpTableHandle;
import com.facebook.presto.plugin.clp.ClpTableLayoutHandle;
import com.facebook.presto.plugin.clp.optimization.ClpTopNSpec;
import com.facebook.presto.spi.SchemaTableName;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;

import javax.inject.Inject;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;

import static com.facebook.presto.plugin.clp.ClpSplit.SplitType;
import static com.facebook.presto.plugin.clp.ClpSplit.SplitType.ARCHIVE;
import static com.facebook.presto.plugin.clp.ClpSplit.SplitType.IR;
import static java.lang.String.format;
import static java.util.Comparator.comparingLong;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.SECONDS;

public class ClpPinotSplitProvider
        implements ClpSplitProvider
{
    private static final Logger log = Logger.get(ClpPinotSplitProvider.class);
    private static final String SQL_SELECT_SPLITS_TEMPLATE = "SELECT tpath FROM %s WHERE 1 = 1 AND (%s) LIMIT 999999";
    private static final String SQL_SELECT_SPLIT_META_TEMPLATE = "SELECT tpath, creationtime, lastmodifiedtime, num_messages FROM %s WHERE 1 = 1 AND (%s) ORDER BY %s %s LIMIT 999999";
    private final ClpConfig config;
    private final URL pinotSqlQueryEndpointUrl;

    @Inject
    public ClpPinotSplitProvider(ClpConfig config)
    {
        this.config = requireNonNull(config, "config is null");
        try {
            this.pinotSqlQueryEndpointUrl = buildPinotSqlQueryEndpointUrl(config);
        }
        catch (MalformedURLException e) {
            throw new IllegalArgumentException(
                    format("Failed to build Pinot sql query endpoint URL using the provided database url: %s", config.getMetadataDbUrl()), e);
        }
    }

    @Override
    public List<ClpSplit> listSplits(ClpTableLayoutHandle clpTableLayoutHandle)
    {
        ClpTableHandle clpTableHandle = clpTableLayoutHandle.getTable();
        Optional<ClpTopNSpec> topNSpecOptional = clpTableLayoutHandle.getTopN();
        String tableName = inferMetadataTableName(clpTableHandle);
        try {
            ImmutableList.Builder<ClpSplit> splits = new ImmutableList.Builder<>();
            if (topNSpecOptional.isPresent()) {
                ClpTopNSpec topNSpec = topNSpecOptional.get();
                // Only handles one range metadata column for now (first ordering)
                ClpTopNSpec.Ordering ordering = topNSpec.getOrderings().get(0);
                // Get the last column in the ordering (the primary sort column for nested fields)
                String col = ordering.getColumns().get(ordering.getColumns().size() - 1);
                String dir = (ordering.getOrder() == ClpTopNSpec.Order.ASC) ? "ASC" : "DESC";
                String splitMetaQuery = buildSplitMetadataQuery(tableName, clpTableLayoutHandle.getMetadataSql().orElse("1 = 1"), col, dir);
                List<ArchiveMeta> archiveMetaList = fetchArchiveMeta(splitMetaQuery, ordering);
                List<ArchiveMeta> selected = selectTopNArchives(archiveMetaList, topNSpec.getLimit(), ordering.getOrder());

                for (ArchiveMeta a : selected) {
                    String splitPath = a.id;
                    splits.add(new ClpSplit(splitPath, determineSplitType(splitPath), clpTableLayoutHandle.getKqlQuery()));
                }

                List<ClpSplit> filteredSplits = splits.build();
                log.debug("Number of topN filtered splits: %s", filteredSplits.size());
                return filteredSplits;
            }

            String splitQuery = buildSplitSelectionQuery(tableName, clpTableLayoutHandle.getMetadataSql().orElse("1 = 1"));
            List<JsonNode> splitRows = getQueryResult(pinotSqlQueryEndpointUrl, splitQuery);
            for (JsonNode row : splitRows) {
                String splitPath = row.elements().next().asText();
                splits.add(new ClpSplit(splitPath, determineSplitType(splitPath), clpTableLayoutHandle.getKqlQuery()));
            }

            List<ClpSplit> filteredSplits = splits.build();
            log.debug("Number of filtered splits: %s", filteredSplits.size());
            return filteredSplits;
        }
        catch (Exception e) {
            log.error(e, "Failed to list splits for table %s", tableName);
            throw new RuntimeException(format("Failed to list splits for table %s: %s", tableName, e.getMessage()), e);
        }
    }

    /**
     * Infers the Pinot metadata table name from the CLP table handle.
     * <p>
     * In the current Pinot metadata, tables across different schemas share the same metadata table.
     * The metadata table name corresponds directly to the logical table name,
     * regardless of which schema is being queried. This allows multiple schemas
     * to have different views or access patterns on the same underlying data.
     * </p>
     * <p>
     * For example:
     * <ul>
     *   <li>Schema: "default", Table: "logs" → Pinot metadata table: "logs"</li>
     *   <li>Schema: "production", Table: "logs" → Pinot metadata table: "logs" (same table)</li>
     *   <li>Schema: "staging", Table: "events" → Pinot metadata table: "events"</li>
     * </ul>
     * </p>
     *
     * @param tableHandle the CLP table handle containing schema and table information
     * @return the Pinot metadata table name (just the table name without schema prefix)
     * @throws NullPointerException if tableHandle is null
     */
    protected String inferMetadataTableName(ClpTableHandle tableHandle)
    {
        requireNonNull(tableHandle, "tableHandle is null");
        SchemaTableName schemaTableName = tableHandle.getSchemaTableName();

        // In Pinot, the metadata table name is just the table name
        // Multiple schemas can reference the same underlying metadata table
        return schemaTableName.getTableName();
    }

    /**
     * Constructs the Pinot SQL query endpoint URL from configuration.
     * Can be overridden by subclasses to customize URL construction.
     *
     * @param config the CLP configuration
     * @return the Pinot SQL query endpoint URL
     * @throws MalformedURLException if the constructed URL is invalid
     */
    protected URL buildPinotSqlQueryEndpointUrl(ClpConfig config) throws MalformedURLException
    {
        return new URL(config.getMetadataDbUrl() + "/query/sql");
    }

    /**
     * Fetches archive metadata from the database.
     *
     * @param query    SQL query string that selects the archives
     * @param ordering The top-N ordering specifying which columns contain lowerBound/upperBound
     * @return List of ArchiveMeta objects representing archive metadata
     */
    private List<ArchiveMeta> fetchArchiveMeta(String query, ClpTopNSpec.Ordering ordering)
    {
        ImmutableList.Builder<ArchiveMeta> archiveMetas = new ImmutableList.Builder<>();
        List<JsonNode> rows = getQueryResult(pinotSqlQueryEndpointUrl, query);
        for (JsonNode row : rows) {
            archiveMetas.add(new ArchiveMeta(
                    row.get(0).asText(),
                    row.get(1).asLong(),
                    row.get(2).asLong(),
                    row.get(3).asLong()));
        }
        return archiveMetas.build();
    }

    /**
     * Selects the set of archives that must be scanned to guarantee the top-N results by timestamp
     * (ASC or DESC), given only archive ranges and message counts.
     * <ul>
     *   <li>Merges overlapping archives into components (union of time ranges).</li>
     *   <li>For DESC: always include the newest component, then add older ones until their total
     *      message counts cover the limit.</li>
     *   <li>For ASC: symmetric — start from the oldest, then add newer ones.</li>
     * </ul>

     * @param archives list of archives with [lowerBound, upperBound, messageCount]
     * @param limit number of messages requested
     * @param order ASC (earliest first) or DESC (latest first)
     * @return archives that must be scanned
     */
    private static List<ArchiveMeta> selectTopNArchives(List<ArchiveMeta> archives, long limit, ClpTopNSpec.Order order)
    {
        if (archives == null || archives.isEmpty() || limit <= 0) {
            return ImmutableList.of();
        }
        requireNonNull(order, "order is null");

        // 1) Merge overlaps into groups
        List<ArchiveGroup> groups = toArchiveGroups(archives);

        if (groups.isEmpty()) {
            return ImmutableList.of();
        }

        // 2) Pick minimal set of groups per order, then return all member archives
        List<ArchiveMeta> selected = new ArrayList<>();
        if (order == ClpTopNSpec.Order.DESC) {
            // newest group index
            int k = groups.size() - 1;

            // must include newest group
            selected.addAll(groups.get(k).members);

            // assume worst case: newest contributes 0 after filter; cover limit from older groups
            long coveredByOlder = 0;
            for (int i = k - 1; i >= 0 && coveredByOlder < limit; --i) {
                selected.addAll(groups.get(i).members);
                coveredByOlder += groups.get(i).count;
            }
        }
        else {
            // oldest group index
            int k = 0;

            // must include oldest group
            selected.addAll(groups.get(k).members);

            // assume worst case: oldest contributes 0; cover limit from newer groups
            long coveredByNewer = 0;
            for (int i = k + 1; i < groups.size() && coveredByNewer < limit; ++i) {
                selected.addAll(groups.get(i).members);
                coveredByNewer += groups.get(i).count;
            }
        }

        return selected;
    }

    /**
     * Groups overlapping archives into non-overlapping archive groups.
     *
     * @param archives archives sorted by lowerBound
     * @return merged components
     */
    private static List<ArchiveGroup> toArchiveGroups(List<ArchiveMeta> archives)
    {
        List<ArchiveMeta> sorted = new ArrayList<>(archives);
        sorted.sort(comparingLong((ArchiveMeta a) -> a.lowerBound)
                .thenComparingLong(a -> a.upperBound));

        List<ArchiveGroup> groups = new ArrayList<>();
        ArchiveGroup cur = null;

        for (ArchiveMeta a : sorted) {
            if (cur == null) {
                cur = startArchiveGroup(a);
            }
            else if (overlaps(cur, a)) {
                // extend current component
                cur.end = Math.max(cur.end, a.upperBound);
                cur.count += a.messageCount;
                cur.members.add(a);
            }
            else {
                // finalize current, start a new one
                groups.add(cur);
                cur = startArchiveGroup(a);
            }
        }
        if (cur != null) {
            groups.add(cur);
        }
        return groups;
    }

    private static ArchiveGroup startArchiveGroup(ArchiveMeta a)
    {
        ArchiveGroup group = new ArchiveGroup();
        group.begin = a.lowerBound;
        group.end = a.upperBound;
        group.count = a.messageCount;
        group.members.add(a);
        return group;
    }

    private static boolean overlaps(ArchiveGroup cur, ArchiveMeta a)
    {
        return a.lowerBound <= cur.end && a.upperBound >= cur.begin;
    }

    /**
     * Determines the split type based on file path extension.
     *
     * @param splitPath the file path
     * @return IR for .clp.zst files, ARCHIVE otherwise
     */
    private static SplitType determineSplitType(String splitPath)
    {
        return splitPath.endsWith(".clp.zst") ? IR : ARCHIVE;
    }

    /**
     * Factory method for building split selection SQL queries.
     * Exposed for testing purposes.
     *
     * @param tableName the Pinot table name
     * @param filterSql the filter SQL expression
     * @return the complete SQL query for selecting splits
     */
    @VisibleForTesting
    protected String buildSplitSelectionQuery(String tableName, String filterSql)
    {
        return format(SQL_SELECT_SPLITS_TEMPLATE, tableName, filterSql);
    }

    /**
     * Factory method for building split metadata SQL queries.
     * Exposed for testing purposes.
     *
     * @param tableName the Pinot table name
     * @param filterSql the filter SQL expression
     * @param orderByColumn the column to order by
     * @param orderDirection the order direction (ASC or DESC)
     * @return the complete SQL query for selecting split metadata
     */
    @VisibleForTesting
    protected String buildSplitMetadataQuery(String tableName, String filterSql, String orderByColumn, String orderDirection)
    {
        return format(SQL_SELECT_SPLIT_META_TEMPLATE, tableName, filterSql, orderByColumn, orderDirection);
    }

    private static List<JsonNode> getQueryResult(URL url, String sql)
    {
        try {
            HttpURLConnection conn = (HttpURLConnection) url.openConnection();
            conn.setRequestMethod("POST");
            conn.setRequestProperty("Content-Type", "application/json");
            conn.setRequestProperty("Accept", "application/json");
            conn.setDoOutput(true);
            conn.setConnectTimeout((int) SECONDS.toMillis(5));
            conn.setReadTimeout((int) SECONDS.toMillis(30));

            log.info("Executing Pinot query: %s", sql);
            ObjectMapper mapper = new ObjectMapper();
            String body = format("{\"sql\": %s }", mapper.writeValueAsString(sql));
            try (OutputStream os = conn.getOutputStream()) {
                os.write(body.getBytes(StandardCharsets.UTF_8));
            }

            int code = conn.getResponseCode();
            InputStream is = (code >= 200 && code < 300) ? conn.getInputStream() : conn.getErrorStream();
            if (is == null) {
                throw new IOException("Pinot HTTP " + code + " with empty body");
            }

            JsonNode root;
            try (InputStream in = is) {
                root = mapper.readTree(in);
            }
            JsonNode resultTable = root.get("resultTable");
            if (resultTable == null) {
                throw new IllegalStateException("Pinot query response missing 'resultTable' field");
            }
            JsonNode rows = resultTable.get("rows");
            if (rows == null) {
                throw new IllegalStateException("Pinot query response missing 'rows' field in resultTable");
            }
            ImmutableList.Builder<JsonNode> resultBuilder = ImmutableList.builder();
            for (Iterator<JsonNode> it = rows.elements(); it.hasNext(); ) {
                JsonNode row = it.next();
                resultBuilder.add(row);
            }
            List<JsonNode> results = resultBuilder.build();
            log.debug("Number of results: %s", results.size());
            return results;
        }
        catch (IOException e) {
            log.error(e, "IO error executing Pinot query: %s", sql);
            return Collections.emptyList();
        }
        catch (Exception e) {
            log.error(e, "Unexpected error executing Pinot query: %s", sql);
            return Collections.emptyList();
        }
    }

    /**
     * Represents metadata of an archive, including its ID, timestamp bounds, and message count.
     */
    private static final class ArchiveMeta
    {
        private final String id;
        private final long lowerBound;
        private final long upperBound;
        private final long messageCount;

        ArchiveMeta(String id, long lowerBound, long upperBound, long messageCount)
        {
            this.id = requireNonNull(id, "id is null");
            if (lowerBound > upperBound) {
                throw new IllegalArgumentException(
                        format("Invalid archive bounds: lowerBound (%d) > upperBound (%d)", lowerBound, upperBound));
            }
            if (messageCount < 0) {
                throw new IllegalArgumentException(
                        format("Invalid message count: %d (must be >= 0)", messageCount));
            }
            this.lowerBound = lowerBound;
            this.upperBound = upperBound;
            this.messageCount = messageCount;
        }
    }

    /**
     * Represents a group of overlapping archives treated as one logical unit.
     */
    private static final class ArchiveGroup
    {
        long begin;
        long end;
        long count;
        final List<ArchiveMeta> members = new ArrayList<>();
    }
}
