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
import com.google.common.collect.ImmutableList;

import javax.inject.Inject;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static com.facebook.presto.plugin.clp.ClpSplit.SplitType.ARCHIVE;
import static java.lang.String.format;
import static java.util.Comparator.comparingLong;

public class ClpMySqlSplitProvider
        implements ClpSplitProvider
{
    // Column names
    public static final String ARCHIVES_TABLE_COLUMN_ID = "id";
    public static final String ARCHIVES_TABLE_NUM_MESSAGES = "num_messages";

    // Table suffixes
    public static final String ARCHIVES_TABLE_SUFFIX = "_archives";

    // SQL templates
    private static final String SQL_SELECT_ARCHIVES_TEMPLATE = format("SELECT * FROM `%%s%%s%s` WHERE 1 = 1", ARCHIVES_TABLE_SUFFIX);

    private static final Logger log = Logger.get(ClpMySqlSplitProvider.class);

    private final ClpConfig config;

    @Inject
    public ClpMySqlSplitProvider(ClpConfig config)
    {
        try {
            Class.forName("com.mysql.cj.jdbc.Driver");
        }
        catch (ClassNotFoundException e) {
            log.error(e, "Failed to load MySQL JDBC driver");
            throw new RuntimeException("MySQL JDBC driver not found", e);
        }
        this.config = config;
    }

    @Override
    public List<ClpSplit> listSplits(ClpTableLayoutHandle clpTableLayoutHandle)
    {
        ImmutableList.Builder<ClpSplit> splits = new ImmutableList.Builder<>();
        ClpTableHandle clpTableHandle = clpTableLayoutHandle.getTable();
        Optional<ClpTopNSpec> topNSpecOptional = clpTableLayoutHandle.getTopN();
        String tablePath = clpTableHandle.getTablePath();
        String tableName = clpTableHandle.getSchemaTableName().getTableName();
        String archivePathQuery = format(SQL_SELECT_ARCHIVES_TEMPLATE, config.getMetadataTablePrefix(), tableName);

        if (clpTableLayoutHandle.getMetadataSql().isPresent()) {
            String metadataFilterQuery = clpTableLayoutHandle.getMetadataSql().get();
            archivePathQuery += " AND (" + metadataFilterQuery + ")";
        }

        if (topNSpecOptional.isPresent()) {
            ClpTopNSpec topNSpec = topNSpecOptional.get();
            // Only handles one range metadata column for now
            ClpTopNSpec.Ordering ordering = topNSpec.getOrderings().get(0);
            String col = ordering.getColumns().get(ordering.getColumns().size() - 1);
            String dir = (ordering.getOrder() == ClpTopNSpec.Order.ASC) ? "ASC" : "DESC";
            archivePathQuery += " ORDER BY " + "`" + col + "` " + dir;

            List<ArchiveMeta> archiveMetaList = fetchArchiveMeta(archivePathQuery, ordering);
            List<ArchiveMeta> selected = selectTopNArchives(archiveMetaList, topNSpec.getLimit(), ordering.getOrder());

            for (ArchiveMeta a : selected) {
                splits.add(new ClpSplit(tablePath + "/" + a.id, ARCHIVE, clpTableLayoutHandle.getKqlQuery()));
            }
            ImmutableList<ClpSplit> result = splits.build();
            log.debug("Number of splits: %s", result.size());
            return result;
        }
        log.debug("Query for archive: %s", archivePathQuery);

        try (Connection connection = getConnection()) {
            // Fetch archive IDs and create splits
            try (PreparedStatement statement = connection.prepareStatement(archivePathQuery); ResultSet resultSet = statement.executeQuery()) {
                while (resultSet.next()) {
                    final String archiveId = resultSet.getString(ARCHIVES_TABLE_COLUMN_ID);
                    final String archivePath = tablePath + "/" + archiveId;
                    splits.add(new ClpSplit(archivePath, ARCHIVE, clpTableLayoutHandle.getKqlQuery()));
                }
            }
        }
        catch (SQLException e) {
            log.warn("Database error while processing splits for %s: %s", tableName, e);
        }

        ImmutableList<ClpSplit> filteredSplits = splits.build();
        log.debug("Number of splits: %s", filteredSplits.size());
        return filteredSplits;
    }

    private Connection getConnection()
            throws SQLException
    {
        Connection connection = DriverManager.getConnection(config.getMetadataDbUrl(), config.getMetadataDbUser(), config.getMetadataDbPassword());
        String dbName = config.getMetadataDbName();
        if (dbName != null && !dbName.isEmpty()) {
            connection.createStatement().execute(format("USE `%s`", dbName));
        }
        return connection;
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
        List<ArchiveMeta> list = new ArrayList<>();
        try (Connection connection = getConnection();
                PreparedStatement stmt = connection.prepareStatement(query);
                ResultSet rs = stmt.executeQuery()) {
            while (rs.next()) {
                list.add(new ArchiveMeta(
                        rs.getString(ARCHIVES_TABLE_COLUMN_ID),
                        rs.getLong(ordering.getColumns().get(0)),
                        rs.getLong(ordering.getColumns().get(1)),
                        rs.getLong(ARCHIVES_TABLE_NUM_MESSAGES)));
            }
        }
        catch (SQLException e) {
            log.warn("Database error while fetching archive metadata: %s", e);
        }
        return list;
    }

    /**
     * Selects the set of archives that must be scanned to guarantee the top-N results by timestamp
     * (ASC or DESC), given only archive ranges and message counts.
     * <ul>
     *   <li>Merges overlapping archives into groups (union of time ranges).</li>
     *   <li>For DESC: always include the newest group, then add older ones until their total
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

        // 1) Merge overlaps into groups
        List<ArchiveGroup> groups = toArchiveGroups(archives);

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
     * @return merged groups
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
                // extend current group
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
     * Represents metadata of an archive, including its ID, timestamp bounds, and message count.
     */
    private static class ArchiveMeta
    {
        final String id;
        final long lowerBound;
        final long upperBound;
        final long messageCount;

        ArchiveMeta(String id, long lowerBound, long upperBound, long messageCount)
        {
            this.id = id;
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
