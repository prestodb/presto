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
package com.facebook.presto.plugin.clp;

import com.facebook.presto.plugin.clp.split.ClpSplitProvider;
import com.facebook.presto.spi.SchemaTableName;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.facebook.presto.plugin.clp.ClpMetadata.DEFAULT_SCHEMA_NAME;
import static com.facebook.presto.plugin.clp.ClpMetadataDbSetUp.ARCHIVES_STORAGE_DIRECTORY_BASE;
import static com.facebook.presto.plugin.clp.ClpMetadataDbSetUp.ArchivesTableRow;
import static com.facebook.presto.plugin.clp.ClpMetadataDbSetUp.DbHandle;
import static com.facebook.presto.plugin.clp.ClpMetadataDbSetUp.getDbHandle;
import static com.facebook.presto.plugin.clp.ClpMetadataDbSetUp.setupSplit;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static org.testng.Assert.assertEquals;

@Test(singleThreaded = true)
public class TestClpSplit
{
    private DbHandle dbHandle;
    private ClpSplitProvider clpSplitProvider;
    private Map<String, List<ArchivesTableRow>> tableSplits;

    @BeforeMethod
    public void setUp()
    {
        dbHandle = getDbHandle("split_testdb");
        tableSplits = new HashMap<>();

        int numKeys = 3;
        int numValuesPerKey = 10;

        for (int i = 0; i < numKeys; i++) {
            String key = "test_" + i;
            List<ArchivesTableRow> values = new ArrayList<>();

            for (int j = 0; j < numValuesPerKey; j++) {
                // We generate synthetic begin_timestamp and end_timestamp values for each split
                // by offsetting two base timestamps (1700000000000L and 1705000000000L) with a
                // fixed increment per split (10^10 * j).
                values.add(new ArchivesTableRow(
                        "id_" + j,
                        1700000000000L + 10000000000L * j,
                        1705000000000L + 10000000000L * j));
            }

            tableSplits.put(key, values);
        }
        clpSplitProvider = setupSplit(dbHandle, tableSplits);
    }

    @AfterMethod
    public void tearDown()
    {
        ClpMetadataDbSetUp.tearDown(dbHandle);
    }

    @Test
    public void testListSplits()
    {
        for (Map.Entry<String, List<ArchivesTableRow>> entry : tableSplits.entrySet()) {
            // Without metadata filters
            compareListSplitsResult(entry, Optional.empty(), ImmutableList.of(0, 1, 2, 3, 4, 5, 6, 7, 8, 9));

            // query_begin_ts < archives_min_ts && query_end_ts > archives_max_ts
            compareListSplitsResult(entry, Optional.of("(end_timestamp > 1699999999999 AND begin_timestamp < 1795000000001)"), ImmutableList.of(0, 1, 2, 3, 4, 5, 6, 7, 8, 9));

            // query_begin_ts < archives_min_ts && query_end_ts > archives_min_ts && query_end_ts < archives_max_ts
            compareListSplitsResult(entry, Optional.of("(end_timestamp > 1699999999999 AND begin_timestamp < 1744999999999)"), ImmutableList.of(0, 1, 2, 3, 4));

            // query_end_ts < archives_min_ts
            compareListSplitsResult(entry, Optional.of("(begin_timestamp < 1699999999999)"), ImmutableList.of());

            // query_begin_ts > archives_min_ts && query_begin_ts < archives_max_ts && query_end_ts > archives_max_ts
            compareListSplitsResult(entry, Optional.of("(end_timestamp > 1745000000001 AND begin_timestamp < 1795000000001)"), ImmutableList.of(5, 6, 7, 8, 9));

            // query_begin_ts > archives_min_ts && query_begin_ts < archives_max_ts && query_end_ts < archives_max_ts
            compareListSplitsResult(entry, Optional.of("(end_timestamp > 1745000000001 AND begin_timestamp <= 1770000000000)"), ImmutableList.of(5, 6, 7));

            // query_begin_ts > archives_max_ts
            compareListSplitsResult(entry, Optional.of("(end_timestamp > 1795000000001)"), ImmutableList.of());

            // query_begin_ts = archive_min_ts
            compareListSplitsResult(entry, Optional.of("(end_timestamp >= 1700000000000 AND begin_timestamp <= 1700000000000)"), ImmutableList.of(0));

            // query_begin_ts = archive_max_ts
            compareListSplitsResult(entry, Optional.of("(end_timestamp >= 1795000000000 AND begin_timestamp <= 1795000000000)"), ImmutableList.of(9));

            // query_ts = x && x > archive_min_ts && x < archive_max_ts (non-exist)
            compareListSplitsResult(entry, Optional.of("(end_timestamp >= 1715000000001 AND begin_timestamp <= 1715000000001)"), ImmutableList.of());

            // query_ts = x && x > archive_min_ts && x < archive_max_ts
            compareListSplitsResult(entry, Optional.of("(end_timestamp >= 1715000000000 AND begin_timestamp <= 1715000000000)"), ImmutableList.of(1));
        }
    }

    private void compareListSplitsResult(
            Map.Entry<String, List<ArchivesTableRow>> entry,
            Optional<String> metadataSql,
            List<Integer> expectedSplitIds)
    {
        String tableName = entry.getKey();
        String tablePath = ARCHIVES_STORAGE_DIRECTORY_BASE + tableName;
        ClpTableLayoutHandle layoutHandle = new ClpTableLayoutHandle(
                new ClpTableHandle(new SchemaTableName(DEFAULT_SCHEMA_NAME, tableName), tablePath),
                Optional.empty(),
                metadataSql);
        List<ArchivesTableRow> expectedSplits = expectedSplitIds.stream()
                .map(expectedSplitId -> entry.getValue().get(expectedSplitId))
                .collect(toImmutableList());
        List<ClpSplit> actualSplits = clpSplitProvider.listSplits(layoutHandle);
        assertEquals(actualSplits.size(), expectedSplits.size());

        ImmutableSet<String> actualSplitPaths = actualSplits.stream()
                .map(ClpSplit::getPath)
                .collect(toImmutableSet());

        ImmutableSet<String> expectedSplitPaths = expectedSplits.stream()
                .map(split -> tablePath + "/" + split.getId())
                .collect(toImmutableSet());

        assertEquals(actualSplitPaths, expectedSplitPaths);
    }
}
