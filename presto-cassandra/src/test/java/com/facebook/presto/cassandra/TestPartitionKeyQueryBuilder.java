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
package com.facebook.presto.cassandra;

import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.testng.annotations.Test;

import java.util.List;

import static io.airlift.slice.Slices.utf8Slice;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

/**
 * Unit tests for the query-builder-based partition-key statement construction in
 * {@link NativeCassandraSession} (review item L6). These replaced hand-written CQL string
 * concatenation, so we assert both the generated CQL shape and the bound values (still converted
 * from Presto native values via {@link CassandraType#getJavaValue}).
 */
public class TestPartitionKeyQueryBuilder
{
    private static final String CONNECTOR_ID = "test";
    private static final CassandraTableHandle TABLE = new CassandraTableHandle(CONNECTOR_ID, "ks", "tbl");

    private static CassandraColumnHandle partitionColumn(String name, int ordinal, CassandraType type)
    {
        return new CassandraColumnHandle(CONNECTOR_ID, name, ordinal, type, null, true, false, false, false);
    }

    @Test
    public void testInQuerySingleColumn()
    {
        List<CassandraColumnHandle> keys = ImmutableList.of(partitionColumn("id", 0, CassandraType.INT));
        // Presto native INT values are Long; getJavaValue converts them to Integer.
        List<java.util.Set<Object>> prefixes = ImmutableList.of(ImmutableSet.of(1L));

        SimpleStatement statement = NativeCassandraSession.buildPartitionKeyInQuery(TABLE, keys, prefixes);

        String query = statement.getQuery();
        assertTrue(query.toUpperCase(java.util.Locale.ROOT).startsWith("SELECT DISTINCT"), query);
        assertTrue(query.contains("WHERE"), query);
        assertTrue(query.contains("IN ?"), query);
        // One marker bound to the per-column list of converted values.
        assertEquals(statement.getPositionalValues(), ImmutableList.of(ImmutableList.of(1)));
    }

    @Test
    public void testInQueryCompositeKey()
    {
        List<CassandraColumnHandle> keys = ImmutableList.of(
                partitionColumn("name", 0, CassandraType.TEXT),
                partitionColumn("id", 1, CassandraType.INT));
        List<java.util.Set<Object>> prefixes = ImmutableList.of(
                ImmutableSet.of(utf8Slice("a")),
                ImmutableSet.of(7L));

        SimpleStatement statement = NativeCassandraSession.buildPartitionKeyInQuery(TABLE, keys, prefixes);

        String query = statement.getQuery();
        assertTrue(query.contains("WHERE"), query);
        assertTrue(query.contains("AND"), query);
        // One "IN ?" per partition-key column.
        assertEquals(countOccurrences(query, "IN ?"), 2, query);
        assertEquals(statement.getPositionalValues(),
                ImmutableList.of(ImmutableList.of("a"), ImmutableList.of(7)));
    }

    @Test
    public void testEqualityQueryCompositeKey()
    {
        List<CassandraColumnHandle> keys = ImmutableList.of(
                partitionColumn("name", 0, CassandraType.TEXT),
                partitionColumn("id", 1, CassandraType.INT));
        List<Object> combination = ImmutableList.of(utf8Slice("a"), 7L);

        SimpleStatement statement = NativeCassandraSession.buildPartitionKeyEqualityQuery(TABLE, keys, combination);

        String query = statement.getQuery();
        assertTrue(query.contains("WHERE"), query);
        assertTrue(query.contains("AND"), query);
        // One equality marker per partition-key column (the builder renders these without spaces, e.g. "name=?").
        assertEquals(countOccurrences(query, "=?"), 2, query);
        // Single value per marker (not wrapped in a list, unlike the IN form).
        assertEquals(statement.getPositionalValues(), ImmutableList.of("a", 7));
    }

    private static int countOccurrences(String haystack, String needle)
    {
        int count = 0;
        int index = 0;
        while ((index = haystack.indexOf(needle, index)) >= 0) {
            count++;
            index += needle.length();
        }
        return count;
    }
}
