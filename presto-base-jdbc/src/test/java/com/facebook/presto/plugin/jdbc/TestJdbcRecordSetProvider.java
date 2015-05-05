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
package com.facebook.presto.plugin.jdbc;

import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorPartitionResult;
import com.facebook.presto.spi.ConnectorSplitSource;
import com.facebook.presto.spi.Domain;
import com.facebook.presto.spi.Range;
import com.facebook.presto.spi.RecordCursor;
import com.facebook.presto.spi.RecordSet;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.SortedRangeSet;
import com.facebook.presto.spi.TupleDomain;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static com.google.common.collect.Iterables.getOnlyElement;
import static io.airlift.concurrent.MoreFutures.getFutureValue;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

@Test
public class TestJdbcRecordSetProvider
{
    private TestingDatabase database;
    private JdbcClient jdbcClient;
    private JdbcSplit split;

    private JdbcTableHandle table;
    private JdbcColumnHandle textColumn;
    private JdbcColumnHandle valueColumn;

    @BeforeClass
    public void setUp()
            throws Exception
    {
        database = new TestingDatabase();
        jdbcClient = database.getJdbcClient();
        split = database.getSplit("example", "numbers");

        table = jdbcClient.getTableHandle(new SchemaTableName("example", "numbers"));

        Map<String, JdbcColumnHandle> columns = database.getColumnHandles("example", "numbers");
        textColumn = columns.get("text");
        valueColumn = columns.get("value");
    }

    @AfterClass
    public void tearDown()
            throws Exception
    {
        database.close();
    }

    @Test
    public void testGetRecordSet()
            throws Exception
    {
        JdbcRecordSetProvider recordSetProvider = new JdbcRecordSetProvider(jdbcClient);
        RecordSet recordSet = recordSetProvider.getRecordSet(split, ImmutableList.of(textColumn, valueColumn));
        assertNotNull(recordSet, "recordSet is null");

        RecordCursor cursor = recordSet.cursor();
        assertNotNull(cursor, "cursor is null");

        Map<String, Long> data = new LinkedHashMap<>();
        while (cursor.advanceNextPosition()) {
            data.put(cursor.getSlice(0).toStringUtf8(), cursor.getLong(1));
        }
        assertEquals(data, ImmutableMap.<String, Long>builder()
                .put("one", 1L)
                .put("two", 2L)
                .put("three", 3L)
                .put("ten", 10L)
                .put("eleven", 11L)
                .put("twelve", 12L)
                .build());
    }

    @Test
    public void testTupleDomain()
            throws Exception
    {
        // single value
        getCursor(table, ImmutableList.of(textColumn, valueColumn), TupleDomain.withColumnDomains(
                ImmutableMap.<ColumnHandle, Domain>of(textColumn, Domain.singleValue("foo"))
        ));

        // multiple values (string)
        getCursor(table, ImmutableList.of(textColumn, valueColumn), TupleDomain.withColumnDomains(
                ImmutableMap.<ColumnHandle, Domain>of(textColumn, Domain.union(ImmutableList.of(Domain.singleValue("foo"), Domain.singleValue("bar"))))
        ));

        // inequality (string)
        getCursor(table, ImmutableList.of(textColumn, valueColumn), TupleDomain.withColumnDomains(
                ImmutableMap.<ColumnHandle, Domain>of(textColumn, Domain.create(SortedRangeSet.of(Range.greaterThan("foo")), false))
        ));

        getCursor(table, ImmutableList.of(textColumn, valueColumn), TupleDomain.withColumnDomains(
                ImmutableMap.<ColumnHandle, Domain>of(textColumn, Domain.create(SortedRangeSet.of(Range.greaterThan("foo")), false))
        ));

        getCursor(table, ImmutableList.of(textColumn, valueColumn), TupleDomain.withColumnDomains(
                ImmutableMap.<ColumnHandle, Domain>of(textColumn, Domain.create(SortedRangeSet.of(Range.lessThanOrEqual("foo")), false))
        ));

        getCursor(table, ImmutableList.of(textColumn, valueColumn), TupleDomain.withColumnDomains(
                ImmutableMap.<ColumnHandle, Domain>of(textColumn, Domain.create(SortedRangeSet.of(Range.lessThan("foo")), false))
        ));

        // is null
        getCursor(table, ImmutableList.of(textColumn, valueColumn), TupleDomain.withColumnDomains(
                ImmutableMap.<ColumnHandle, Domain>of(textColumn, Domain.onlyNull(String.class))
        ));

        // not null
        getCursor(table, ImmutableList.of(textColumn, valueColumn), TupleDomain.withColumnDomains(
                ImmutableMap.<ColumnHandle, Domain>of(textColumn, Domain.notNull(String.class))
        ));

        // specific value or null
        getCursor(table, ImmutableList.of(textColumn, valueColumn), TupleDomain.withColumnDomains(
                ImmutableMap.<ColumnHandle, Domain>of(textColumn, Domain.union(ImmutableList.of(Domain.singleValue("foo"), Domain.onlyNull(String.class))))
        ));

        getCursor(table, ImmutableList.of(textColumn, valueColumn), TupleDomain.withColumnDomains(
                ImmutableMap.<ColumnHandle, Domain>of(textColumn, Domain.create(SortedRangeSet.of(Range.range("bar", true, "foo", true)), false))
        ));

        getCursor(table, ImmutableList.of(textColumn, valueColumn), TupleDomain.withColumnDomains(
                ImmutableMap.<ColumnHandle, Domain>of(textColumn, Domain.create(SortedRangeSet.of(
                                Range.range("bar", true, "foo", true),
                                Range.range("hello", false, "world", false)),
                        false
                ))
        ));

        getCursor(table, ImmutableList.of(textColumn, valueColumn), TupleDomain.withColumnDomains(
                ImmutableMap.<ColumnHandle, Domain>of(
                        textColumn,
                        Domain.create(SortedRangeSet.of(
                                        Range.range("bar", true, "foo", true),
                                        Range.range("hello", false, "world", false),
                                        Range.equal("apple"),
                                        Range.equal("banana"),
                                        Range.equal("zoo")),
                                false
                        ),

                        valueColumn,
                        Domain.create(SortedRangeSet.of(
                                        Range.range(1, true, 5, true),
                                        Range.range(10, false, 20, false)),
                                true
                        )
                )
        ));
    }

    private RecordCursor getCursor(JdbcTableHandle jdbcTableHandle, List<JdbcColumnHandle> columns, TupleDomain<ColumnHandle> domain)
            throws InterruptedException
    {
        ConnectorPartitionResult partitions = jdbcClient.getPartitions(jdbcTableHandle, domain);
        ConnectorSplitSource splits = jdbcClient.getPartitionSplits((JdbcPartition) getOnlyElement(partitions.getPartitions()));
        JdbcSplit split = (JdbcSplit) getOnlyElement(getFutureValue(splits.getNextBatch(1000)));

        JdbcRecordSetProvider recordSetProvider = new JdbcRecordSetProvider(jdbcClient);
        RecordSet recordSet = recordSetProvider.getRecordSet(split, columns);

        return recordSet.cursor();
    }
}
