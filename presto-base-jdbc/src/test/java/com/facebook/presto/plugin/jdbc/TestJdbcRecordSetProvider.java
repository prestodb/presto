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
import com.facebook.presto.spi.ConnectorSplitSource;
import com.facebook.presto.spi.RecordCursor;
import com.facebook.presto.spi.RecordSet;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.predicate.Domain;
import com.facebook.presto.spi.predicate.Range;
import com.facebook.presto.spi.predicate.TupleDomain;
import com.facebook.presto.spi.predicate.ValueSet;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.facebook.presto.spi.type.VarcharType.createVarcharType;
import static com.facebook.presto.testing.TestingConnectorSession.SESSION;
import static com.google.common.collect.Iterables.getOnlyElement;
import static io.airlift.concurrent.MoreFutures.getFutureValue;
import static io.airlift.slice.Slices.utf8Slice;
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
    private JdbcColumnHandle textShortColumn;
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
        textShortColumn = columns.get("text_short");
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
        RecordSet recordSet = recordSetProvider.getRecordSet(SESSION, split, ImmutableList.of(textColumn, textShortColumn, valueColumn));
        assertNotNull(recordSet, "recordSet is null");

        RecordCursor cursor = recordSet.cursor();
        assertNotNull(cursor, "cursor is null");

        Map<String, Long> data = new LinkedHashMap<>();
        while (cursor.advanceNextPosition()) {
            data.put(cursor.getSlice(0).toStringUtf8(), cursor.getLong(2));
            assertEquals(cursor.getSlice(0), cursor.getSlice(1));
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
                ImmutableMap.<ColumnHandle, Domain>of(textColumn, Domain.singleValue(VARCHAR, utf8Slice("foo")))
        ));

        // multiple values (string)
        getCursor(table, ImmutableList.of(textColumn, valueColumn), TupleDomain.withColumnDomains(
                ImmutableMap.<ColumnHandle, Domain>of(textColumn, Domain.union(ImmutableList.of(Domain.singleValue(VARCHAR, utf8Slice("foo")), Domain.singleValue(VARCHAR, utf8Slice("bar")))))
        ));

        // inequality (string)
        getCursor(table, ImmutableList.of(textColumn, valueColumn), TupleDomain.withColumnDomains(
                ImmutableMap.<ColumnHandle, Domain>of(textColumn, Domain.create(ValueSet.ofRanges(Range.greaterThan(VARCHAR, utf8Slice("foo"))), false))
        ));

        getCursor(table, ImmutableList.of(textColumn, valueColumn), TupleDomain.withColumnDomains(
                ImmutableMap.<ColumnHandle, Domain>of(textColumn, Domain.create(ValueSet.ofRanges(Range.greaterThan(VARCHAR, utf8Slice("foo"))), false))
        ));

        getCursor(table, ImmutableList.of(textColumn, valueColumn), TupleDomain.withColumnDomains(
                ImmutableMap.<ColumnHandle, Domain>of(textColumn, Domain.create(ValueSet.ofRanges(Range.lessThanOrEqual(VARCHAR, utf8Slice("foo"))), false))
        ));

        getCursor(table, ImmutableList.of(textColumn, valueColumn), TupleDomain.withColumnDomains(
                ImmutableMap.<ColumnHandle, Domain>of(textColumn, Domain.create(ValueSet.ofRanges(Range.lessThan(VARCHAR, utf8Slice("foo"))), false))
        ));

        // is null
        getCursor(table, ImmutableList.of(textColumn, valueColumn), TupleDomain.withColumnDomains(
                ImmutableMap.<ColumnHandle, Domain>of(textColumn, Domain.onlyNull(VARCHAR))
        ));

        // not null
        getCursor(table, ImmutableList.of(textColumn, valueColumn), TupleDomain.withColumnDomains(
                ImmutableMap.<ColumnHandle, Domain>of(textColumn, Domain.notNull(VARCHAR))
        ));

        // specific value or null
        getCursor(table, ImmutableList.of(textColumn, valueColumn), TupleDomain.withColumnDomains(
                ImmutableMap.<ColumnHandle, Domain>of(textColumn, Domain.union(ImmutableList.of(Domain.singleValue(VARCHAR, utf8Slice("foo")), Domain.onlyNull(VARCHAR))))
        ));

        getCursor(table, ImmutableList.of(textColumn, valueColumn), TupleDomain.withColumnDomains(
                ImmutableMap.<ColumnHandle, Domain>of(textColumn, Domain.create(ValueSet.ofRanges(Range.range(VARCHAR, utf8Slice("bar"), true, utf8Slice("foo"), true)), false))
        ));

        getCursor(table, ImmutableList.of(textColumn, textShortColumn, valueColumn), TupleDomain.withColumnDomains(
                ImmutableMap.<ColumnHandle, Domain>of(
                        textColumn,
                        Domain.create(ValueSet.ofRanges(
                                        Range.range(VARCHAR, utf8Slice("bar"), true, utf8Slice("foo"), true),
                                        Range.range(VARCHAR, utf8Slice("hello"), false, utf8Slice("world"), false)),
                                false
                        ),

                        textShortColumn,
                        Domain.create(ValueSet.ofRanges(
                                        Range.range(createVarcharType(32), utf8Slice("bar"), true, utf8Slice("foo"), true),
                                        Range.range(createVarcharType(32), utf8Slice("hello"), false, utf8Slice("world"), false)),
                                false
                        )
                )
        ));

        getCursor(table, ImmutableList.of(textColumn, valueColumn), TupleDomain.withColumnDomains(
                ImmutableMap.<ColumnHandle, Domain>of(
                        textColumn,
                        Domain.create(ValueSet.ofRanges(
                                        Range.range(VARCHAR, utf8Slice("bar"), true, utf8Slice("foo"), true),
                                        Range.range(VARCHAR, utf8Slice("hello"), false, utf8Slice("world"), false),
                                        Range.equal(VARCHAR, utf8Slice("apple")),
                                        Range.equal(VARCHAR, utf8Slice("banana")),
                                        Range.equal(VARCHAR, utf8Slice("zoo"))),
                                false
                        ),

                        valueColumn,
                        Domain.create(ValueSet.ofRanges(
                                        Range.range(BIGINT, 1L, true, 5L, true),
                                        Range.range(BIGINT, 10L, false, 20L, false)),
                                true
                        )
                )
        ));
    }

    private RecordCursor getCursor(JdbcTableHandle jdbcTableHandle, List<JdbcColumnHandle> columns, TupleDomain<ColumnHandle> domain)
            throws InterruptedException
    {
        JdbcTableLayoutHandle layoutHandle = new JdbcTableLayoutHandle(jdbcTableHandle, domain);
        ConnectorSplitSource splits = jdbcClient.getSplits(layoutHandle);
        JdbcSplit split = (JdbcSplit) getOnlyElement(getFutureValue(splits.getNextBatch(1000)));

        JdbcRecordSetProvider recordSetProvider = new JdbcRecordSetProvider(jdbcClient);
        RecordSet recordSet = recordSetProvider.getRecordSet(SESSION, split, columns);

        return recordSet.cursor();
    }
}
