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
package com.facebook.presto.accumulo.index;

import com.facebook.presto.accumulo.iterators.ValueSummingIterator;
import com.facebook.presto.accumulo.metadata.AccumuloTable;
import com.facebook.presto.accumulo.model.AccumuloColumnHandle;
import com.facebook.presto.accumulo.serializers.AccumuloRowSerializer;
import com.facebook.presto.accumulo.serializers.LexicoderRowSerializer;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.type.ArrayType;
import com.google.common.collect.ImmutableList;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.mock.MockInstance;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.Iterator;
import java.util.Map.Entry;
import java.util.Optional;

import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class TestIndexer
{
    private static final LexicoderRowSerializer SERIALIZER = new LexicoderRowSerializer();

    private static byte[] encode(Type type, Object v)
    {
        return SERIALIZER.encode(type, v);
    }

    private static final byte[] AGE = bytes("age");
    private static final byte[] CF = bytes("cf");
    private static final byte[] FIRSTNAME = bytes("firstname");
    private static final byte[] SENDERS = bytes("arr");

    private static final byte[] M1_ROWID = encode(VARCHAR, "row1");
    private static final byte[] AGE_VALUE = encode(BIGINT, 27L);
    private static final byte[] M1_FNAME_VALUE = encode(VARCHAR, "alice");
    private static final byte[] M1_ARR_VALUE = encode(new ArrayType(VARCHAR), AccumuloRowSerializer.getBlockFromArray(VARCHAR, ImmutableList.of("abc", "def", "ghi")));

    private static final byte[] M2_ROWID = encode(VARCHAR, "row2");
    private static final byte[] M2_FNAME_VALUE = encode(VARCHAR, "bob");
    private static final byte[] M2_ARR_VALUE = encode(new ArrayType(VARCHAR), AccumuloRowSerializer.getBlockFromArray(VARCHAR, ImmutableList.of("ghi", "mno", "abc")));

    private static final byte[] M3_ROWID = encode(VARCHAR, "row3");
    private static final byte[] M3_FNAME_VALUE = encode(VARCHAR, "carol");
    private static final byte[] M3_ARR_VALUE = encode(new ArrayType(VARCHAR), AccumuloRowSerializer.getBlockFromArray(VARCHAR, ImmutableList.of("def", "ghi", "jkl")));

    private Mutation m1;
    private Mutation m2;
    private Mutation m2v;
    private Mutation m3v;
    private AccumuloTable table;

    @BeforeClass
    public void setupClass()
            throws Exception
    {
        AccumuloColumnHandle c1 = new AccumuloColumnHandle("id", Optional.empty(), Optional.empty(), VARCHAR, 0, "", false);
        AccumuloColumnHandle c2 = new AccumuloColumnHandle("age", Optional.of("cf"), Optional.of("age"), BIGINT, 1, "", true);
        AccumuloColumnHandle c3 = new AccumuloColumnHandle("firstname", Optional.of("cf"), Optional.of("firstname"), VARCHAR, 2, "", true);
        AccumuloColumnHandle c4 = new AccumuloColumnHandle("arr", Optional.of("cf"), Optional.of("arr"), new ArrayType(VARCHAR), 3, "", true);

        table = new AccumuloTable("default", "index_test_table", ImmutableList.of(c1, c2, c3, c4), "id", true, LexicoderRowSerializer.class.getCanonicalName(), null);

        m1 = new Mutation(M1_ROWID);
        m1.put(CF, AGE, AGE_VALUE);
        m1.put(CF, FIRSTNAME, M1_FNAME_VALUE);
        m1.put(CF, SENDERS, M1_ARR_VALUE);

        m2 = new Mutation(M2_ROWID);
        m2.put(CF, AGE, AGE_VALUE);
        m2.put(CF, FIRSTNAME, M2_FNAME_VALUE);
        m2.put(CF, SENDERS, M2_ARR_VALUE);

        ColumnVisibility visibility1 = new ColumnVisibility("private");
        m2v = new Mutation(M2_ROWID);
        m2v.put(CF, AGE, visibility1, AGE_VALUE);
        m2v.put(CF, FIRSTNAME, visibility1, M2_FNAME_VALUE);
        m2v.put(CF, SENDERS, visibility1, M2_ARR_VALUE);

        ColumnVisibility visibility2 = new ColumnVisibility("moreprivate");
        m3v = new Mutation(M3_ROWID);
        m3v.put(CF, AGE, visibility2, AGE_VALUE);
        m3v.put(CF, FIRSTNAME, visibility2, M3_FNAME_VALUE);
        m3v.put(CF, SENDERS, visibility2, M3_ARR_VALUE);
    }

    @Test
    public void testMutationIndex()
            throws Exception
    {
        Instance inst = new MockInstance();
        Connector conn = inst.getConnector("root", new PasswordToken(""));
        conn.tableOperations().create(table.getFullTableName());
        conn.tableOperations().create(table.getIndexTableName());
        conn.tableOperations().create(table.getMetricsTableName());

        for (IteratorSetting s : Indexer.getMetricIterators(table)) {
            conn.tableOperations().attachIterator(table.getMetricsTableName(), s);
        }

        Indexer indexer = new Indexer(conn, new Authorizations(), table, new BatchWriterConfig());
        indexer.index(m1);
        indexer.flush();

        Scanner scan = conn.createScanner(table.getIndexTableName(), new Authorizations());

        Iterator<Entry<Key, Value>> iter = scan.iterator();
        assertKeyValuePair(iter.next(), AGE_VALUE, "cf_age", "row1", "");
        assertKeyValuePair(iter.next(), bytes("abc"), "cf_arr", "row1", "");
        assertKeyValuePair(iter.next(), M1_FNAME_VALUE, "cf_firstname", "row1", "");
        assertKeyValuePair(iter.next(), bytes("def"), "cf_arr", "row1", "");
        assertKeyValuePair(iter.next(), bytes("ghi"), "cf_arr", "row1", "");
        assertFalse(iter.hasNext());

        scan.close();

        scan = conn.createScanner(table.getMetricsTableName(), new Authorizations());

        iter = scan.iterator();
        assertKeyValuePair(iter.next(), AGE_VALUE, "cf_age", "___card___", "1");
        assertKeyValuePair(iter.next(), Indexer.METRICS_TABLE_ROW_ID.array(), "___rows___", "___card___", "1");
        assertKeyValuePair(iter.next(), bytes("abc"), "cf_arr", "___card___", "1");
        assertKeyValuePair(iter.next(), M1_FNAME_VALUE, "cf_firstname", "___card___", "1");
        assertKeyValuePair(iter.next(), bytes("def"), "cf_arr", "___card___", "1");
        assertKeyValuePair(iter.next(), bytes("ghi"), "cf_arr", "___card___", "1");
        assertFalse(iter.hasNext());

        scan.close();
        indexer.index(m2);
        indexer.close();

        scan = conn.createScanner(table.getIndexTableName(), new Authorizations());
        iter = scan.iterator();
        assertKeyValuePair(iter.next(), AGE_VALUE, "cf_age", "row1", "");
        assertKeyValuePair(iter.next(), AGE_VALUE, "cf_age", "row2", "");
        assertKeyValuePair(iter.next(), bytes("abc"), "cf_arr", "row1", "");
        assertKeyValuePair(iter.next(), bytes("abc"), "cf_arr", "row2", "");
        assertKeyValuePair(iter.next(), M1_FNAME_VALUE, "cf_firstname", "row1", "");
        assertKeyValuePair(iter.next(), M2_FNAME_VALUE, "cf_firstname", "row2", "");
        assertKeyValuePair(iter.next(), bytes("def"), "cf_arr", "row1", "");
        assertKeyValuePair(iter.next(), bytes("ghi"), "cf_arr", "row1", "");
        assertKeyValuePair(iter.next(), bytes("ghi"), "cf_arr", "row2", "");
        assertKeyValuePair(iter.next(), bytes("mno"), "cf_arr", "row2", "");
        assertFalse(iter.hasNext());

        scan.close();

        scan = conn.createScanner(table.getMetricsTableName(), new Authorizations());
        iter = scan.iterator();
        assertKeyValuePair(iter.next(), AGE_VALUE, "cf_age", "___card___", "2");
        assertKeyValuePair(iter.next(), Indexer.METRICS_TABLE_ROW_ID.array(), "___rows___", "___card___", "2");
        assertKeyValuePair(iter.next(), bytes("abc"), "cf_arr", "___card___", "2");
        assertKeyValuePair(iter.next(), M1_FNAME_VALUE, "cf_firstname", "___card___", "1");
        assertKeyValuePair(iter.next(), M2_FNAME_VALUE, "cf_firstname", "___card___", "1");
        assertKeyValuePair(iter.next(), bytes("def"), "cf_arr", "___card___", "1");
        assertKeyValuePair(iter.next(), bytes("ghi"), "cf_arr", "___card___", "2");
        assertKeyValuePair(iter.next(), bytes("mno"), "cf_arr", "___card___", "1");
        assertFalse(iter.hasNext());

        scan.close();
    }

    @Test
    public void testMutationIndexWithVisibilities()
            throws Exception
    {
        Instance inst = new MockInstance();
        Connector conn = inst.getConnector("root", new PasswordToken(""));
        conn.tableOperations().create(table.getFullTableName());
        conn.tableOperations().create(table.getIndexTableName());
        conn.tableOperations().create(table.getMetricsTableName());

        for (IteratorSetting s : Indexer.getMetricIterators(table)) {
            conn.tableOperations().attachIterator(table.getMetricsTableName(), s);
        }

        Indexer indexer = new Indexer(conn, new Authorizations(), table, new BatchWriterConfig());
        indexer.index(m1);
        indexer.flush();

        Scanner scan = conn.createScanner(table.getIndexTableName(), new Authorizations());

        Iterator<Entry<Key, Value>> iter = scan.iterator();
        assertTrue(iter.hasNext());
        assertKeyValuePair(iter.next(), AGE_VALUE, "cf_age", "row1", "");
        assertKeyValuePair(iter.next(), bytes("abc"), "cf_arr", "row1", "");
        assertKeyValuePair(iter.next(), M1_FNAME_VALUE, "cf_firstname", "row1", "");
        assertKeyValuePair(iter.next(), bytes("def"), "cf_arr", "row1", "");
        assertKeyValuePair(iter.next(), bytes("ghi"), "cf_arr", "row1", "");
        assertFalse(iter.hasNext());

        scan.close();
        scan = conn.createScanner(table.getMetricsTableName(), new Authorizations());
        iter = scan.iterator();
        assertTrue(iter.hasNext());
        assertKeyValuePair(iter.next(), AGE_VALUE, "cf_age", "___card___", "1");
        assertKeyValuePair(iter.next(), Indexer.METRICS_TABLE_ROW_ID.array(), "___rows___", "___card___", "1");
        assertKeyValuePair(iter.next(), bytes("abc"), "cf_arr", "___card___", "1");
        assertKeyValuePair(iter.next(), M1_FNAME_VALUE, "cf_firstname", "___card___", "1");
        assertKeyValuePair(iter.next(), bytes("def"), "cf_arr", "___card___", "1");
        assertKeyValuePair(iter.next(), bytes("ghi"), "cf_arr", "___card___", "1");
        assertFalse(iter.hasNext());

        scan.close();

        indexer.index(m2v);
        indexer.close();

        scan = conn.createScanner(table.getIndexTableName(), new Authorizations("private"));
        iter = scan.iterator();
        assertTrue(iter.hasNext());
        assertKeyValuePair(iter.next(), AGE_VALUE, "cf_age", "row1", "");
        assertKeyValuePair(iter.next(), AGE_VALUE, "cf_age", "row2", "private", "");
        assertKeyValuePair(iter.next(), bytes("abc"), "cf_arr", "row1", "");
        assertKeyValuePair(iter.next(), bytes("abc"), "cf_arr", "row2", "private", "");
        assertKeyValuePair(iter.next(), M1_FNAME_VALUE, "cf_firstname", "row1", "");
        assertKeyValuePair(iter.next(), M2_FNAME_VALUE, "cf_firstname", "row2", "private", "");
        assertKeyValuePair(iter.next(), bytes("def"), "cf_arr", "row1", "");
        assertKeyValuePair(iter.next(), bytes("ghi"), "cf_arr", "row1", "");
        assertKeyValuePair(iter.next(), bytes("ghi"), "cf_arr", "row2", "private", "");
        assertKeyValuePair(iter.next(), bytes("mno"), "cf_arr", "row2", "private", "");
        assertFalse(iter.hasNext());
        scan.close();

        scan = conn.createScanner(table.getMetricsTableName(), new Authorizations("private"));
        iter = scan.iterator();
        assertKeyValuePair(iter.next(), AGE_VALUE, "cf_age", "___card___", "1");
        assertKeyValuePair(iter.next(), AGE_VALUE, "cf_age", "___card___", "private", "1");
        assertKeyValuePair(iter.next(), Indexer.METRICS_TABLE_ROW_ID.array(), "___rows___", "___card___", "1");
        assertKeyValuePair(iter.next(), Indexer.METRICS_TABLE_ROW_ID.array(), "___rows___", "___card___", "private", "1");
        assertKeyValuePair(iter.next(), bytes("abc"), "cf_arr", "___card___", "1");
        assertKeyValuePair(iter.next(), bytes("abc"), "cf_arr", "___card___", "private", "1");
        assertKeyValuePair(iter.next(), M1_FNAME_VALUE, "cf_firstname", "___card___", "1");
        assertKeyValuePair(iter.next(), M2_FNAME_VALUE, "cf_firstname", "___card___", "private", "1");
        assertKeyValuePair(iter.next(), bytes("def"), "cf_arr", "___card___", "1");
        assertKeyValuePair(iter.next(), bytes("ghi"), "cf_arr", "___card___", "1");
        assertKeyValuePair(iter.next(), bytes("ghi"), "cf_arr", "___card___", "private", "1");
        assertKeyValuePair(iter.next(), bytes("mno"), "cf_arr", "___card___", "private", "1");
        assertFalse(iter.hasNext());

        scan.close();

        indexer.index(m3v);
        indexer.close();

        scan = conn.createScanner(table.getIndexTableName(), new Authorizations("private", "moreprivate"));
        iter = scan.iterator();
        assertTrue(iter.hasNext());
        assertKeyValuePair(iter.next(), AGE_VALUE, "cf_age", "row1", "");
        assertKeyValuePair(iter.next(), AGE_VALUE, "cf_age", "row2", "private", "");
        assertKeyValuePair(iter.next(), AGE_VALUE, "cf_age", "row3", "moreprivate", "");
        assertKeyValuePair(iter.next(), bytes("abc"), "cf_arr", "row1", "");
        assertKeyValuePair(iter.next(), bytes("abc"), "cf_arr", "row2", "private", "");
        assertKeyValuePair(iter.next(), M1_FNAME_VALUE, "cf_firstname", "row1", "");
        assertKeyValuePair(iter.next(), M2_FNAME_VALUE, "cf_firstname", "row2", "private", "");
        assertKeyValuePair(iter.next(), M3_FNAME_VALUE, "cf_firstname", "row3", "moreprivate", "");
        assertKeyValuePair(iter.next(), bytes("def"), "cf_arr", "row1", "");
        assertKeyValuePair(iter.next(), bytes("def"), "cf_arr", "row3", "moreprivate", "");
        assertKeyValuePair(iter.next(), bytes("ghi"), "cf_arr", "row1", "");
        assertKeyValuePair(iter.next(), bytes("ghi"), "cf_arr", "row2", "private", "");
        assertKeyValuePair(iter.next(), bytes("ghi"), "cf_arr", "row3", "moreprivate", "");
        assertKeyValuePair(iter.next(), bytes("jkl"), "cf_arr", "row3", "moreprivate", "");
        assertKeyValuePair(iter.next(), bytes("mno"), "cf_arr", "row2", "private", "");
        assertFalse(iter.hasNext());

        scan.close();

        scan = conn.createScanner(table.getMetricsTableName(), new Authorizations("private", "moreprivate"));
        iter = scan.iterator();
        assertTrue(iter.hasNext());
        assertKeyValuePair(iter.next(), AGE_VALUE, "cf_age", "___card___", "1");
        assertKeyValuePair(iter.next(), AGE_VALUE, "cf_age", "___card___", "moreprivate", "1");
        assertKeyValuePair(iter.next(), AGE_VALUE, "cf_age", "___card___", "private", "1");
        assertKeyValuePair(iter.next(), Indexer.METRICS_TABLE_ROW_ID.array(), "___rows___", "___card___", "1");
        assertKeyValuePair(iter.next(), Indexer.METRICS_TABLE_ROW_ID.array(), "___rows___", "___card___", "moreprivate", "1");
        assertKeyValuePair(iter.next(), Indexer.METRICS_TABLE_ROW_ID.array(), "___rows___", "___card___", "private", "1");
        assertKeyValuePair(iter.next(), bytes("abc"), "cf_arr", "___card___", "1");
        assertKeyValuePair(iter.next(), bytes("abc"), "cf_arr", "___card___", "private", "1");
        assertKeyValuePair(iter.next(), M1_FNAME_VALUE, "cf_firstname", "___card___", "1");
        assertKeyValuePair(iter.next(), M2_FNAME_VALUE, "cf_firstname", "___card___", "private", "1");
        assertKeyValuePair(iter.next(), M3_FNAME_VALUE, "cf_firstname", "___card___", "moreprivate", "1");
        assertKeyValuePair(iter.next(), bytes("def"), "cf_arr", "___card___", "1");
        assertKeyValuePair(iter.next(), bytes("def"), "cf_arr", "___card___", "moreprivate", "1");
        assertKeyValuePair(iter.next(), bytes("ghi"), "cf_arr", "___card___", "1");
        assertKeyValuePair(iter.next(), bytes("ghi"), "cf_arr", "___card___", "moreprivate", "1");
        assertKeyValuePair(iter.next(), bytes("ghi"), "cf_arr", "___card___", "private", "1");
        assertKeyValuePair(iter.next(), bytes("jkl"), "cf_arr", "___card___", "moreprivate", "1");
        assertKeyValuePair(iter.next(), bytes("mno"), "cf_arr", "___card___", "private", "1");
        assertFalse(iter.hasNext());

        scan.close();
    }

    @Test
    public void testMutationIndexWithVisibilitiesAndIterator()
            throws Exception
    {
        Instance inst = new MockInstance();
        Connector conn = inst.getConnector("root", new PasswordToken(""));
        conn.tableOperations().create(table.getFullTableName());
        conn.tableOperations().create(table.getIndexTableName());
        conn.tableOperations().create(table.getMetricsTableName());
        for (IteratorSetting s : Indexer.getMetricIterators(table)) {
            conn.tableOperations().attachIterator(table.getMetricsTableName(), s);
        }
        Indexer indexer = new Indexer(conn, new Authorizations(), table, new BatchWriterConfig());
        indexer.index(m1);
        indexer.index(m2v);
        indexer.index(m3v);
        indexer.flush();

        Scanner scan = conn.createScanner(table.getMetricsTableName(), new Authorizations());
        IteratorSetting setting = new IteratorSetting(Integer.MAX_VALUE, "valuesummingiterator", ValueSummingIterator.class);
        ValueSummingIterator.setEncodingType(setting, Indexer.ENCODER_TYPE);
        scan.addScanIterator(setting);
        Iterator<Entry<Key, Value>> iter = scan.iterator();
        assertTrue(iter.hasNext());
        assertEquals(iter.next().getValue().get(), "6".getBytes(UTF_8));
        assertFalse(iter.hasNext());

        scan.close();

        scan = conn.createScanner(table.getMetricsTableName(), new Authorizations("private"));
        scan.addScanIterator(setting);
        iter = scan.iterator();
        assertTrue(iter.hasNext());
        assertEquals(iter.next().getValue().get(), "12".getBytes(UTF_8));
        assertFalse(iter.hasNext());

        scan.close();

        scan = conn.createScanner(table.getMetricsTableName(), new Authorizations("private", "moreprivate"));
        scan.addScanIterator(setting);
        iter = scan.iterator();
        assertTrue(iter.hasNext());
        assertEquals(iter.next().getValue().get(), "18".getBytes(UTF_8));
        assertFalse(iter.hasNext());

        scan.close();

        scan = conn.createScanner(table.getMetricsTableName(), new Authorizations());
        scan.addScanIterator(setting);
        scan.setRange(new Range("ghi"));
        iter = scan.iterator();
        assertTrue(iter.hasNext());
        assertEquals(iter.next().getValue().get(), "1".getBytes(UTF_8));
        assertFalse(iter.hasNext());

        scan.close();

        scan = conn.createScanner(table.getMetricsTableName(), new Authorizations("private"));
        scan.addScanIterator(setting);
        scan.setRange(new Range("ghi"));
        iter = scan.iterator();
        assertTrue(iter.hasNext());
        assertEquals(iter.next().getValue().get(), "2".getBytes(UTF_8));
        assertFalse(iter.hasNext());

        scan.close();

        scan = conn.createScanner(table.getMetricsTableName(), new Authorizations("private", "moreprivate"));
        scan.addScanIterator(setting);
        scan.setRange(new Range("ghi"));
        iter = scan.iterator();
        assertTrue(iter.hasNext());
        assertEquals(iter.next().getValue().get(), "3".getBytes(UTF_8));

        assertFalse(iter.hasNext());

        scan.close();
    }

    private static void assertKeyValuePair(Entry<Key, Value> e, byte[] row, String cf, String cq, String value)
    {
        assertEquals(e.getKey().getRow().copyBytes(), row);
        assertEquals(e.getKey().getColumnFamily().toString(), cf);
        assertEquals(e.getKey().getColumnQualifier().toString(), cq);
        assertEquals(e.getValue().toString(), value);
    }

    private static void assertKeyValuePair(Entry<Key, Value> e, byte[] row, String cf, String cq, String cv, String value)
    {
        assertEquals(e.getKey().getRow().copyBytes(), row);
        assertEquals(e.getKey().getColumnFamily().toString(), cf);
        assertEquals(e.getKey().getColumnQualifier().toString(), cq);
        assertEquals(e.getKey().getColumnVisibility().toString(), cv);
        assertEquals(e.getValue().toString(), value);
    }

    private static byte[] bytes(String s)
    {
        return s.getBytes(UTF_8);
    }
}
