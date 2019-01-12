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
package io.prestosql.plugin.accumulo.index;

import com.google.common.collect.ImmutableList;
import io.prestosql.plugin.accumulo.metadata.AccumuloTable;
import io.prestosql.plugin.accumulo.model.AccumuloColumnHandle;
import io.prestosql.plugin.accumulo.serializers.AccumuloRowSerializer;
import io.prestosql.plugin.accumulo.serializers.LexicoderRowSerializer;
import io.prestosql.spi.type.ArrayType;
import io.prestosql.spi.type.Type;
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

import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.spi.type.VarcharType.VARCHAR;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;

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

    private Mutation m1;
    private Mutation m2;
    private Mutation m1v;
    private Mutation m2v;
    private AccumuloTable table;

    @BeforeClass
    public void setupClass()
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
        ColumnVisibility visibility2 = new ColumnVisibility("moreprivate");
        m1v = new Mutation(M1_ROWID);
        m1v.put(CF, AGE, visibility1, AGE_VALUE);
        m1v.put(CF, FIRSTNAME, visibility1, M1_FNAME_VALUE);
        m1v.put(CF, SENDERS, visibility2, M1_ARR_VALUE);

        m2v = new Mutation(M2_ROWID);
        m2v.put(CF, AGE, visibility1, AGE_VALUE);
        m2v.put(CF, FIRSTNAME, visibility2, M2_FNAME_VALUE);
        m2v.put(CF, SENDERS, visibility2, M2_ARR_VALUE);
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
        scan.setRange(new Range());

        Iterator<Entry<Key, Value>> iter = scan.iterator();
        assertKeyValuePair(iter.next(), AGE_VALUE, "cf_age", "row1", "");
        assertKeyValuePair(iter.next(), bytes("abc"), "cf_arr", "row1", "");
        assertKeyValuePair(iter.next(), M1_FNAME_VALUE, "cf_firstname", "row1", "");
        assertKeyValuePair(iter.next(), bytes("def"), "cf_arr", "row1", "");
        assertKeyValuePair(iter.next(), bytes("ghi"), "cf_arr", "row1", "");
        assertFalse(iter.hasNext());

        scan.close();

        scan = conn.createScanner(table.getMetricsTableName(), new Authorizations());
        scan.setRange(new Range());

        iter = scan.iterator();
        assertKeyValuePair(iter.next(), AGE_VALUE, "cf_age", "___card___", "1");
        assertKeyValuePair(iter.next(), Indexer.METRICS_TABLE_ROW_ID.array(), "___rows___", "___card___", "1");
        assertKeyValuePair(iter.next(), Indexer.METRICS_TABLE_ROW_ID.array(), "___rows___", "___first_row___", "row1");
        assertKeyValuePair(iter.next(), Indexer.METRICS_TABLE_ROW_ID.array(), "___rows___", "___last_row___", "row1");
        assertKeyValuePair(iter.next(), bytes("abc"), "cf_arr", "___card___", "1");
        assertKeyValuePair(iter.next(), M1_FNAME_VALUE, "cf_firstname", "___card___", "1");
        assertKeyValuePair(iter.next(), bytes("def"), "cf_arr", "___card___", "1");
        assertKeyValuePair(iter.next(), bytes("ghi"), "cf_arr", "___card___", "1");
        assertFalse(iter.hasNext());

        scan.close();

        indexer.index(m2);
        indexer.close();

        scan = conn.createScanner(table.getIndexTableName(), new Authorizations());
        scan.setRange(new Range());
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
        scan.setRange(new Range());

        iter = scan.iterator();
        assertKeyValuePair(iter.next(), AGE_VALUE, "cf_age", "___card___", "2");
        assertKeyValuePair(iter.next(), Indexer.METRICS_TABLE_ROW_ID.array(), "___rows___", "___card___", "2");
        assertKeyValuePair(iter.next(), Indexer.METRICS_TABLE_ROW_ID.array(), "___rows___", "___first_row___", "row1");
        assertKeyValuePair(iter.next(), Indexer.METRICS_TABLE_ROW_ID.array(), "___rows___", "___last_row___", "row2");
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
        indexer.index(m1v);
        indexer.flush();

        Scanner scan = conn.createScanner(table.getIndexTableName(), new Authorizations("private", "moreprivate"));
        scan.setRange(new Range());

        Iterator<Entry<Key, Value>> iter = scan.iterator();
        assertKeyValuePair(iter.next(), AGE_VALUE, "cf_age", "row1", "");
        assertKeyValuePair(iter.next(), AGE_VALUE, "cf_age", "row1", "private", "");
        assertKeyValuePair(iter.next(), bytes("abc"), "cf_arr", "row1", "");
        assertKeyValuePair(iter.next(), bytes("abc"), "cf_arr", "row1", "moreprivate", "");
        assertKeyValuePair(iter.next(), M1_FNAME_VALUE, "cf_firstname", "row1", "");
        assertKeyValuePair(iter.next(), M1_FNAME_VALUE, "cf_firstname", "row1", "private", "");
        assertKeyValuePair(iter.next(), bytes("def"), "cf_arr", "row1", "");
        assertKeyValuePair(iter.next(), bytes("def"), "cf_arr", "row1", "moreprivate", "");
        assertKeyValuePair(iter.next(), bytes("ghi"), "cf_arr", "row1", "");
        assertKeyValuePair(iter.next(), bytes("ghi"), "cf_arr", "row1", "moreprivate", "");
        assertFalse(iter.hasNext());

        scan.close();

        scan = conn.createScanner(table.getMetricsTableName(), new Authorizations("private", "moreprivate"));
        scan.setRange(new Range());

        iter = scan.iterator();
        assertKeyValuePair(iter.next(), AGE_VALUE, "cf_age", "___card___", "1");
        assertKeyValuePair(iter.next(), AGE_VALUE, "cf_age", "___card___", "private", "1");
        assertKeyValuePair(iter.next(), Indexer.METRICS_TABLE_ROW_ID.array(), "___rows___", "___card___", "2");
        assertKeyValuePair(iter.next(), Indexer.METRICS_TABLE_ROW_ID.array(), "___rows___", "___first_row___", "row1");
        assertKeyValuePair(iter.next(), Indexer.METRICS_TABLE_ROW_ID.array(), "___rows___", "___last_row___", "row1");
        assertKeyValuePair(iter.next(), bytes("abc"), "cf_arr", "___card___", "1");
        assertKeyValuePair(iter.next(), bytes("abc"), "cf_arr", "___card___", "moreprivate", "1");
        assertKeyValuePair(iter.next(), M1_FNAME_VALUE, "cf_firstname", "___card___", "1");
        assertKeyValuePair(iter.next(), M1_FNAME_VALUE, "cf_firstname", "___card___", "private", "1");
        assertKeyValuePair(iter.next(), bytes("def"), "cf_arr", "___card___", "1");
        assertKeyValuePair(iter.next(), bytes("def"), "cf_arr", "___card___", "moreprivate", "1");
        assertKeyValuePair(iter.next(), bytes("ghi"), "cf_arr", "___card___", "1");
        assertKeyValuePair(iter.next(), bytes("ghi"), "cf_arr", "___card___", "moreprivate", "1");
        assertFalse(iter.hasNext());

        scan.close();

        indexer.index(m2);
        indexer.index(m2v);
        indexer.close();

        scan = conn.createScanner(table.getIndexTableName(), new Authorizations("private", "moreprivate"));
        scan.setRange(new Range());
        iter = scan.iterator();
        assertKeyValuePair(iter.next(), AGE_VALUE, "cf_age", "row1", "");
        assertKeyValuePair(iter.next(), AGE_VALUE, "cf_age", "row1", "private", "");
        assertKeyValuePair(iter.next(), AGE_VALUE, "cf_age", "row2", "");
        assertKeyValuePair(iter.next(), AGE_VALUE, "cf_age", "row2", "private", "");
        assertKeyValuePair(iter.next(), bytes("abc"), "cf_arr", "row1", "");
        assertKeyValuePair(iter.next(), bytes("abc"), "cf_arr", "row1", "moreprivate", "");
        assertKeyValuePair(iter.next(), bytes("abc"), "cf_arr", "row2", "");
        assertKeyValuePair(iter.next(), bytes("abc"), "cf_arr", "row2", "moreprivate", "");
        assertKeyValuePair(iter.next(), M1_FNAME_VALUE, "cf_firstname", "row1", "");
        assertKeyValuePair(iter.next(), M1_FNAME_VALUE, "cf_firstname", "row1", "private", "");
        assertKeyValuePair(iter.next(), M2_FNAME_VALUE, "cf_firstname", "row2", "");
        assertKeyValuePair(iter.next(), M2_FNAME_VALUE, "cf_firstname", "row2", "moreprivate", "");
        assertKeyValuePair(iter.next(), bytes("def"), "cf_arr", "row1", "");
        assertKeyValuePair(iter.next(), bytes("def"), "cf_arr", "row1", "moreprivate", "");
        assertKeyValuePair(iter.next(), bytes("ghi"), "cf_arr", "row1", "");
        assertKeyValuePair(iter.next(), bytes("ghi"), "cf_arr", "row1", "moreprivate", "");
        assertKeyValuePair(iter.next(), bytes("ghi"), "cf_arr", "row2", "");
        assertKeyValuePair(iter.next(), bytes("ghi"), "cf_arr", "row2", "moreprivate", "");
        assertKeyValuePair(iter.next(), bytes("mno"), "cf_arr", "row2", "");
        assertKeyValuePair(iter.next(), bytes("mno"), "cf_arr", "row2", "moreprivate", "");
        assertFalse(iter.hasNext());

        scan.close();

        scan = conn.createScanner(table.getMetricsTableName(), new Authorizations("private", "moreprivate"));
        scan.setRange(new Range());

        iter = scan.iterator();
        assertKeyValuePair(iter.next(), AGE_VALUE, "cf_age", "___card___", "2");
        assertKeyValuePair(iter.next(), AGE_VALUE, "cf_age", "___card___", "private", "2");
        assertKeyValuePair(iter.next(), Indexer.METRICS_TABLE_ROW_ID.array(), "___rows___", "___card___", "4");
        assertKeyValuePair(iter.next(), Indexer.METRICS_TABLE_ROW_ID.array(), "___rows___", "___first_row___", "row1");
        assertKeyValuePair(iter.next(), Indexer.METRICS_TABLE_ROW_ID.array(), "___rows___", "___last_row___", "row2");
        assertKeyValuePair(iter.next(), bytes("abc"), "cf_arr", "___card___", "2");
        assertKeyValuePair(iter.next(), bytes("abc"), "cf_arr", "___card___", "moreprivate", "2");
        assertKeyValuePair(iter.next(), M1_FNAME_VALUE, "cf_firstname", "___card___", "1");
        assertKeyValuePair(iter.next(), M1_FNAME_VALUE, "cf_firstname", "___card___", "private", "1");
        assertKeyValuePair(iter.next(), M2_FNAME_VALUE, "cf_firstname", "___card___", "1");
        assertKeyValuePair(iter.next(), M2_FNAME_VALUE, "cf_firstname", "___card___", "moreprivate", "1");
        assertKeyValuePair(iter.next(), bytes("def"), "cf_arr", "___card___", "1");
        assertKeyValuePair(iter.next(), bytes("def"), "cf_arr", "___card___", "moreprivate", "1");
        assertKeyValuePair(iter.next(), bytes("ghi"), "cf_arr", "___card___", "2");
        assertKeyValuePair(iter.next(), bytes("ghi"), "cf_arr", "___card___", "moreprivate", "2");
        assertKeyValuePair(iter.next(), bytes("mno"), "cf_arr", "___card___", "1");
        assertKeyValuePair(iter.next(), bytes("mno"), "cf_arr", "___card___", "moreprivate", "1");
        assertFalse(iter.hasNext());

        scan.close();
    }

    private static void assertKeyValuePair(Entry<Key, Value> e, byte[] row, String cf, String cq, String value)
    {
        assertEquals(row, e.getKey().getRow().copyBytes());
        assertEquals(cf, e.getKey().getColumnFamily().toString());
        assertEquals(cq, e.getKey().getColumnQualifier().toString());
        assertEquals(value, e.getValue().toString());
    }

    private static void assertKeyValuePair(Entry<Key, Value> e, byte[] row, String cf, String cq, String cv, String value)
    {
        assertEquals(row, e.getKey().getRow().copyBytes());
        assertEquals(cf, e.getKey().getColumnFamily().toString());
        assertEquals(cq, e.getKey().getColumnQualifier().toString());
        assertEquals(cv, e.getKey().getColumnVisibility().toString());
        assertEquals(value, e.getValue().toString());
    }

    private static byte[] bytes(String s)
    {
        return s.getBytes(UTF_8);
    }
}
