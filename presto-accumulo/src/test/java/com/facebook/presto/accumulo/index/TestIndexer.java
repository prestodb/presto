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

import com.facebook.presto.accumulo.metadata.AccumuloTable;
import com.facebook.presto.accumulo.model.AccumuloColumnHandle;
import com.facebook.presto.accumulo.serializers.AccumuloRowSerializer;
import com.facebook.presto.accumulo.serializers.LexicoderRowSerializer;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.type.ArrayType;
import com.google.common.collect.ImmutableList;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.mock.MockInstance;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Iterator;
import java.util.Map.Entry;
import java.util.Optional;

import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;

public class TestIndexer
{
    private static LexicoderRowSerializer serializer = new LexicoderRowSerializer();

    private static byte[] encode(Type type, Object v)
    {
        return serializer.encode(type, v);
    }

    private static final byte[] AGE = "age".getBytes();
    private static final byte[] CF = "cf".getBytes();
    private static final byte[] FIRSTNAME = "firstname".getBytes();
    private static final byte[] SENDERS = "arr".getBytes();

    private static final byte[] M1_ROWID = encode(VARCHAR, "row1");
    private static final byte[] AGE_VALUE = encode(BIGINT, 27L);
    private static final byte[] M1_FNAME_VALUE = encode(VARCHAR, "alice");
    private static final byte[] M1_ARR_VALUE = encode(new ArrayType(VARCHAR), AccumuloRowSerializer
            .getBlockFromArray(VARCHAR, ImmutableList.of("abc", "def", "ghi")));

    private static final byte[] M2_ROWID = encode(VARCHAR, "row2");
    private static final byte[] M2_FNAME_VALUE = encode(VARCHAR, "bob");
    private static final byte[] M2_ARR_VALUE = encode(new ArrayType(VARCHAR), AccumuloRowSerializer
            .getBlockFromArray(VARCHAR, ImmutableList.of("ghi", "mno", "abc")));

    private Indexer indexer;
    private Instance inst = new MockInstance();
    private Connector conn;

    private Mutation m1 = new Mutation(M1_ROWID);
    private Mutation m2 = new Mutation(M2_ROWID);

    private AccumuloTable table;
    private Authorizations auths;

    @BeforeMethod
    public void setup()
            throws Exception
    {
        conn = inst.getConnector("root", new PasswordToken(""));

        AccumuloColumnHandle c1 =
                new AccumuloColumnHandle("id", Optional.empty(), Optional.empty(), VARCHAR, 0, "", false);
        AccumuloColumnHandle c2 =
                new AccumuloColumnHandle("age", Optional.of("cf"), Optional.of("age"), BIGINT, 1, "", true);
        AccumuloColumnHandle c3 = new AccumuloColumnHandle("firstname", Optional.of("cf"),
                Optional.of("firstname"), VARCHAR, 2, "", true);
        AccumuloColumnHandle c4 = new AccumuloColumnHandle("arr", Optional.of("cf"), Optional.of("arr"),
                new ArrayType(VARCHAR), 3, "", true);

        table = new AccumuloTable("default", "index_test_table", ImmutableList.of(c1, c2, c3, c4),
                "id", true, LexicoderRowSerializer.class.getCanonicalName(), null);

        conn.tableOperations().create(table.getFullTableName());
        conn.tableOperations().create(table.getIndexTableName());
        conn.tableOperations().create(table.getMetricsTableName());
        for (IteratorSetting s : Indexer.getMetricIterators(table)) {
            conn.tableOperations().attachIterator(table.getMetricsTableName(), s);
        }

        m1.put(CF, AGE, AGE_VALUE);
        m1.put(CF, FIRSTNAME, M1_FNAME_VALUE);
        m1.put(CF, SENDERS, M1_ARR_VALUE);
        m2.put(CF, AGE, AGE_VALUE);
        m2.put(CF, FIRSTNAME, M2_FNAME_VALUE);
        m2.put(CF, SENDERS, M2_ARR_VALUE);

        auths = conn.securityOperations().getUserAuthorizations("root");

        indexer = new Indexer(conn, auths, table, new BatchWriterConfig());
    }

    @AfterMethod
    public void cleanup()
            throws AccumuloException, AccumuloSecurityException, TableNotFoundException
    {
        conn.tableOperations().delete(table.getFullTableName());
        conn.tableOperations().delete(table.getIndexTableName());
        conn.tableOperations().delete(table.getMetricsTableName());
    }

    @Test
    public void testMutationIndex()
            throws Exception
    {
        indexer.index(m1);
        indexer.flush();

        Scanner scan = conn.createScanner(table.getIndexTableName(), auths);
        scan.setRange(new Range());

        Iterator<Entry<Key, Value>> iter = scan.iterator();
        assertKeyValuePair(iter.next(), AGE_VALUE, "cf_age", "row1", "");
        assertKeyValuePair(iter.next(), "abc".getBytes(), "cf_arr", "row1", "");
        assertKeyValuePair(iter.next(), M1_FNAME_VALUE, "cf_firstname", "row1", "");
        assertKeyValuePair(iter.next(), "def".getBytes(), "cf_arr", "row1", "");
        assertKeyValuePair(iter.next(), "ghi".getBytes(), "cf_arr", "row1", "");
        assertFalse(iter.hasNext());

        scan.close();

        scan = conn.createScanner(table.getMetricsTableName(), auths);
        scan.setRange(new Range());

        iter = scan.iterator();
        assertKeyValuePair(iter.next(), AGE_VALUE, "cf_age", "___card___", "1");
        assertKeyValuePair(iter.next(), Indexer.METRICS_TABLE_ROW_ID.array(), "___rows___",
                "___card___", "1");
        assertKeyValuePair(iter.next(), Indexer.METRICS_TABLE_ROW_ID.array(), "___rows___",
                "___first_row___", "row1");
        assertKeyValuePair(iter.next(), Indexer.METRICS_TABLE_ROW_ID.array(), "___rows___",
                "___last_row___", "row1");
        assertKeyValuePair(iter.next(), "abc".getBytes(), "cf_arr", "___card___", "1");
        assertKeyValuePair(iter.next(), M1_FNAME_VALUE, "cf_firstname", "___card___", "1");
        assertKeyValuePair(iter.next(), "def".getBytes(), "cf_arr", "___card___", "1");
        assertKeyValuePair(iter.next(), "ghi".getBytes(), "cf_arr", "___card___", "1");
        assertFalse(iter.hasNext());

        scan.close();

        indexer.index(m2);
        indexer.close();

        scan = conn.createScanner(table.getIndexTableName(), auths);
        scan.setRange(new Range());
        iter = scan.iterator();
        assertKeyValuePair(iter.next(), AGE_VALUE, "cf_age", "row1", "");
        assertKeyValuePair(iter.next(), AGE_VALUE, "cf_age", "row2", "");
        assertKeyValuePair(iter.next(), "abc".getBytes(), "cf_arr", "row1", "");
        assertKeyValuePair(iter.next(), "abc".getBytes(), "cf_arr", "row2", "");
        assertKeyValuePair(iter.next(), M1_FNAME_VALUE, "cf_firstname", "row1", "");
        assertKeyValuePair(iter.next(), M2_FNAME_VALUE, "cf_firstname", "row2", "");
        assertKeyValuePair(iter.next(), "def".getBytes(), "cf_arr", "row1", "");
        assertKeyValuePair(iter.next(), "ghi".getBytes(), "cf_arr", "row1", "");
        assertKeyValuePair(iter.next(), "ghi".getBytes(), "cf_arr", "row2", "");
        assertKeyValuePair(iter.next(), "mno".getBytes(), "cf_arr", "row2", "");
        assertFalse(iter.hasNext());

        scan.close();

        scan = conn.createScanner(table.getMetricsTableName(), auths);
        scan.setRange(new Range());

        iter = scan.iterator();
        assertKeyValuePair(iter.next(), AGE_VALUE, "cf_age", "___card___", "2");
        assertKeyValuePair(iter.next(), Indexer.METRICS_TABLE_ROW_ID.array(), "___rows___",
                "___card___", "2");
        assertKeyValuePair(iter.next(), Indexer.METRICS_TABLE_ROW_ID.array(), "___rows___",
                "___first_row___", "row1");
        assertKeyValuePair(iter.next(), Indexer.METRICS_TABLE_ROW_ID.array(), "___rows___",
                "___last_row___", "row2");
        assertKeyValuePair(iter.next(), "abc".getBytes(), "cf_arr", "___card___", "2");
        assertKeyValuePair(iter.next(), M1_FNAME_VALUE, "cf_firstname", "___card___", "1");
        assertKeyValuePair(iter.next(), M2_FNAME_VALUE, "cf_firstname", "___card___", "1");
        assertKeyValuePair(iter.next(), "def".getBytes(), "cf_arr", "___card___", "1");
        assertKeyValuePair(iter.next(), "ghi".getBytes(), "cf_arr", "___card___", "2");
        assertKeyValuePair(iter.next(), "mno".getBytes(), "cf_arr", "___card___", "1");
        assertFalse(iter.hasNext());

        scan.close();
    }

    private void assertKeyValuePair(Entry<Key, Value> e, byte[] row, String cf, String cq,
            String value)
    {
        System.out.println(e);
        assertEquals(row, e.getKey().getRow().copyBytes());
        assertEquals(cf, e.getKey().getColumnFamily().toString());
        assertEquals(cq, e.getKey().getColumnQualifier().toString());
        assertEquals(value, new String(e.getValue().get()));
    }
}
