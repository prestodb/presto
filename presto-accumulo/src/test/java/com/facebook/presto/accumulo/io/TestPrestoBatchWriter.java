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
package com.facebook.presto.accumulo.io;

import com.facebook.presto.accumulo.AccumuloQueryRunner;
import com.facebook.presto.accumulo.conf.AccumuloConfig;
import com.facebook.presto.accumulo.index.metrics.MetricCacheKey;
import com.facebook.presto.accumulo.index.metrics.MetricsStorage;
import com.facebook.presto.accumulo.metadata.AccumuloTable;
import com.facebook.presto.accumulo.model.AccumuloColumnHandle;
import com.facebook.presto.accumulo.model.Row;
import com.facebook.presto.accumulo.serializers.LexicoderRowSerializer;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.type.ArrayType;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.io.Text;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.Iterator;
import java.util.Map.Entry;
import java.util.Optional;

import static com.facebook.presto.accumulo.serializers.AccumuloRowSerializer.getBlockFromArray;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

@Test(singleThreaded = true)
public class TestPrestoBatchWriter
{
    private static final LexicoderRowSerializer SERIALIZER = new LexicoderRowSerializer();

    private static final AccumuloConfig CONFIG = new AccumuloConfig();

    private static final byte[] ROW_ID_COLUMN = PrestoBatchWriter.ROW_ID_COLUMN.copyBytes();
    private static final byte[] AGE = bytes("age");
    private static final byte[] CF = bytes("cf");
    private static final byte[] FIRSTNAME = bytes("firstname");
    private static final byte[] SENDERS = bytes("arr");

    private static final byte[] M1_ROWID = encode(VARCHAR, "row1");
    private static final byte[] AGE_VALUE = encode(BIGINT, 27L);
    private static final byte[] UPDATED_AGE_VALUE = encode(BIGINT, 28L);
    private static final byte[] M1_FNAME_VALUE = encode(VARCHAR, "alice");
    private static final byte[] M1_ARR_VALUE = encode(new ArrayType(VARCHAR), getBlockFromArray(VARCHAR, ImmutableList.of("abc", "def", "ghi")));

    private static final byte[] M2_ROWID = encode(VARCHAR, "row2");
    private static final byte[] M2_FNAME_VALUE = encode(VARCHAR, "bob");
    private static final byte[] UPDATED_M2_FNAME_VALUE = encode(VARCHAR, "dave");
    private static final byte[] M2_ARR_VALUE = encode(new ArrayType(VARCHAR), getBlockFromArray(VARCHAR, ImmutableList.of("ghi", "mno", "abc")));

    private static final byte[] M3_ROWID = encode(VARCHAR, "row3");
    private static final byte[] M3_FNAME_VALUE = encode(VARCHAR, "carol");
    private static final byte[] M3_ARR_VALUE = encode(new ArrayType(VARCHAR), getBlockFromArray(VARCHAR, ImmutableList.of("def", "ghi", "jkl")));

    private Row r1 = null;
    private Row r2 = null;
    private Row r3 = null;
    private Mutation m1 = null;
    private Mutation m2v = null;
    private Mutation m3v = null;
    private AccumuloTable table;
    private Connector connector;
    private MetricsStorage metricsStorage;

    @BeforeClass
    public void setupClass()
            throws Exception
    {
        CONFIG.setUsername("root");
        CONFIG.setPassword("secret");

        connector = AccumuloQueryRunner.getAccumuloConnector();
        connector.securityOperations().changeUserAuthorizations("root", new Authorizations("private", "moreprivate", "foo", "bar", "xyzzy"));
        metricsStorage = MetricsStorage.getDefault(connector);

        AccumuloColumnHandle c1 = new AccumuloColumnHandle("id", Optional.empty(), Optional.empty(), VARCHAR, 0, "", false);
        AccumuloColumnHandle c2 = new AccumuloColumnHandle("age", Optional.of("cf"), Optional.of("age"), BIGINT, 1, "", true);
        AccumuloColumnHandle c3 = new AccumuloColumnHandle("firstname", Optional.of("cf"), Optional.of("firstname"), VARCHAR, 2, "", true);
        AccumuloColumnHandle c4 = new AccumuloColumnHandle("arr", Optional.of("cf"), Optional.of("arr"), new ArrayType(VARCHAR), 3, "", true);

        table = new AccumuloTable("default", "presto_batch_writer_test_table", ImmutableList.of(c1, c2, c3, c4), "id", false, LexicoderRowSerializer.class.getCanonicalName(), null, Optional.empty(), false);

        m1 = new Mutation(M1_ROWID);
        m1.put(ROW_ID_COLUMN, ROW_ID_COLUMN, M1_ROWID);
        m1.put(CF, AGE, AGE_VALUE);
        m1.put(CF, FIRSTNAME, M1_FNAME_VALUE);
        m1.put(CF, SENDERS, M1_ARR_VALUE);

        r1 = new Row()
                .addField("row1", VARCHAR)
                .addField(27L, BIGINT)
                .addField("alice", VARCHAR)
                .addField(getBlockFromArray(VARCHAR, ImmutableList.of("abc", "def", "ghi")), new ArrayType(VARCHAR));

        ColumnVisibility visibility1 = new ColumnVisibility("private");
        m2v = new Mutation(M2_ROWID);
        m2v.put(ROW_ID_COLUMN, ROW_ID_COLUMN, M2_ROWID);
        m2v.put(CF, AGE, visibility1, AGE_VALUE);
        m2v.put(CF, FIRSTNAME, visibility1, M2_FNAME_VALUE);
        m2v.put(CF, SENDERS, visibility1, M2_ARR_VALUE);

        r2 = new Row()
                .addField("row2", VARCHAR)
                .addField(27L, BIGINT)
                .addField("bob", VARCHAR)
                .addField(getBlockFromArray(VARCHAR, ImmutableList.of("ghi", "mno", "abc")), new ArrayType(VARCHAR));

        ColumnVisibility visibility2 = new ColumnVisibility("moreprivate");
        m3v = new Mutation(M3_ROWID);
        m3v.put(ROW_ID_COLUMN, ROW_ID_COLUMN, M3_ROWID);
        m3v.put(CF, AGE, visibility2, AGE_VALUE);
        m3v.put(CF, FIRSTNAME, visibility2, M3_FNAME_VALUE);
        m3v.put(CF, SENDERS, visibility2, M3_ARR_VALUE);

        r3 = new Row()
                .addField("row3", VARCHAR)
                .addField(27L, BIGINT)
                .addField("carol", VARCHAR)
                .addField(getBlockFromArray(VARCHAR, ImmutableList.of("def", "ghi", "jkl")), new ArrayType(VARCHAR));
    }

    @AfterMethod
    public void cleanup()
            throws Exception
    {
        if (connector.tableOperations().exists(table.getFullTableName())) {
            connector.tableOperations().delete(table.getFullTableName());
        }

        if (connector.tableOperations().exists(table.getIndexTableName())) {
            connector.tableOperations().delete(table.getIndexTableName());
        }

        metricsStorage.drop(table);
    }

    @Test
    public void testAddRow()
            throws Exception
    {
        connector.tableOperations().create(table.getFullTableName());
        connector.tableOperations().create(table.getIndexTableName());
        metricsStorage.create(table);

        PrestoBatchWriter prestoBatchWriter = new PrestoBatchWriter(connector, connector.securityOperations().getUserAuthorizations("root"), table);
        prestoBatchWriter.addRow(r1);
        prestoBatchWriter.flush();

        Scanner scan = connector.createScanner(table.getFullTableName(), new Authorizations("private", "moreprivate"));
        Iterator<Entry<Key, Value>> iter = scan.iterator();
        assertTrue(iter.hasNext());
        assertKeyValuePair(iter.next(), bytes("row1"), "___ROW___", "___ROW___", "row1");
        assertKeyValuePair(iter.next(), bytes("row1"), "cf", "age", AGE_VALUE);
        assertKeyValuePair(iter.next(), bytes("row1"), "cf", "arr", M1_ARR_VALUE);
        assertKeyValuePair(iter.next(), bytes("row1"), "cf", "firstname", M1_FNAME_VALUE);
        assertFalse(iter.hasNext());

        scan = connector.createScanner(table.getIndexTableName(), new Authorizations());
        iter = scan.iterator();
        assertTrue(iter.hasNext());
        assertKeyValuePair(iter.next(), AGE_VALUE, "cf_age", "row1", "");
        assertKeyValuePair(iter.next(), bytes("abc"), "cf_arr", "row1", "");
        assertKeyValuePair(iter.next(), M1_FNAME_VALUE, "cf_firstname", "row1", "");
        assertKeyValuePair(iter.next(), bytes("def"), "cf_arr", "row1", "");
        assertKeyValuePair(iter.next(), bytes("ghi"), "cf_arr", "row1", "");
        assertFalse(iter.hasNext());

        scan.close();

        assertEquals(metricsStorage.newReader().getCardinality(mck("cf", "age", AGE_VALUE)), 1);
        assertEquals(metricsStorage.newReader().getNumRowsInTable(table.getSchema(), table.getTable()), 1);
        assertEquals(metricsStorage.newReader().getCardinality(mck("cf", "arr", "abc")), 1);
        assertEquals(metricsStorage.newReader().getCardinality(mck("cf", "firstname", M1_FNAME_VALUE)), 1);
        assertEquals(metricsStorage.newReader().getCardinality(mck("cf", "arr", "def")), 1);
        assertEquals(metricsStorage.newReader().getCardinality(mck("cf", "arr", "ghi")), 1);

        prestoBatchWriter.addRow(r2);
        prestoBatchWriter.flush();

        scan = connector.createScanner(table.getFullTableName(), new Authorizations("private", "moreprivate"));
        iter = scan.iterator();
        assertTrue(iter.hasNext());
        assertKeyValuePair(iter.next(), bytes("row1"), "___ROW___", "___ROW___", "row1");
        assertKeyValuePair(iter.next(), bytes("row1"), "cf", "age", AGE_VALUE);
        assertKeyValuePair(iter.next(), bytes("row1"), "cf", "arr", M1_ARR_VALUE);
        assertKeyValuePair(iter.next(), bytes("row1"), "cf", "firstname", M1_FNAME_VALUE);
        assertKeyValuePair(iter.next(), bytes("row2"), "___ROW___", "___ROW___", "row2");
        assertKeyValuePair(iter.next(), bytes("row2"), "cf", "age", AGE_VALUE);
        assertKeyValuePair(iter.next(), bytes("row2"), "cf", "arr", M2_ARR_VALUE);
        assertKeyValuePair(iter.next(), bytes("row2"), "cf", "firstname", M2_FNAME_VALUE);
        assertFalse(iter.hasNext());

        scan = connector.createScanner(table.getIndexTableName(), new Authorizations());
        iter = scan.iterator();
        assertTrue(iter.hasNext());
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

        assertEquals(metricsStorage.newReader().getCardinality(mck("cf", "age", AGE_VALUE)), 2);
        assertEquals(metricsStorage.newReader().getNumRowsInTable(table.getSchema(), table.getTable()), 2);
        assertEquals(metricsStorage.newReader().getCardinality(mck("cf", "arr", "abc")), 2);
        assertEquals(metricsStorage.newReader().getCardinality(mck("cf", "firstname", M1_FNAME_VALUE)), 1);
        assertEquals(metricsStorage.newReader().getCardinality(mck("cf", "firstname", M2_FNAME_VALUE)), 1);
        assertEquals(metricsStorage.newReader().getCardinality(mck("cf", "arr", "def")), 1);
        assertEquals(metricsStorage.newReader().getCardinality(mck("cf", "arr", "ghi")), 2);
        assertEquals(metricsStorage.newReader().getCardinality(mck("cf", "arr", "mno")), 1);

        prestoBatchWriter.addRow(r3);
        prestoBatchWriter.close();

        scan = connector.createScanner(table.getFullTableName(), new Authorizations("private", "moreprivate"));
        iter = scan.iterator();
        assertTrue(iter.hasNext());
        assertKeyValuePair(iter.next(), bytes("row1"), "___ROW___", "___ROW___", "row1");
        assertKeyValuePair(iter.next(), bytes("row1"), "cf", "age", AGE_VALUE);
        assertKeyValuePair(iter.next(), bytes("row1"), "cf", "arr", M1_ARR_VALUE);
        assertKeyValuePair(iter.next(), bytes("row1"), "cf", "firstname", M1_FNAME_VALUE);
        assertKeyValuePair(iter.next(), bytes("row2"), "___ROW___", "___ROW___", "row2");
        assertKeyValuePair(iter.next(), bytes("row2"), "cf", "age", AGE_VALUE);
        assertKeyValuePair(iter.next(), bytes("row2"), "cf", "arr", M2_ARR_VALUE);
        assertKeyValuePair(iter.next(), bytes("row2"), "cf", "firstname", M2_FNAME_VALUE);
        assertKeyValuePair(iter.next(), bytes("row3"), "___ROW___", "___ROW___", "row3");
        assertKeyValuePair(iter.next(), bytes("row3"), "cf", "age", AGE_VALUE);
        assertKeyValuePair(iter.next(), bytes("row3"), "cf", "arr", M3_ARR_VALUE);
        assertKeyValuePair(iter.next(), bytes("row3"), "cf", "firstname", M3_FNAME_VALUE);
        assertFalse(iter.hasNext());

        scan = connector.createScanner(table.getIndexTableName(), new Authorizations());
        iter = scan.iterator();
        assertTrue(iter.hasNext());
        assertKeyValuePair(iter.next(), AGE_VALUE, "cf_age", "row1", "");
        assertKeyValuePair(iter.next(), AGE_VALUE, "cf_age", "row2", "");
        assertKeyValuePair(iter.next(), AGE_VALUE, "cf_age", "row3", "");
        assertKeyValuePair(iter.next(), bytes("abc"), "cf_arr", "row1", "");
        assertKeyValuePair(iter.next(), bytes("abc"), "cf_arr", "row2", "");
        assertKeyValuePair(iter.next(), M1_FNAME_VALUE, "cf_firstname", "row1", "");
        assertKeyValuePair(iter.next(), M2_FNAME_VALUE, "cf_firstname", "row2", "");
        assertKeyValuePair(iter.next(), M3_FNAME_VALUE, "cf_firstname", "row3", "");
        assertKeyValuePair(iter.next(), bytes("def"), "cf_arr", "row1", "");
        assertKeyValuePair(iter.next(), bytes("def"), "cf_arr", "row3", "");
        assertKeyValuePair(iter.next(), bytes("ghi"), "cf_arr", "row1", "");
        assertKeyValuePair(iter.next(), bytes("ghi"), "cf_arr", "row2", "");
        assertKeyValuePair(iter.next(), bytes("ghi"), "cf_arr", "row3", "");
        assertKeyValuePair(iter.next(), bytes("jkl"), "cf_arr", "row3", "");
        assertKeyValuePair(iter.next(), bytes("mno"), "cf_arr", "row2", "");
        assertFalse(iter.hasNext());

        scan.close();

        assertEquals(metricsStorage.newReader().getCardinality(mck("cf", "age", AGE_VALUE)), 3);
        assertEquals(metricsStorage.newReader().getNumRowsInTable(table.getSchema(), table.getTable()), 3);
        assertEquals(metricsStorage.newReader().getCardinality(mck("cf", "arr", "abc")), 2);
        assertEquals(metricsStorage.newReader().getCardinality(mck("cf", "firstname", M1_FNAME_VALUE)), 1);
        assertEquals(metricsStorage.newReader().getCardinality(mck("cf", "firstname", M2_FNAME_VALUE)), 1);
        assertEquals(metricsStorage.newReader().getCardinality(mck("cf", "firstname", M3_FNAME_VALUE)), 1);
        assertEquals(metricsStorage.newReader().getCardinality(mck("cf", "arr", "def")), 2);
        assertEquals(metricsStorage.newReader().getCardinality(mck("cf", "arr", "ghi")), 3);
        assertEquals(metricsStorage.newReader().getCardinality(mck("cf", "arr", "jkl")), 1);
        assertEquals(metricsStorage.newReader().getCardinality(mck("cf", "arr", "mno")), 1);
    }

    @Test
    public void testAddRows()
            throws Exception
    {
        connector.tableOperations().create(table.getFullTableName());
        connector.tableOperations().create(table.getIndexTableName());
        metricsStorage.create(table);

        PrestoBatchWriter prestoBatchWriter = new PrestoBatchWriter(connector, connector.securityOperations().getUserAuthorizations("root"), table);
        prestoBatchWriter.addRows(ImmutableList.of(r1, r2, r3));
        prestoBatchWriter.flush();

        Scanner scan = connector.createScanner(table.getFullTableName(), new Authorizations("private", "moreprivate"));
        Iterator<Entry<Key, Value>> iter = scan.iterator();
        assertTrue(iter.hasNext());
        assertKeyValuePair(iter.next(), bytes("row1"), "___ROW___", "___ROW___", "row1");
        assertKeyValuePair(iter.next(), bytes("row1"), "cf", "age", AGE_VALUE);
        assertKeyValuePair(iter.next(), bytes("row1"), "cf", "arr", M1_ARR_VALUE);
        assertKeyValuePair(iter.next(), bytes("row1"), "cf", "firstname", M1_FNAME_VALUE);
        assertKeyValuePair(iter.next(), bytes("row2"), "___ROW___", "___ROW___", "row2");
        assertKeyValuePair(iter.next(), bytes("row2"), "cf", "age", AGE_VALUE);
        assertKeyValuePair(iter.next(), bytes("row2"), "cf", "arr", M2_ARR_VALUE);
        assertKeyValuePair(iter.next(), bytes("row2"), "cf", "firstname", M2_FNAME_VALUE);
        assertKeyValuePair(iter.next(), bytes("row3"), "___ROW___", "___ROW___", "row3");
        assertKeyValuePair(iter.next(), bytes("row3"), "cf", "age", AGE_VALUE);
        assertKeyValuePair(iter.next(), bytes("row3"), "cf", "arr", M3_ARR_VALUE);
        assertKeyValuePair(iter.next(), bytes("row3"), "cf", "firstname", M3_FNAME_VALUE);
        assertFalse(iter.hasNext());

        scan = connector.createScanner(table.getIndexTableName(), new Authorizations());
        iter = scan.iterator();
        assertTrue(iter.hasNext());
        assertKeyValuePair(iter.next(), AGE_VALUE, "cf_age", "row1", "");
        assertKeyValuePair(iter.next(), AGE_VALUE, "cf_age", "row2", "");
        assertKeyValuePair(iter.next(), AGE_VALUE, "cf_age", "row3", "");
        assertKeyValuePair(iter.next(), bytes("abc"), "cf_arr", "row1", "");
        assertKeyValuePair(iter.next(), bytes("abc"), "cf_arr", "row2", "");
        assertKeyValuePair(iter.next(), M1_FNAME_VALUE, "cf_firstname", "row1", "");
        assertKeyValuePair(iter.next(), M2_FNAME_VALUE, "cf_firstname", "row2", "");
        assertKeyValuePair(iter.next(), M3_FNAME_VALUE, "cf_firstname", "row3", "");
        assertKeyValuePair(iter.next(), bytes("def"), "cf_arr", "row1", "");
        assertKeyValuePair(iter.next(), bytes("def"), "cf_arr", "row3", "");
        assertKeyValuePair(iter.next(), bytes("ghi"), "cf_arr", "row1", "");
        assertKeyValuePair(iter.next(), bytes("ghi"), "cf_arr", "row2", "");
        assertKeyValuePair(iter.next(), bytes("ghi"), "cf_arr", "row3", "");
        assertKeyValuePair(iter.next(), bytes("jkl"), "cf_arr", "row3", "");
        assertKeyValuePair(iter.next(), bytes("mno"), "cf_arr", "row2", "");
        assertFalse(iter.hasNext());

        scan.close();

        assertEquals(metricsStorage.newReader().getCardinality(mck("cf", "age", AGE_VALUE)), 3);
        assertEquals(metricsStorage.newReader().getNumRowsInTable(table.getSchema(), table.getTable()), 3);
        assertEquals(metricsStorage.newReader().getCardinality(mck("cf", "arr", "abc")), 2);
        assertEquals(metricsStorage.newReader().getCardinality(mck("cf", "firstname", M1_FNAME_VALUE)), 1);
        assertEquals(metricsStorage.newReader().getCardinality(mck("cf", "firstname", M2_FNAME_VALUE)), 1);
        assertEquals(metricsStorage.newReader().getCardinality(mck("cf", "firstname", M3_FNAME_VALUE)), 1);
        assertEquals(metricsStorage.newReader().getCardinality(mck("cf", "arr", "def")), 2);
        assertEquals(metricsStorage.newReader().getCardinality(mck("cf", "arr", "ghi")), 3);
        assertEquals(metricsStorage.newReader().getCardinality(mck("cf", "arr", "jkl")), 1);
        assertEquals(metricsStorage.newReader().getCardinality(mck("cf", "arr", "mno")), 1);
    }

    @Test
    public void testAddMutation()
            throws Exception
    {
        connector.tableOperations().create(table.getFullTableName());
        connector.tableOperations().create(table.getIndexTableName());
        metricsStorage.create(table);

        PrestoBatchWriter prestoBatchWriter = new PrestoBatchWriter(connector, connector.securityOperations().getUserAuthorizations("root"), table);
        prestoBatchWriter.addMutation(m1);
        prestoBatchWriter.flush();

        Scanner scan = connector.createScanner(table.getFullTableName(), new Authorizations("private", "moreprivate"));
        Iterator<Entry<Key, Value>> iter = scan.iterator();
        assertTrue(iter.hasNext());
        assertKeyValuePair(iter.next(), bytes("row1"), "___ROW___", "___ROW___", "row1");
        assertKeyValuePair(iter.next(), bytes("row1"), "cf", "age", AGE_VALUE);
        assertKeyValuePair(iter.next(), bytes("row1"), "cf", "arr", M1_ARR_VALUE);
        assertKeyValuePair(iter.next(), bytes("row1"), "cf", "firstname", M1_FNAME_VALUE);
        assertFalse(iter.hasNext());

        scan.close();

        scan = connector.createScanner(table.getIndexTableName(), new Authorizations("private", "moreprivate"));
        iter = scan.iterator();

        assertTrue(iter.hasNext());
        assertKeyValuePair(iter.next(), AGE_VALUE, "cf_age", "row1", "");
        assertKeyValuePair(iter.next(), bytes("abc"), "cf_arr", "row1", "");
        assertKeyValuePair(iter.next(), M1_FNAME_VALUE, "cf_firstname", "row1", "");
        assertKeyValuePair(iter.next(), bytes("def"), "cf_arr", "row1", "");
        assertKeyValuePair(iter.next(), bytes("ghi"), "cf_arr", "row1", "");
        assertFalse(iter.hasNext());

        scan.close();

        assertEquals(metricsStorage.newReader().getCardinality(mck("cf", "age", AGE_VALUE, "private", "moreprivate")), 1);
        assertEquals(metricsStorage.newReader().getNumRowsInTable(table.getSchema(), table.getTable()), 1);
        assertEquals(metricsStorage.newReader().getCardinality(mck("cf", "arr", "abc", "private", "moreprivate")), 1);
        assertEquals(metricsStorage.newReader().getCardinality(mck("cf", "firstname", M1_FNAME_VALUE, "private", "moreprivate")), 1);
        assertEquals(metricsStorage.newReader().getCardinality(mck("cf", "arr", "def", "private", "moreprivate")), 1);
        assertEquals(metricsStorage.newReader().getCardinality(mck("cf", "arr", "ghi", "private", "moreprivate")), 1);

        prestoBatchWriter.addMutation(m2v);
        prestoBatchWriter.flush();

        scan = connector.createScanner(table.getFullTableName(), new Authorizations("private", "moreprivate"));
        iter = scan.iterator();
        assertTrue(iter.hasNext());
        assertKeyValuePair(iter.next(), bytes("row1"), "___ROW___", "___ROW___", "row1");
        assertKeyValuePair(iter.next(), bytes("row1"), "cf", "age", AGE_VALUE);
        assertKeyValuePair(iter.next(), bytes("row1"), "cf", "arr", M1_ARR_VALUE);
        assertKeyValuePair(iter.next(), bytes("row1"), "cf", "firstname", M1_FNAME_VALUE);
        assertKeyValuePair(iter.next(), bytes("row2"), "___ROW___", "___ROW___", "row2");
        assertKeyValuePair(iter.next(), bytes("row2"), "cf", "age", AGE_VALUE);
        assertKeyValuePair(iter.next(), bytes("row2"), "cf", "arr", M2_ARR_VALUE);
        assertKeyValuePair(iter.next(), bytes("row2"), "cf", "firstname", M2_FNAME_VALUE);
        assertFalse(iter.hasNext());

        scan = connector.createScanner(table.getIndexTableName(), new Authorizations("private", "moreprivate"));
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

        assertEquals(metricsStorage.newReader().getCardinality(mck("cf", "age", AGE_VALUE, "private", "moreprivate")), 2);
        assertEquals(metricsStorage.newReader().getNumRowsInTable(table.getSchema(), table.getTable()), 2);
        assertEquals(metricsStorage.newReader().getCardinality(mck("cf", "arr", "abc", "private", "moreprivate")), 2);
        assertEquals(metricsStorage.newReader().getCardinality(mck("cf", "firstname", M1_FNAME_VALUE, "private", "moreprivate")), 1);
        assertEquals(metricsStorage.newReader().getCardinality(mck("cf", "firstname", M2_FNAME_VALUE, "private", "moreprivate")), 1);
        assertEquals(metricsStorage.newReader().getCardinality(mck("cf", "arr", "def", "private", "moreprivate")), 1);
        assertEquals(metricsStorage.newReader().getCardinality(mck("cf", "arr", "ghi", "private", "moreprivate")), 2);
        assertEquals(metricsStorage.newReader().getCardinality(mck("cf", "arr", "mno", "private", "moreprivate")), 1);

        prestoBatchWriter.addMutation(m3v);
        prestoBatchWriter.close();

        scan = connector.createScanner(table.getFullTableName(), new Authorizations("private", "moreprivate"));
        iter = scan.iterator();
        assertTrue(iter.hasNext());
        assertKeyValuePair(iter.next(), bytes("row1"), "___ROW___", "___ROW___", "row1");
        assertKeyValuePair(iter.next(), bytes("row1"), "cf", "age", AGE_VALUE);
        assertKeyValuePair(iter.next(), bytes("row1"), "cf", "arr", M1_ARR_VALUE);
        assertKeyValuePair(iter.next(), bytes("row1"), "cf", "firstname", M1_FNAME_VALUE);
        assertKeyValuePair(iter.next(), bytes("row2"), "___ROW___", "___ROW___", "row2");
        assertKeyValuePair(iter.next(), bytes("row2"), "cf", "age", AGE_VALUE);
        assertKeyValuePair(iter.next(), bytes("row2"), "cf", "arr", M2_ARR_VALUE);
        assertKeyValuePair(iter.next(), bytes("row2"), "cf", "firstname", M2_FNAME_VALUE);
        assertKeyValuePair(iter.next(), bytes("row3"), "___ROW___", "___ROW___", "row3");
        assertKeyValuePair(iter.next(), bytes("row3"), "cf", "age", AGE_VALUE);
        assertKeyValuePair(iter.next(), bytes("row3"), "cf", "arr", M3_ARR_VALUE);
        assertKeyValuePair(iter.next(), bytes("row3"), "cf", "firstname", M3_FNAME_VALUE);
        assertFalse(iter.hasNext());

        scan = connector.createScanner(table.getIndexTableName(), new Authorizations("private", "moreprivate"));
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

        assertEquals(metricsStorage.newReader().getCardinality(mck("cf", "age", AGE_VALUE, "private", "moreprivate")), 3);
        assertEquals(metricsStorage.newReader().getNumRowsInTable(table.getSchema(), table.getTable()), 3);
        assertEquals(metricsStorage.newReader().getCardinality(mck("cf", "arr", "abc", "private", "moreprivate")), 2);
        assertEquals(metricsStorage.newReader().getCardinality(mck("cf", "firstname", M1_FNAME_VALUE, "private", "moreprivate")), 1);
        assertEquals(metricsStorage.newReader().getCardinality(mck("cf", "firstname", M2_FNAME_VALUE, "private", "moreprivate")), 1);
        assertEquals(metricsStorage.newReader().getCardinality(mck("cf", "firstname", M3_FNAME_VALUE, "private", "moreprivate")), 1);
        assertEquals(metricsStorage.newReader().getCardinality(mck("cf", "arr", "def", "private", "moreprivate")), 2);
        assertEquals(metricsStorage.newReader().getCardinality(mck("cf", "arr", "ghi", "private", "moreprivate")), 3);
        assertEquals(metricsStorage.newReader().getCardinality(mck("cf", "arr", "jkl", "private", "moreprivate")), 1);
        assertEquals(metricsStorage.newReader().getCardinality(mck("cf", "arr", "mno", "private", "moreprivate")), 1);
    }

    @Test
    public void testAddMutations()
            throws Exception
    {
        connector.tableOperations().create(table.getFullTableName());
        connector.tableOperations().create(table.getIndexTableName());
        metricsStorage.create(table);

        PrestoBatchWriter prestoBatchWriter = new PrestoBatchWriter(connector, connector.securityOperations().getUserAuthorizations("root"), table);
        prestoBatchWriter.addMutations(ImmutableList.of(m1, m2v, m3v));
        prestoBatchWriter.close();

        Scanner scan = connector.createScanner(table.getFullTableName(), new Authorizations("private", "moreprivate"));
        Iterator<Entry<Key, Value>> iter = scan.iterator();
        assertTrue(iter.hasNext());
        assertKeyValuePair(iter.next(), bytes("row1"), "___ROW___", "___ROW___", "row1");
        assertKeyValuePair(iter.next(), bytes("row1"), "cf", "age", AGE_VALUE);
        assertKeyValuePair(iter.next(), bytes("row1"), "cf", "arr", M1_ARR_VALUE);
        assertKeyValuePair(iter.next(), bytes("row1"), "cf", "firstname", M1_FNAME_VALUE);
        assertKeyValuePair(iter.next(), bytes("row2"), "___ROW___", "___ROW___", "row2");
        assertKeyValuePair(iter.next(), bytes("row2"), "cf", "age", AGE_VALUE);
        assertKeyValuePair(iter.next(), bytes("row2"), "cf", "arr", M2_ARR_VALUE);
        assertKeyValuePair(iter.next(), bytes("row2"), "cf", "firstname", M2_FNAME_VALUE);
        assertKeyValuePair(iter.next(), bytes("row3"), "___ROW___", "___ROW___", "row3");
        assertKeyValuePair(iter.next(), bytes("row3"), "cf", "age", AGE_VALUE);
        assertKeyValuePair(iter.next(), bytes("row3"), "cf", "arr", M3_ARR_VALUE);
        assertKeyValuePair(iter.next(), bytes("row3"), "cf", "firstname", M3_FNAME_VALUE);
        assertFalse(iter.hasNext());

        scan = connector.createScanner(table.getIndexTableName(), new Authorizations("private", "moreprivate"));
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

        assertEquals(metricsStorage.newReader().getCardinality(mck("cf", "age", AGE_VALUE, "private", "moreprivate")), 3);
        assertEquals(metricsStorage.newReader().getNumRowsInTable(table.getSchema(), table.getTable()), 3);
        assertEquals(metricsStorage.newReader().getCardinality(mck("cf", "arr", "abc", "private", "moreprivate")), 2);
        assertEquals(metricsStorage.newReader().getCardinality(mck("cf", "firstname", M1_FNAME_VALUE, "private", "moreprivate")), 1);
        assertEquals(metricsStorage.newReader().getCardinality(mck("cf", "firstname", M2_FNAME_VALUE, "private", "moreprivate")), 1);
        assertEquals(metricsStorage.newReader().getCardinality(mck("cf", "firstname", M3_FNAME_VALUE, "private", "moreprivate")), 1);
        assertEquals(metricsStorage.newReader().getCardinality(mck("cf", "arr", "def", "private", "moreprivate")), 2);
        assertEquals(metricsStorage.newReader().getCardinality(mck("cf", "arr", "ghi", "private", "moreprivate")), 3);
        assertEquals(metricsStorage.newReader().getCardinality(mck("cf", "arr", "jkl", "private", "moreprivate")), 1);
        assertEquals(metricsStorage.newReader().getCardinality(mck("cf", "arr", "mno", "private", "moreprivate")), 1);
    }

    @Test
    public void testUpdateColumn()
            throws Exception
    {
        testAddMutations();

        PrestoBatchWriter prestoBatchWriter = new PrestoBatchWriter(connector, connector.securityOperations().getUserAuthorizations("root"), table);
        prestoBatchWriter.updateColumnByName("row2", "age", 28L);
        prestoBatchWriter.flush();

        Scanner scan = connector.createScanner(table.getFullTableName(), new Authorizations("private", "moreprivate"));
        Iterator<Entry<Key, Value>> iter = scan.iterator();
        assertTrue(iter.hasNext());
        assertKeyValuePair(iter.next(), bytes("row1"), "___ROW___", "___ROW___", "row1");
        assertKeyValuePair(iter.next(), bytes("row1"), "cf", "age", AGE_VALUE);
        assertKeyValuePair(iter.next(), bytes("row1"), "cf", "arr", M1_ARR_VALUE);
        assertKeyValuePair(iter.next(), bytes("row1"), "cf", "firstname", M1_FNAME_VALUE);
        assertKeyValuePair(iter.next(), bytes("row2"), "___ROW___", "___ROW___", "row2");
        assertKeyValuePair(iter.next(), bytes("row2"), "cf", "age", encode(BIGINT, 28L));
        assertKeyValuePair(iter.next(), bytes("row2"), "cf", "arr", M2_ARR_VALUE);
        assertKeyValuePair(iter.next(), bytes("row2"), "cf", "firstname", M2_FNAME_VALUE);
        assertKeyValuePair(iter.next(), bytes("row3"), "___ROW___", "___ROW___", "row3");
        assertKeyValuePair(iter.next(), bytes("row3"), "cf", "age", AGE_VALUE);
        assertKeyValuePair(iter.next(), bytes("row3"), "cf", "arr", M3_ARR_VALUE);
        assertKeyValuePair(iter.next(), bytes("row3"), "cf", "firstname", M3_FNAME_VALUE);
        assertFalse(iter.hasNext());

        scan = connector.createScanner(table.getIndexTableName(), new Authorizations("private", "moreprivate"));
        iter = scan.iterator();
        assertTrue(iter.hasNext());
        assertKeyValuePair(iter.next(), AGE_VALUE, "cf_age", "row1", "");
        assertKeyValuePair(iter.next(), AGE_VALUE, "cf_age", "row3", "moreprivate", "");
        assertKeyValuePair(iter.next(), UPDATED_AGE_VALUE, "cf_age", "row2", "", "");
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

        assertEquals(metricsStorage.newReader().getCardinality(mck("cf", "age", AGE_VALUE, "private", "moreprivate")), 2);
        assertEquals(metricsStorage.newReader().getCardinality(mck("cf", "age", UPDATED_AGE_VALUE)), 1);
        assertEquals(metricsStorage.newReader().getNumRowsInTable(table.getSchema(), table.getTable()), 3);
        assertEquals(metricsStorage.newReader().getCardinality(mck("cf", "arr", "abc", "private", "moreprivate")), 2);
        assertEquals(metricsStorage.newReader().getCardinality(mck("cf", "firstname", M1_FNAME_VALUE, "private", "moreprivate")), 1);
        assertEquals(metricsStorage.newReader().getCardinality(mck("cf", "firstname", M2_FNAME_VALUE, "private", "moreprivate")), 1);
        assertEquals(metricsStorage.newReader().getCardinality(mck("cf", "firstname", M3_FNAME_VALUE, "private", "moreprivate")), 1);
        assertEquals(metricsStorage.newReader().getCardinality(mck("cf", "arr", "def", "private", "moreprivate")), 2);
        assertEquals(metricsStorage.newReader().getCardinality(mck("cf", "arr", "ghi", "private", "moreprivate")), 3);
        assertEquals(metricsStorage.newReader().getCardinality(mck("cf", "arr", "jkl", "private", "moreprivate")), 1);
        assertEquals(metricsStorage.newReader().getCardinality(mck("cf", "arr", "mno", "private", "moreprivate")), 1);

        prestoBatchWriter.updateColumnByName("row2", "firstname", new ColumnVisibility("moreprivate"), "dave");
        prestoBatchWriter.flush();

        scan = connector.createScanner(table.getFullTableName(), new Authorizations("private", "moreprivate"));
        iter = scan.iterator();
        assertTrue(iter.hasNext());
        assertKeyValuePair(iter.next(), bytes("row1"), "___ROW___", "___ROW___", "row1");
        assertKeyValuePair(iter.next(), bytes("row1"), "cf", "age", AGE_VALUE);
        assertKeyValuePair(iter.next(), bytes("row1"), "cf", "arr", M1_ARR_VALUE);
        assertKeyValuePair(iter.next(), bytes("row1"), "cf", "firstname", M1_FNAME_VALUE);
        assertKeyValuePair(iter.next(), bytes("row2"), "___ROW___", "___ROW___", "row2");
        assertKeyValuePair(iter.next(), bytes("row2"), "cf", "age", encode(BIGINT, 28L));
        assertKeyValuePair(iter.next(), bytes("row2"), "cf", "arr", M2_ARR_VALUE);
        assertKeyValuePair(iter.next(), bytes("row2"), "cf", "firstname", "dave");
        assertKeyValuePair(iter.next(), bytes("row3"), "___ROW___", "___ROW___", "row3");
        assertKeyValuePair(iter.next(), bytes("row3"), "cf", "age", AGE_VALUE);
        assertKeyValuePair(iter.next(), bytes("row3"), "cf", "arr", M3_ARR_VALUE);
        assertKeyValuePair(iter.next(), bytes("row3"), "cf", "firstname", M3_FNAME_VALUE);
        assertFalse(iter.hasNext());

        scan = connector.createScanner(table.getIndexTableName(), new Authorizations("private", "moreprivate"));
        iter = scan.iterator();
        assertTrue(iter.hasNext());
        assertKeyValuePair(iter.next(), AGE_VALUE, "cf_age", "row1", "");
        assertKeyValuePair(iter.next(), AGE_VALUE, "cf_age", "row3", "moreprivate", "");
        assertKeyValuePair(iter.next(), UPDATED_AGE_VALUE, "cf_age", "row2", "", "");
        assertKeyValuePair(iter.next(), bytes("abc"), "cf_arr", "row1", "");
        assertKeyValuePair(iter.next(), bytes("abc"), "cf_arr", "row2", "private", "");
        assertKeyValuePair(iter.next(), M1_FNAME_VALUE, "cf_firstname", "row1", "");
        assertKeyValuePair(iter.next(), M3_FNAME_VALUE, "cf_firstname", "row3", "moreprivate", "");
        assertKeyValuePair(iter.next(), UPDATED_M2_FNAME_VALUE, "cf_firstname", "row2", "moreprivate", "");
        assertKeyValuePair(iter.next(), bytes("def"), "cf_arr", "row1", "");
        assertKeyValuePair(iter.next(), bytes("def"), "cf_arr", "row3", "moreprivate", "");
        assertKeyValuePair(iter.next(), bytes("ghi"), "cf_arr", "row1", "");
        assertKeyValuePair(iter.next(), bytes("ghi"), "cf_arr", "row2", "private", "");
        assertKeyValuePair(iter.next(), bytes("ghi"), "cf_arr", "row3", "moreprivate", "");
        assertKeyValuePair(iter.next(), bytes("jkl"), "cf_arr", "row3", "moreprivate", "");
        assertKeyValuePair(iter.next(), bytes("mno"), "cf_arr", "row2", "private", "");
        assertFalse(iter.hasNext());

        scan.close();

        assertEquals(metricsStorage.newReader().getCardinality(mck("cf", "age", AGE_VALUE, "private", "moreprivate")), 2);
        assertEquals(metricsStorage.newReader().getCardinality(mck("cf", "age", UPDATED_AGE_VALUE)), 1);
        assertEquals(metricsStorage.newReader().getNumRowsInTable(table.getSchema(), table.getTable()), 3);
        assertEquals(metricsStorage.newReader().getCardinality(mck("cf", "arr", "abc", "private", "moreprivate")), 2);
        assertEquals(metricsStorage.newReader().getCardinality(mck("cf", "firstname", M1_FNAME_VALUE, "private", "moreprivate")), 1);
        assertEquals(metricsStorage.newReader().getCardinality(mck("cf", "firstname", M2_FNAME_VALUE, "private", "moreprivate")), 0);
        assertEquals(metricsStorage.newReader().getCardinality(mck("cf", "firstname", M3_FNAME_VALUE, "private", "moreprivate")), 1);
        assertEquals(metricsStorage.newReader().getCardinality(mck("cf", "firstname", UPDATED_M2_FNAME_VALUE)), 0);
        assertEquals(metricsStorage.newReader().getCardinality(mck("cf", "firstname", UPDATED_M2_FNAME_VALUE, "private", "moreprivate")), 1);
        assertEquals(metricsStorage.newReader().getCardinality(mck("cf", "arr", "def", "private", "moreprivate")), 2);
        assertEquals(metricsStorage.newReader().getCardinality(mck("cf", "arr", "ghi", "private", "moreprivate")), 3);
        assertEquals(metricsStorage.newReader().getCardinality(mck("cf", "arr", "jkl", "private", "moreprivate")), 1);
        assertEquals(metricsStorage.newReader().getCardinality(mck("cf", "arr", "mno", "private", "moreprivate")), 1);

        prestoBatchWriter.updateColumnByName("row2", "arr", new ColumnVisibility("moreprivate"), ImmutableList.of("jkl", "mno", "pqr"));
        prestoBatchWriter.close();

        scan = connector.createScanner(table.getFullTableName(), new Authorizations("private", "moreprivate"));
        iter = scan.iterator();
        assertTrue(iter.hasNext());
        assertKeyValuePair(iter.next(), bytes("row1"), "___ROW___", "___ROW___", "row1");
        assertKeyValuePair(iter.next(), bytes("row1"), "cf", "age", AGE_VALUE);
        assertKeyValuePair(iter.next(), bytes("row1"), "cf", "arr", M1_ARR_VALUE);
        assertKeyValuePair(iter.next(), bytes("row1"), "cf", "firstname", M1_FNAME_VALUE);
        assertKeyValuePair(iter.next(), bytes("row2"), "___ROW___", "___ROW___", "row2");
        assertKeyValuePair(iter.next(), bytes("row2"), "cf", "age", encode(BIGINT, 28L));
        assertKeyValuePair(iter.next(), bytes("row2"), "cf", "arr", encode(new ArrayType(VARCHAR), getBlockFromArray(VARCHAR, ImmutableList.of("jkl", "mno", "pqr"))));
        assertKeyValuePair(iter.next(), bytes("row2"), "cf", "firstname", "dave");
        assertKeyValuePair(iter.next(), bytes("row3"), "___ROW___", "___ROW___", "row3");
        assertKeyValuePair(iter.next(), bytes("row3"), "cf", "age", AGE_VALUE);
        assertKeyValuePair(iter.next(), bytes("row3"), "cf", "arr", M3_ARR_VALUE);
        assertKeyValuePair(iter.next(), bytes("row3"), "cf", "firstname", M3_FNAME_VALUE);
        assertFalse(iter.hasNext());

        scan = connector.createScanner(table.getIndexTableName(), new Authorizations("private", "moreprivate"));
        iter = scan.iterator();
        assertTrue(iter.hasNext());
        assertKeyValuePair(iter.next(), AGE_VALUE, "cf_age", "row1", "");
        assertKeyValuePair(iter.next(), AGE_VALUE, "cf_age", "row3", "moreprivate", "");
        assertKeyValuePair(iter.next(), UPDATED_AGE_VALUE, "cf_age", "row2", "", "");
        assertKeyValuePair(iter.next(), bytes("abc"), "cf_arr", "row1", "");
        assertKeyValuePair(iter.next(), M1_FNAME_VALUE, "cf_firstname", "row1", "");
        assertKeyValuePair(iter.next(), M3_FNAME_VALUE, "cf_firstname", "row3", "moreprivate", "");
        assertKeyValuePair(iter.next(), UPDATED_M2_FNAME_VALUE, "cf_firstname", "row2", "moreprivate", "");
        assertKeyValuePair(iter.next(), bytes("def"), "cf_arr", "row1", "");
        assertKeyValuePair(iter.next(), bytes("def"), "cf_arr", "row3", "moreprivate", "");
        assertKeyValuePair(iter.next(), bytes("ghi"), "cf_arr", "row1", "");
        assertKeyValuePair(iter.next(), bytes("ghi"), "cf_arr", "row3", "moreprivate", "");
        assertKeyValuePair(iter.next(), bytes("jkl"), "cf_arr", "row2", "moreprivate", "");
        assertKeyValuePair(iter.next(), bytes("jkl"), "cf_arr", "row3", "moreprivate", "");
        assertKeyValuePair(iter.next(), bytes("mno"), "cf_arr", "row2", "moreprivate", "");
        assertKeyValuePair(iter.next(), bytes("pqr"), "cf_arr", "row2", "moreprivate", "");
        assertFalse(iter.hasNext());

        scan.close();

        assertEquals(metricsStorage.newReader().getCardinality(mck("cf", "age", AGE_VALUE, "private", "moreprivate")), 2);
        assertEquals(metricsStorage.newReader().getCardinality(mck("cf", "age", UPDATED_AGE_VALUE)), 1);
        assertEquals(metricsStorage.newReader().getNumRowsInTable(table.getSchema(), table.getTable()), 3);
        assertEquals(metricsStorage.newReader().getCardinality(mck("cf", "arr", "abc", "private", "moreprivate")), 1);
        assertEquals(metricsStorage.newReader().getCardinality(mck("cf", "firstname", M1_FNAME_VALUE, "private", "moreprivate")), 1);
        assertEquals(metricsStorage.newReader().getCardinality(mck("cf", "firstname", M2_FNAME_VALUE, "private", "moreprivate")), 0);
        assertEquals(metricsStorage.newReader().getCardinality(mck("cf", "firstname", M3_FNAME_VALUE, "private", "moreprivate")), 1);
        assertEquals(metricsStorage.newReader().getCardinality(mck("cf", "firstname", UPDATED_M2_FNAME_VALUE)), 0);
        assertEquals(metricsStorage.newReader().getCardinality(mck("cf", "firstname", UPDATED_M2_FNAME_VALUE, "private", "moreprivate")), 1);
        assertEquals(metricsStorage.newReader().getCardinality(mck("cf", "arr", "def", "private", "moreprivate")), 2);
        assertEquals(metricsStorage.newReader().getCardinality(mck("cf", "arr", "ghi", "private", "moreprivate")), 2);
        assertEquals(metricsStorage.newReader().getCardinality(mck("cf", "arr", "jkl", "private", "moreprivate")), 2);
        assertEquals(metricsStorage.newReader().getCardinality(mck("cf", "arr", "mno", "private", "moreprivate")), 1);
        assertEquals(metricsStorage.newReader().getCardinality(mck("cf", "arr", "mno", "private")), 0);
        assertEquals(metricsStorage.newReader().getCardinality(mck("cf", "arr", "mno", "private", "moreprivate")), 1);
    }

    @Test
    public void testUpdateColumns()
            throws Exception
    {
        testAddMutations();

        PrestoBatchWriter prestoBatchWriter = new PrestoBatchWriter(connector, connector.securityOperations().getUserAuthorizations("root"), table);
        prestoBatchWriter.updateColumnByName("row2", ImmutableMap.of(Pair.of("age", new ColumnVisibility("moreprivate")), 28L, Pair.of("firstname", new ColumnVisibility("moreprivate")), "dave", Pair.of("arr", new ColumnVisibility("moreprivate")), ImmutableList.of("jkl", "mno", "pqr")));
        prestoBatchWriter.close();

        Scanner scan = connector.createScanner(table.getFullTableName(), new Authorizations("private", "moreprivate"));
        Iterator<Entry<Key, Value>> iter = scan.iterator();
        assertTrue(iter.hasNext());
        assertKeyValuePair(iter.next(), bytes("row1"), "___ROW___", "___ROW___", "row1");
        assertKeyValuePair(iter.next(), bytes("row1"), "cf", "age", AGE_VALUE);
        assertKeyValuePair(iter.next(), bytes("row1"), "cf", "arr", M1_ARR_VALUE);
        assertKeyValuePair(iter.next(), bytes("row1"), "cf", "firstname", M1_FNAME_VALUE);
        assertKeyValuePair(iter.next(), bytes("row2"), "___ROW___", "___ROW___", "row2");
        assertKeyValuePair(iter.next(), bytes("row2"), "cf", "age", encode(BIGINT, 28L));
        assertKeyValuePair(iter.next(), bytes("row2"), "cf", "arr", encode(new ArrayType(VARCHAR), getBlockFromArray(VARCHAR, ImmutableList.of("jkl", "mno", "pqr"))));
        assertKeyValuePair(iter.next(), bytes("row2"), "cf", "firstname", "dave");
        assertKeyValuePair(iter.next(), bytes("row3"), "___ROW___", "___ROW___", "row3");
        assertKeyValuePair(iter.next(), bytes("row3"), "cf", "age", AGE_VALUE);
        assertKeyValuePair(iter.next(), bytes("row3"), "cf", "arr", M3_ARR_VALUE);
        assertKeyValuePair(iter.next(), bytes("row3"), "cf", "firstname", M3_FNAME_VALUE);
        assertFalse(iter.hasNext());

        scan = connector.createScanner(table.getIndexTableName(), new Authorizations("private", "moreprivate"));
        iter = scan.iterator();
        assertTrue(iter.hasNext());
        assertKeyValuePair(iter.next(), AGE_VALUE, "cf_age", "row1", "");
        assertKeyValuePair(iter.next(), AGE_VALUE, "cf_age", "row3", "moreprivate", "");
        assertKeyValuePair(iter.next(), UPDATED_AGE_VALUE, "cf_age", "row2", "moreprivate", "");
        assertKeyValuePair(iter.next(), bytes("abc"), "cf_arr", "row1", "");
        assertKeyValuePair(iter.next(), M1_FNAME_VALUE, "cf_firstname", "row1", "");
        assertKeyValuePair(iter.next(), M3_FNAME_VALUE, "cf_firstname", "row3", "moreprivate", "");
        assertKeyValuePair(iter.next(), UPDATED_M2_FNAME_VALUE, "cf_firstname", "row2", "moreprivate", "");
        assertKeyValuePair(iter.next(), bytes("def"), "cf_arr", "row1", "");
        assertKeyValuePair(iter.next(), bytes("def"), "cf_arr", "row3", "moreprivate", "");
        assertKeyValuePair(iter.next(), bytes("ghi"), "cf_arr", "row1", "");
        assertKeyValuePair(iter.next(), bytes("ghi"), "cf_arr", "row3", "moreprivate", "");
        assertKeyValuePair(iter.next(), bytes("jkl"), "cf_arr", "row2", "moreprivate", "");
        assertKeyValuePair(iter.next(), bytes("jkl"), "cf_arr", "row3", "moreprivate", "");
        assertKeyValuePair(iter.next(), bytes("mno"), "cf_arr", "row2", "moreprivate", "");
        assertKeyValuePair(iter.next(), bytes("pqr"), "cf_arr", "row2", "moreprivate", "");
        assertFalse(iter.hasNext());

        scan.close();

        assertEquals(metricsStorage.newReader().getCardinality(mck("cf", "age", AGE_VALUE, "private", "moreprivate")), 2);
        assertEquals(metricsStorage.newReader().getNumRowsInTable(table.getSchema(), table.getTable()), 3);
        assertEquals(metricsStorage.newReader().getCardinality(mck("cf", "arr", "abc", "private", "moreprivate")), 1);
        assertEquals(metricsStorage.newReader().getCardinality(mck("cf", "firstname", M1_FNAME_VALUE, "private", "moreprivate")), 1);
        assertEquals(metricsStorage.newReader().getCardinality(mck("cf", "firstname", M2_FNAME_VALUE, "private", "moreprivate")), 0);
        assertEquals(metricsStorage.newReader().getCardinality(mck("cf", "firstname", M3_FNAME_VALUE, "private", "moreprivate")), 1);
        assertEquals(metricsStorage.newReader().getCardinality(mck("cf", "firstname", UPDATED_M2_FNAME_VALUE, "private", "moreprivate")), 1);
        assertEquals(metricsStorage.newReader().getCardinality(mck("cf", "arr", "def", "private", "moreprivate")), 2);
        assertEquals(metricsStorage.newReader().getCardinality(mck("cf", "arr", "ghi", "private", "moreprivate")), 2);
        assertEquals(metricsStorage.newReader().getCardinality(mck("cf", "arr", "jkl", "private", "moreprivate")), 2);
        assertEquals(metricsStorage.newReader().getCardinality(mck("cf", "arr", "mno", "private", "moreprivate")), 1);
        assertEquals(metricsStorage.newReader().getCardinality(mck("cf", "arr", "mno", "private", "moreprivate")), 1);
    }

    @Test
    public void testDeleteMutation()
            throws Exception
    {
        // Populate the table with data
        testAddMutation();

        PrestoBatchWriter prestoBatchWriter = new PrestoBatchWriter(connector, connector.securityOperations().getUserAuthorizations("root"), table);
        prestoBatchWriter.deleteRow("row1");
        prestoBatchWriter.flush();

        Scanner scan = connector.createScanner(table.getFullTableName(), new Authorizations("private", "moreprivate"));
        Iterator<Entry<Key, Value>> iter = scan.iterator();
        assertTrue(iter.hasNext());
        assertKeyValuePair(iter.next(), bytes("row2"), "___ROW___", "___ROW___", "row2");
        assertKeyValuePair(iter.next(), bytes("row2"), "cf", "age", AGE_VALUE);
        assertKeyValuePair(iter.next(), bytes("row2"), "cf", "arr", M2_ARR_VALUE);
        assertKeyValuePair(iter.next(), bytes("row2"), "cf", "firstname", M2_FNAME_VALUE);
        assertKeyValuePair(iter.next(), bytes("row3"), "___ROW___", "___ROW___", "row3");
        assertKeyValuePair(iter.next(), bytes("row3"), "cf", "age", AGE_VALUE);
        assertKeyValuePair(iter.next(), bytes("row3"), "cf", "arr", M3_ARR_VALUE);
        assertKeyValuePair(iter.next(), bytes("row3"), "cf", "firstname", M3_FNAME_VALUE);
        assertFalse(iter.hasNext());

        scan = connector.createScanner(table.getIndexTableName(), new Authorizations("private", "moreprivate"));
        iter = scan.iterator();
        assertTrue(iter.hasNext());
        assertKeyValuePair(iter.next(), AGE_VALUE, "cf_age", "row2", "private", "");
        assertKeyValuePair(iter.next(), AGE_VALUE, "cf_age", "row3", "moreprivate", "");
        assertKeyValuePair(iter.next(), bytes("abc"), "cf_arr", "row2", "private", "");
        assertKeyValuePair(iter.next(), M2_FNAME_VALUE, "cf_firstname", "row2", "private", "");
        assertKeyValuePair(iter.next(), M3_FNAME_VALUE, "cf_firstname", "row3", "moreprivate", "");
        assertKeyValuePair(iter.next(), bytes("def"), "cf_arr", "row3", "moreprivate", "");
        assertKeyValuePair(iter.next(), bytes("ghi"), "cf_arr", "row2", "private", "");
        assertKeyValuePair(iter.next(), bytes("ghi"), "cf_arr", "row3", "moreprivate", "");
        assertKeyValuePair(iter.next(), bytes("jkl"), "cf_arr", "row3", "moreprivate", "");
        assertKeyValuePair(iter.next(), bytes("mno"), "cf_arr", "row2", "private", "");
        assertFalse(iter.hasNext());

        scan.close();

        assertEquals(metricsStorage.newReader().getCardinality(mck("cf", "age", AGE_VALUE, "private", "moreprivate")), 2);
        assertEquals(metricsStorage.newReader().getNumRowsInTable(table.getSchema(), table.getTable()), 2);
        assertEquals(metricsStorage.newReader().getCardinality(mck("cf", "arr", "abc", "private", "moreprivate")), 1);
        assertEquals(metricsStorage.newReader().getCardinality(mck("cf", "firstname", M1_FNAME_VALUE, "private", "moreprivate")), 0);
        assertEquals(metricsStorage.newReader().getCardinality(mck("cf", "firstname", M2_FNAME_VALUE, "private", "moreprivate")), 1);
        assertEquals(metricsStorage.newReader().getCardinality(mck("cf", "firstname", M3_FNAME_VALUE, "private", "moreprivate")), 1);
        assertEquals(metricsStorage.newReader().getCardinality(mck("cf", "arr", "def", "private", "moreprivate")), 1);
        assertEquals(metricsStorage.newReader().getCardinality(mck("cf", "arr", "ghi", "private", "moreprivate")), 2);
        assertEquals(metricsStorage.newReader().getCardinality(mck("cf", "arr", "jkl", "private", "moreprivate")), 1);
        assertEquals(metricsStorage.newReader().getCardinality(mck("cf", "arr", "mno", "private", "moreprivate")), 1);

        prestoBatchWriter.deleteRow("row2");
        prestoBatchWriter.flush();

        scan = connector.createScanner(table.getFullTableName(), new Authorizations("private", "moreprivate"));
        iter = scan.iterator();
        assertTrue(iter.hasNext());
        assertKeyValuePair(iter.next(), bytes("row3"), "___ROW___", "___ROW___", "row3");
        assertKeyValuePair(iter.next(), bytes("row3"), "cf", "age", AGE_VALUE);
        assertKeyValuePair(iter.next(), bytes("row3"), "cf", "arr", M3_ARR_VALUE);
        assertKeyValuePair(iter.next(), bytes("row3"), "cf", "firstname", M3_FNAME_VALUE);
        assertFalse(iter.hasNext());

        scan = connector.createScanner(table.getIndexTableName(), new Authorizations("private", "moreprivate"));
        iter = scan.iterator();
        assertTrue(iter.hasNext());
        assertKeyValuePair(iter.next(), AGE_VALUE, "cf_age", "row3", "moreprivate", "");
        assertKeyValuePair(iter.next(), M3_FNAME_VALUE, "cf_firstname", "row3", "moreprivate", "");
        assertKeyValuePair(iter.next(), bytes("def"), "cf_arr", "row3", "moreprivate", "");
        assertKeyValuePair(iter.next(), bytes("ghi"), "cf_arr", "row3", "moreprivate", "");
        assertKeyValuePair(iter.next(), bytes("jkl"), "cf_arr", "row3", "moreprivate", "");
        assertFalse(iter.hasNext());
        scan.close();

        assertEquals(metricsStorage.newReader().getCardinality(mck("cf", "age", AGE_VALUE, "private", "moreprivate")), 1);
        assertEquals(metricsStorage.newReader().getNumRowsInTable(table.getSchema(), table.getTable()), 1);
        assertEquals(metricsStorage.newReader().getCardinality(mck("cf", "arr", "abc", "private", "moreprivate")), 0);
        assertEquals(metricsStorage.newReader().getCardinality(mck("cf", "firstname", M1_FNAME_VALUE, "private", "moreprivate")), 0);
        assertEquals(metricsStorage.newReader().getCardinality(mck("cf", "firstname", M2_FNAME_VALUE, "private", "moreprivate")), 0);
        assertEquals(metricsStorage.newReader().getCardinality(mck("cf", "firstname", M3_FNAME_VALUE, "private", "moreprivate")), 1);
        assertEquals(metricsStorage.newReader().getCardinality(mck("cf", "arr", "def", "private", "moreprivate")), 1);
        assertEquals(metricsStorage.newReader().getCardinality(mck("cf", "arr", "ghi", "private", "moreprivate")), 1);
        assertEquals(metricsStorage.newReader().getCardinality(mck("cf", "arr", "jkl", "private", "moreprivate")), 1);
        assertEquals(metricsStorage.newReader().getCardinality(mck("cf", "arr", "mno", "private", "moreprivate")), 0);

        prestoBatchWriter.deleteRow("row3");
        prestoBatchWriter.close();

        scan = connector.createScanner(table.getFullTableName(), new Authorizations("private", "moreprivate"));
        iter = scan.iterator();
        assertFalse(iter.hasNext());

        scan = connector.createScanner(table.getIndexTableName(), new Authorizations("private", "moreprivate"));
        iter = scan.iterator();
        assertFalse(iter.hasNext());
        scan.close();

        assertEquals(metricsStorage.newReader().getCardinality(mck("cf", "age", AGE_VALUE, "private", "moreprivate")), 0);
        assertEquals(metricsStorage.newReader().getNumRowsInTable(table.getSchema(), table.getTable()), 0);
        assertEquals(metricsStorage.newReader().getCardinality(mck("cf", "arr", "abc", "private", "moreprivate")), 0);
        assertEquals(metricsStorage.newReader().getCardinality(mck("cf", "firstname", M1_FNAME_VALUE, "private", "moreprivate")), 0);
        assertEquals(metricsStorage.newReader().getCardinality(mck("cf", "firstname", M2_FNAME_VALUE, "private", "moreprivate")), 0);
        assertEquals(metricsStorage.newReader().getCardinality(mck("cf", "firstname", M3_FNAME_VALUE, "private", "moreprivate")), 0);
        assertEquals(metricsStorage.newReader().getCardinality(mck("cf", "arr", "def", "private", "moreprivate")), 0);
        assertEquals(metricsStorage.newReader().getCardinality(mck("cf", "arr", "ghi", "private", "moreprivate")), 0);
        assertEquals(metricsStorage.newReader().getCardinality(mck("cf", "arr", "jkl", "private", "moreprivate")), 0);
        assertEquals(metricsStorage.newReader().getCardinality(mck("cf", "arr", "mno", "private", "moreprivate")), 0);
    }

    @Test
    public void testDeleteMutations()
            throws Exception
    {
        testAddMutations();

        PrestoBatchWriter prestoBatchWriter = new PrestoBatchWriter(connector, connector.securityOperations().getUserAuthorizations("root"), table);
        prestoBatchWriter.deleteRows(ImmutableList.of("row1", "row2", "row3"));
        prestoBatchWriter.close();

        Scanner scan = connector.createScanner(table.getFullTableName(), new Authorizations("private", "moreprivate"));
        Iterator<Entry<Key, Value>> iter = scan.iterator();
        assertFalse(iter.hasNext());

        scan = connector.createScanner(table.getIndexTableName(), new Authorizations("private", "moreprivate"));
        iter = scan.iterator();
        assertFalse(iter.hasNext());
        scan.close();

        assertEquals(metricsStorage.newReader().getCardinality(mck("cf", "age", AGE_VALUE, "private", "moreprivate")), 0);
        assertEquals(metricsStorage.newReader().getNumRowsInTable(table.getSchema(), table.getTable()), 0);
        assertEquals(metricsStorage.newReader().getCardinality(mck("cf", "arr", "abc", "private", "moreprivate")), 0);
        assertEquals(metricsStorage.newReader().getCardinality(mck("cf", "firstname", M1_FNAME_VALUE, "private", "moreprivate")), 0);
        assertEquals(metricsStorage.newReader().getCardinality(mck("cf", "firstname", M2_FNAME_VALUE, "private", "moreprivate")), 0);
        assertEquals(metricsStorage.newReader().getCardinality(mck("cf", "firstname", M3_FNAME_VALUE, "private", "moreprivate")), 0);
        assertEquals(metricsStorage.newReader().getCardinality(mck("cf", "arr", "def", "private", "moreprivate")), 0);
        assertEquals(metricsStorage.newReader().getCardinality(mck("cf", "arr", "ghi", "private", "moreprivate")), 0);
        assertEquals(metricsStorage.newReader().getCardinality(mck("cf", "arr", "jkl", "private", "moreprivate")), 0);
        assertEquals(metricsStorage.newReader().getCardinality(mck("cf", "arr", "mno", "private", "moreprivate")), 0);
    }

    private static byte[] encode(Type type, Object v)
    {
        return SERIALIZER.encode(type, v);
    }

    private MetricCacheKey mck(String family, String qualifier, String range, String... auths)
    {
        return mck(family, qualifier, range.getBytes(UTF_8), auths);
    }

    private MetricCacheKey mck(String family, String qualifier, byte[] range, String... auths)
    {
        return new MetricCacheKey(metricsStorage, table.getSchema(), table.getTable(), family, qualifier, false, new Authorizations(auths), new Range(new Text(range)));
    }

    private void assertKeyValuePair(Entry<Key, Value> e, byte[] row, String cf, String cq, String value)
    {
        assertEquals(e.getKey().getRow().copyBytes(), row);
        assertEquals(e.getKey().getColumnFamily().toString(), cf);
        assertEquals(e.getKey().getColumnQualifier().toString(), cq);
        assertEquals(e.getValue().toString(), value);
    }

    private void assertKeyValuePair(Entry<Key, Value> e, byte[] row, String cf, String cq, byte[] value)
    {
        assertEquals(e.getKey().getRow().copyBytes(), row);
        assertEquals(e.getKey().getColumnFamily().toString(), cf);
        assertEquals(e.getKey().getColumnQualifier().toString(), cq);
        assertEquals(e.getValue().get(), value);
    }

    private void assertKeyValuePair(Entry<Key, Value> e, byte[] row, String cf, String cq, String cv, String value)
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
