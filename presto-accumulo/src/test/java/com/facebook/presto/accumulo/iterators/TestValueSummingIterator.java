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
package com.facebook.presto.accumulo.iterators;

import com.facebook.presto.accumulo.AccumuloQueryRunner;
import com.google.common.collect.ImmutableList;
import io.airlift.log.Logger;
import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.LongCombiner.FixedLenEncoder;
import org.apache.accumulo.core.iterators.LongCombiner.StringEncoder;
import org.apache.accumulo.core.iterators.LongCombiner.Type;
import org.apache.accumulo.core.iterators.LongCombiner.VarLenEncoder;
import org.apache.accumulo.core.iterators.TypedValueCombiner.Encoder;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.hadoop.io.Text;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.Map;

import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.testng.Assert.assertEquals;

@Test(singleThreaded = true)
public class TestValueSummingIterator
{
    private static final String TABLE_NAME = "testvaluesummingiterator";
    public static final Encoder<Long> FIXED_LEN_ENCODER = new FixedLenEncoder();
    public static final Encoder<Long> VAR_LEN_ENCODER = new VarLenEncoder();
    public static final Encoder<Long> STRING_ENCODER = new StringEncoder();

    private Connector connector = null;

    @BeforeClass
    public void setup()
            throws Exception
    {
        connector = AccumuloQueryRunner.getAccumuloConnector();
    }

    @AfterMethod
    public void cleanup()
            throws Exception
    {
        if (connector.tableOperations().exists(TABLE_NAME)) {
            connector.tableOperations().delete(TABLE_NAME);
        }
    }

    @Test
    public void testNoData()
            throws Exception
    {
        connector.tableOperations().create(TABLE_NAME);

        Scanner scanner = connector.createScanner(TABLE_NAME, new Authorizations());
        IteratorSetting setting = new IteratorSetting(Integer.MAX_VALUE, ValueSummingIterator.class);
        long expectedSum = 0;

        ValueSummingIterator.setEncodingType(setting, Type.STRING);
        scanner.addScanIterator(setting);

        long sum = 0;
        for (Map.Entry<Key, Value> entry : scanner) {
            System.out.println(entry);
            sum += STRING_ENCODER.decode(entry.getValue().get());
        }

        assertEquals(sum, expectedSum);
    }

    @Test
    public void testNoDataInColumn()
            throws Exception
    {
        connector.tableOperations().create(TABLE_NAME);
        BatchWriter writer = connector.createBatchWriter(TABLE_NAME, new BatchWriterConfig());

        Mutation m1 = new Mutation("foo1");
        m1.put(b("cf2"), b("cq1"), STRING_ENCODER.encode(1L));
        m1.put(b("cf2"), b("cq2"), STRING_ENCODER.encode(1L));
        writer.addMutation(m1);

        writer.close();

        Scanner scanner = connector.createScanner(TABLE_NAME, new Authorizations());
        scanner.fetchColumnFamily(t("cf1"));
        IteratorSetting setting = new IteratorSetting(Integer.MAX_VALUE, ValueSummingIterator.class);
        long expectedSum = 0;

        ValueSummingIterator.setEncodingType(setting, Type.STRING);
        scanner.addScanIterator(setting);

        long sum = 0;
        for (Map.Entry<Key, Value> entry : scanner) {
            System.out.println(entry);
            sum += STRING_ENCODER.decode(entry.getValue().get());
        }

        assertEquals(sum, expectedSum);
    }

    @Test
    public void testSumAllFixedLenEncoder()
            throws Exception
    {
        testSum(FIXED_LEN_ENCODER, Type.FIXEDLEN);
    }

    @Test
    public void testSumBatchAllFixedLenEncoder()
            throws Exception
    {
        testSumBatch(FIXED_LEN_ENCODER, Type.FIXEDLEN);
    }

    @Test
    public void testSumAllStringEncoder()
            throws Exception
    {
        testSum(STRING_ENCODER, Type.STRING);
    }

    @Test
    public void testSumBatchllStringEncoder()
            throws Exception
    {
        testSumBatch(STRING_ENCODER, Type.STRING);
    }

    @Test
    public void testSumAllVarLenEncoder()
            throws Exception
    {
        testSum(VAR_LEN_ENCODER, Type.VARLEN);
    }

    @Test
    public void testSumBatchAllVarLenEncoder()
            throws Exception
    {
        testSumBatch(VAR_LEN_ENCODER, Type.VARLEN);
    }

    @Test
    public void testSumFamilyFixedLenEncoder()
            throws Exception
    {
        testSum(FIXED_LEN_ENCODER, Type.FIXEDLEN, true);
    }

    @Test
    public void testSumBatchFamilyFixedLenEncoder()
            throws Exception
    {
        testSumBatch(FIXED_LEN_ENCODER, Type.FIXEDLEN, true);
    }

    @Test
    public void testSumFamilyStringEncoder()
            throws Exception
    {
        testSum(STRING_ENCODER, Type.STRING, true);
    }

    @Test
    public void testSumBatchFamilyStringEncoder()
            throws Exception
    {
        testSumBatch(STRING_ENCODER, Type.STRING, true);
    }

    @Test
    public void testSumFamilyVarLenEncoder()
            throws Exception
    {
        testSum(VAR_LEN_ENCODER, Type.VARLEN, true);
    }

    @Test
    public void testSumBatchFamilyVarLenEncoder()
            throws Exception
    {
        testSumBatch(VAR_LEN_ENCODER, Type.VARLEN, true);
    }

    @Test
    public void testSumColumnFixedLenEncoder()
            throws Exception
    {
        testSum(FIXED_LEN_ENCODER, Type.FIXEDLEN, true, true);
    }

    @Test
    public void testSumBatchColumnFixedLenEncoder()
            throws Exception
    {
        testSumBatch(FIXED_LEN_ENCODER, Type.FIXEDLEN, true, true);
    }

    @Test
    public void testSumColumnStringEncoder()
            throws Exception
    {
        testSum(STRING_ENCODER, Type.STRING, true, true);
    }

    @Test
    public void testSumBatchColumnStringEncoder()
            throws Exception
    {
        testSumBatch(STRING_ENCODER, Type.STRING, true, true);
    }

    @Test
    public void testSumColumnVarLenEncoder()
            throws Exception
    {
        testSum(VAR_LEN_ENCODER, Type.VARLEN, true, true);
    }

    @Test
    public void testSumBatchColumnVarLenEncoder()
            throws Exception
    {
        testSumBatch(VAR_LEN_ENCODER, Type.VARLEN, true, true);
    }

    private void testSum(Encoder<Long> encoder, Type type)
            throws Exception
    {
        testSum(encoder, type, false, false);
    }

    private void testSum(Encoder<Long> encoder, Type type, boolean fetchColumn)
            throws Exception
    {
        testSum(encoder, type, fetchColumn, false);
    }

    private void testSum(Encoder<Long> encoder, Type type, boolean fetchColumn, boolean fetchQualifier)
            throws Exception
    {
        connector.tableOperations().create(TABLE_NAME);
        BatchWriter writer = connector.createBatchWriter(TABLE_NAME, new BatchWriterConfig());

        Mutation m1 = new Mutation("foo1");
        m1.put(b("cf1"), b("cq1"), encoder.encode(1L));
        m1.put(b("cf1"), b("cq2"), encoder.encode(1L));
        m1.put(b("cf2"), b("cq1"), encoder.encode(1L));
        m1.put(b("cf2"), b("cq2"), encoder.encode(1L));
        writer.addMutation(m1);

        Mutation m2 = new Mutation("foo2");
        m2.put(b("cf1"), b("cq1"), encoder.encode(2L));
        m2.put(b("cf1"), b("cq2"), encoder.encode(2L));
        m2.put(b("cf2"), b("cq1"), encoder.encode(2L));
        m2.put(b("cf2"), b("cq2"), encoder.encode(2L));
        writer.addMutation(m2);

        Mutation m3 = new Mutation("foo3");
        m3.put(b("cf1"), b("cq1"), encoder.encode(3L));
        m3.put(b("cf1"), b("cq2"), encoder.encode(3L));
        m3.put(b("cf2"), b("cq1"), encoder.encode(3L));
        m3.put(b("cf2"), b("cq2"), encoder.encode(3L));
        writer.addMutation(m3);

        writer.close();

        Scanner scanner = connector.createScanner(TABLE_NAME, new Authorizations());
        IteratorSetting setting = new IteratorSetting(Integer.MAX_VALUE, ValueSummingIterator.class);
        long expectedSum;
        if (fetchColumn) {
            if (fetchQualifier) {
                scanner.fetchColumn(t("cf1"), t("cq1"));
                expectedSum = 6;
            }
            else {
                scanner.fetchColumnFamily(t("cf1"));
                expectedSum = 12;
            }
        }
        else {
            expectedSum = 24;
        }

        ValueSummingIterator.setEncodingType(setting, type);
        scanner.addScanIterator(setting);

        long sum = 0;
        for (Map.Entry<Key, Value> entry : scanner) {
            System.out.println(entry);
            sum += encoder.decode(entry.getValue().get());
        }

        assertEquals(sum, expectedSum);
    }

    private void testSumBatch(Encoder<Long> encoder, Type type)
            throws Exception
    {
        testSumBatch(encoder, type, false, false);
    }

    private void testSumBatch(Encoder<Long> encoder, Type type, boolean fetchColumn)
            throws Exception
    {
        testSumBatch(encoder, type, fetchColumn, false);
    }

    private void testSumBatch(Encoder<Long> encoder, Type type, boolean fetchColumn, boolean fetchQualifier)
            throws Exception
    {
        connector.tableOperations().create(TABLE_NAME);
        BatchWriter writer = connector.createBatchWriter(TABLE_NAME, new BatchWriterConfig());

        Mutation m1 = new Mutation("foo1");
        m1.put(b("cf1"), b("cq1"), encoder.encode(1L));
        m1.put(b("cf1"), b("cq2"), encoder.encode(1L));
        m1.put(b("cf2"), b("cq1"), encoder.encode(1L));
        m1.put(b("cf2"), b("cq2"), encoder.encode(1L));
        writer.addMutation(m1);

        Mutation m2 = new Mutation("foo2");
        m2.put(b("cf1"), b("cq1"), encoder.encode(2L));
        m2.put(b("cf1"), b("cq2"), encoder.encode(2L));
        m2.put(b("cf2"), b("cq1"), encoder.encode(2L));
        m2.put(b("cf2"), b("cq2"), encoder.encode(2L));
        writer.addMutation(m2);

        Mutation m3 = new Mutation("foo3");
        m3.put(b("cf1"), b("cq1"), encoder.encode(3L));
        m3.put(b("cf1"), b("cq2"), encoder.encode(3L));
        m3.put(b("cf2"), b("cq1"), encoder.encode(3L));
        m3.put(b("cf2"), b("cq2"), encoder.encode(3L));
        writer.addMutation(m3);

        writer.close();

        BatchScanner scanner = connector.createBatchScanner(TABLE_NAME, new Authorizations(), 10);
        IteratorSetting setting = new IteratorSetting(Integer.MAX_VALUE, ValueSummingIterator.class);
        scanner.setRanges(ImmutableList.of(new Range("foo0", "foo1"), new Range("foo2"), new Range("foo3"), new Range("foo4")));

        long expectedSum;
        if (fetchColumn) {
            if (fetchQualifier) {
                scanner.fetchColumn(t("cf1"), t("cq1"));
                expectedSum = 6;
            }
            else {
                scanner.fetchColumnFamily(t("cf1"));
                expectedSum = 12;
            }
        }
        else {
            expectedSum = 24;
        }

        ValueSummingIterator.setEncodingType(setting, type);
        scanner.addScanIterator(setting);

        long sum = 0;
        for (Map.Entry<Key, Value> entry : scanner) {
            System.out.println(entry);
            sum += encoder.decode(entry.getValue().get());
        }
        Logger.get(getClass()).info(format("%s %s", sum, expectedSum));
        assertEquals(sum, expectedSum);
    }

    @Test(expectedExceptions = RuntimeException.class)
    public void testTypeNotSet()
            throws Exception
    {
        connector.tableOperations().create(TABLE_NAME);
        Scanner scanner = connector.createScanner(TABLE_NAME, new Authorizations());
        IteratorSetting setting = new IteratorSetting(Integer.MAX_VALUE, ValueSummingIterator.class);
        scanner.addScanIterator(setting);
        scanner.iterator().next();
    }

    private static byte[] b(String s)
    {
        return s.getBytes(UTF_8);
    }

    private static Text t(String s)
    {
        return new Text(b(s));
    }
}
