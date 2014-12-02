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
package com.facebook.presto.raptor.storage;

import com.facebook.presto.spi.type.BigintType;
import com.facebook.presto.spi.type.BooleanType;
import com.facebook.presto.spi.type.DoubleType;
import com.facebook.presto.spi.type.HyperLogLogType;
import com.facebook.presto.spi.type.TimestampType;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.VarbinaryType;
import com.facebook.presto.spi.type.VarcharType;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.airlift.stats.cardinality.HyperLogLog;
import org.testng.annotations.Test;

import java.util.Collections;
import java.util.List;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

@Test(singleThreaded = true)
public class TestTupleBuffer
{
    @Test
    public void testTypes()
            throws Exception
    {
        List<Type> types = ImmutableList.<Type>builder()
                .add(BigintType.BIGINT)
                .add(DoubleType.DOUBLE)
                .add(BooleanType.BOOLEAN)
                .add(VarcharType.VARCHAR)
                .add(VarbinaryType.VARBINARY)
                .add(TimestampType.TIMESTAMP)
                .add(HyperLogLogType.HYPER_LOG_LOG)
                .build();

        HyperLogLog hyperLogLog = HyperLogLog.newInstance(16);
        hyperLogLog.add(1);
        hyperLogLog.add(2);

        Slice varCharSlice = Slices.wrappedBuffer("test".getBytes(UTF_8));
        Slice varBinarySlice = Slices.wrappedBuffer(new byte[] { 1, 2, 3 });
        Slice hllSlice = hyperLogLog.serialize();

        TupleBuffer tupleBuffer = new TupleBuffer(types, -1);
        for (int row = 0; row < 10; row++) {
            TupleBuffer tuple = tupleBuffer
                    .appendLong(1)
                    .appendDouble(1.1)
                    .appendBoolean(true)
                    .appendSlice(varCharSlice)
                    .appendSlice(varBinarySlice)
                    .appendLong(123)
                    .appendSlice(hllSlice);

            assertEquals(tuple.getTypes(), types);
            assertEquals(tuple.getFieldCount(), types.size());

            for (int i = 0; i < tuple.getFieldCount(); i++) {
                assertFalse(tuple.isNull(i));
                assertEquals(tuple.getType(i), types.get(i));
            }

            assertEquals(tuple.getLong(0), 1);
            assertEquals(tuple.getDouble(1), 1.1);
            assertEquals(tuple.getBoolean(2), true);
            assertEquals(tuple.getSlice(3), varCharSlice);
            assertEquals(tuple.getSlice(4), varBinarySlice);
            assertEquals(tuple.getLong(5), 123);
            assertEquals(tuple.getSlice(6), hllSlice);
            tupleBuffer.reset();
        }
    }

    @Test
    public void testZeroFieldTuple()
            throws Exception
    {
        List<Type> types = ImmutableList.of();
        TupleBuffer tuple = new TupleBuffer(types, -1);

        assertEquals(tuple.getTypes(), types);
        assertEquals(tuple.getFieldCount(), types.size());
    }

    @Test
    public void testAllNullFields()
            throws Exception
    {
        List<Type> types = ImmutableList.<Type>builder()
                .add(BigintType.BIGINT)
                .add(DoubleType.DOUBLE)
                .add(BooleanType.BOOLEAN)
                .add(VarcharType.VARCHAR)
                .add(VarbinaryType.VARBINARY)
                .add(TimestampType.TIMESTAMP)
                .add(HyperLogLogType.HYPER_LOG_LOG)
                .build();

        TupleBuffer tuple = new TupleBuffer(types, -1);
        for (int i = 0; i < types.size(); i++) {
            tuple.appendNull();
        }

        assertEquals(tuple.getTypes(), types);
        assertEquals(tuple.getFieldCount(), types.size());

        for (int i = 0; i < tuple.getFieldCount(); i++) {
            assertTrue(tuple.isNull(i));
            assertEquals(tuple.getType(i), types.get(i));
        }
    }

    @Test
    public void testLong()
            throws Exception
    {
        List<Long> interestingValues = ImmutableList.<Long>builder()
                .add(Long.MIN_VALUE)
                .add(0L)
                .add(Long.MAX_VALUE)
                .build();

        // +1 for last NULl field
        List<Type> types = Lists.<Type>newArrayList(Collections.nCopies(interestingValues.size() + 1, BigintType.BIGINT));

        TupleBuffer tupleBuffer = new TupleBuffer(types, -1);
        for (Long interestingValue : interestingValues) {
            tupleBuffer.appendLong(interestingValue);
        }
        tupleBuffer.appendNull();

        for (int i = 0; i < interestingValues.size(); i++) {
            assertFalse(tupleBuffer.isNull(i));
            assertEquals((Long) tupleBuffer.getLong(i), interestingValues.get(i));
        }
        assertTrue(tupleBuffer.isNull(interestingValues.size()));
    }

    @Test
    public void testDouble()
            throws Exception
    {
        List<Double> interestingValues = ImmutableList.<Double>builder()
                .add(Double.NEGATIVE_INFINITY)
                .add(Double.MIN_VALUE)
                .add(Double.MIN_NORMAL)
                .add(0.0)
                .add(Double.NaN)
                .add(Double.MAX_VALUE)
                .add(Double.POSITIVE_INFINITY)
                .build();

        // +1 for last NULl field
        List<Type> types = Lists.<Type>newArrayList(Collections.nCopies(interestingValues.size() + 1, DoubleType.DOUBLE));

        TupleBuffer tupleBuffer = new TupleBuffer(types, -1);
        for (Double interestingValue : interestingValues) {
            tupleBuffer.appendDouble(interestingValue);
        }
        tupleBuffer.appendNull();
        for (int i = 0; i < interestingValues.size(); i++) {
            assertFalse(tupleBuffer.isNull(i));
            assertEquals((Double) tupleBuffer.getDouble(i), interestingValues.get(i));
        }
        assertTrue(tupleBuffer.isNull(interestingValues.size()));
    }

    @Test
    public void testBoolean()
            throws Exception
    {
        List<Boolean> interestingValues = ImmutableList.<Boolean>builder()
                .add(Boolean.TRUE)
                .add(Boolean.FALSE)
                .build();

        // +1 for last NULl field
        List<Type> types = Lists.<Type>newArrayList(Collections.nCopies(interestingValues.size() + 1, BooleanType.BOOLEAN));

        TupleBuffer tupleBuffer = new TupleBuffer(types, -1);
        for (Boolean interestingValue : interestingValues) {
            tupleBuffer.appendBoolean(interestingValue);
        }
        tupleBuffer.appendNull();

        for (int i = 0; i < interestingValues.size(); i++) {
            assertFalse(tupleBuffer.isNull(i));
            assertEquals((Boolean) tupleBuffer.getBoolean(i), interestingValues.get(i));
        }
        assertTrue(tupleBuffer.isNull(interestingValues.size()));
    }

    @Test
    public void testSlice()
            throws Exception
    {
        List<Slice> interestingValues = ImmutableList.<Slice>builder()
                .add(Slices.EMPTY_SLICE)
                .add(Slices.allocate(65535))
                .build();

        // +1 for last NULl field
        List<Type> types = Lists.<Type>newArrayList(Collections.nCopies(interestingValues.size() + 1, VarbinaryType.VARBINARY));

        TupleBuffer tupleBuffer = new TupleBuffer(types, -1);
        for (Slice interestingValue : interestingValues) {
            tupleBuffer.appendSlice(interestingValue);
        }
        tupleBuffer.appendNull();

        for (int i = 0; i < interestingValues.size(); i++) {
            assertFalse(tupleBuffer.isNull(i));
            assertEquals(tupleBuffer.getSlice(i), interestingValues.get(i));
        }
        assertTrue(tupleBuffer.isNull(interestingValues.size()));
    }

    @Test
    public void testReset()
            throws Exception
    {
        List<Type> types = ImmutableList.<Type>builder()
                .add(BigintType.BIGINT)
                .add(DoubleType.DOUBLE)
                .build();

        TupleBuffer tupleBuffer = new TupleBuffer(types, -1);

        TupleBuffer tupleA = tupleBuffer.appendLong(1L)
                .appendNull();
        assertEquals(tupleA.getLong(0), 1L);
        assertTrue(tupleA.isNull(1));

        tupleBuffer.reset();
        TupleBuffer tupleB = tupleBuffer.appendLong(2L)
                .appendDouble(0.1);
        assertEquals(tupleB.getLong(0), 2L);
        assertEquals(tupleB.getDouble(1), 0.1);
    }
}
