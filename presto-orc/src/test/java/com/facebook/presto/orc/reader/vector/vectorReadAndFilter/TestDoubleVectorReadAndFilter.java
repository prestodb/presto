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
package com.facebook.presto.orc.reader.vector.vectorReadAndFilter;

import com.facebook.presto.common.predicate.vector.DoubleRangeVector;
import com.facebook.presto.common.predicate.vector.TupleDomainFilterVector;
import com.facebook.presto.orc.reader.vector.DoubleVectorSelectiveReadAndFilterSpecies512;
import com.facebook.presto.orc.stream.DoubleInputStream;
import org.mockito.Mockito;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import static org.mockito.Matchers.anyInt;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

@Test(singleThreaded = true)
public class TestDoubleVectorReadAndFilter
{
    TupleDomainFilterVector filter;

    DoubleInputStream inputStream;

    int[] positions;

    int positionCount;

    int[] outputPositions;

    @BeforeMethod
    public void setup()
    {
        filter = (TupleDomainFilterVector) DoubleRangeVector.of(10.0, false, false, 40.0, false, false, false);
        inputStream = Mockito.mock(DoubleInputStream.class);
    }

    @Test
    public void testBothVectorAndTailPart()
    {
        try {
            // Both vector part and tail part of logic is executed
            double[] doubleArr = new double[20];
            for (int i = 1; i < 21; i++) {
                doubleArr[i - 1] = (double) ((i * 10) % 80);
            }
            ByteBuffer bb = ByteBuffer.allocate(doubleArr.length * 8);
            bb.order(ByteOrder.LITTLE_ENDIAN);
            for (double d : doubleArr) {
                bb.putDouble(d);
            }
            Mockito.when(inputStream.readDoubleDataInBytes(anyInt())).thenReturn(bb.array());
            Mockito.when(inputStream.next()).thenReturn(10.0, 20.0, 30.0, 40.0);
            positions = new int[20];
            for (int i = 0; i < 20; i++) {
                positions[i] = i;
            }
            positionCount = 20;
            outputPositions = new int[20];

            DoubleVectorSelectiveReadAndFilterSpecies512 doubleOptimizedReadAndFilterSpecies512 = new DoubleVectorSelectiveReadAndFilterSpecies512();
            int outputPositionCount = doubleOptimizedReadAndFilterSpecies512.firstLevelReadAndFilter(filter, inputStream, positions, positionCount, outputPositions);
            assertEquals(outputPositionCount, 12);
            assertEquals(outputPositions[0], 0);
            assertEquals(outputPositions[1], 1);
            assertEquals(outputPositions[2], 2);
            assertEquals(outputPositions[3], 3);
            assertEquals(outputPositions[4], 8);
            assertEquals(outputPositions[5], 9);
            assertEquals(outputPositions[6], 10);
            assertEquals(outputPositions[7], 11);
            assertEquals(outputPositions[8], 16);
            assertEquals(outputPositions[9], 17);
            assertEquals(outputPositions[10], 18);
            assertEquals(outputPositions[11], 19);
            assertEquals(outputPositions[12], 0);
            assertEquals(outputPositions[13], 0);
            assertEquals(outputPositions[14], 0);
            assertEquals(outputPositions[15], 0);
            assertEquals(outputPositions[16], 0);
            assertEquals(outputPositions[17], 0);
            assertEquals(outputPositions[18], 0);
            assertEquals(outputPositions[19], 0);
        }
        catch (IOException e) {
            assertTrue(false, "IOException");
        }
    }

    @Test
    public void testBothVectorAndTailPartNonePass()
    {
        try {
            filter = (TupleDomainFilterVector) DoubleRangeVector.of(100.0, false, false, 140.0, false, false, false);
            // Both vector part and tail part of logic is executed
            double[] doubleArr = new double[20];
            for (int i = 1; i < 21; i++) {
                doubleArr[i - 1] = (double) ((i * 10) % 80);
            }
            ByteBuffer bb = ByteBuffer.allocate(doubleArr.length * 8);
            bb.order(ByteOrder.LITTLE_ENDIAN);
            for (double d : doubleArr) {
                bb.putDouble(d);
            }
            Mockito.when(inputStream.readDoubleDataInBytes(anyInt())).thenReturn(bb.array());
            Mockito.when(inputStream.next()).thenReturn(10.0, 20.0, 30.0, 40.0);
            positions = new int[20];
            for (int i = 0; i < 20; i++) {
                positions[i] = i;
            }
            positionCount = 20;
            outputPositions = new int[20];

            DoubleVectorSelectiveReadAndFilterSpecies512 doubleOptimizedReadAndFilterSpecies512 = new DoubleVectorSelectiveReadAndFilterSpecies512();
            int outputPositionCount = doubleOptimizedReadAndFilterSpecies512.firstLevelReadAndFilter(filter, inputStream, positions, positionCount, outputPositions);
            assertEquals(outputPositionCount, 0);
            for (int i = 0; i < 20; i++) {
                assertEquals(outputPositions[i], 0);
            }
        }
        catch (IOException e) {
            assertTrue(false, "IOException");
        }
    }

    @Test
    public void testOnlyTailPart()
    {
        try {
            // When we have less than 8 double values only tail part of logic is executed as data wont fit into vector logic
            double[] doubleArr = new double[4];
            for (int i = 1; i < 4; i++) {
                doubleArr[i - 1] = (double) ((i * 10) % 80);
            }
            ByteBuffer bb = ByteBuffer.allocate(doubleArr.length * 8);
            bb.order(ByteOrder.LITTLE_ENDIAN);
            for (double d : doubleArr) {
                bb.putDouble(d);
            }
            Mockito.when(inputStream.readDoubleDataInBytes(anyInt())).thenReturn(bb.array());
            Mockito.when(inputStream.next()).thenReturn(10.0, 20.0, 30.0, 40.0);
            positions = new int[4];
            for (int i = 0; i < 4; i++) {
                positions[i] = i;
            }
            positionCount = 4;
            outputPositions = new int[4];

            DoubleVectorSelectiveReadAndFilterSpecies512 doubleOptimizedReadAndFilterSpecies512 = new DoubleVectorSelectiveReadAndFilterSpecies512();
            int outputPositionCount = doubleOptimizedReadAndFilterSpecies512.firstLevelReadAndFilter(filter, inputStream, positions, positionCount, outputPositions);
            assertEquals(outputPositionCount, 4);
            assertEquals(outputPositions[0], 0);
            assertEquals(outputPositions[1], 1);
            assertEquals(outputPositions[2], 2);
            assertEquals(outputPositions[3], 3);
        }
        catch (IOException e) {
            assertTrue(false, "IOException");
        }
    }

    @Test
    public void testOnlyVectorPart()
    {
        try {
            // When we have data in multiple of 8 numbers only vector path will be hit
            double[] doubleArr = new double[16];
            for (int i = 1; i < 17; i++) {
                doubleArr[i - 1] = (double) ((i * 10) % 80);
            }
            ByteBuffer bb = ByteBuffer.allocate(doubleArr.length * 8);
            bb.order(ByteOrder.LITTLE_ENDIAN);
            for (double d : doubleArr) {
                bb.putDouble(d);
            }
            Mockito.when(inputStream.readDoubleDataInBytes(anyInt())).thenReturn(bb.array());
            Mockito.when(inputStream.next()).thenReturn(10.0, 20.0, 30.0, 40.0);
            positions = new int[16];
            for (int i = 0; i < 16; i++) {
                positions[i] = i;
            }
            positionCount = 16;
            outputPositions = new int[16];

            DoubleVectorSelectiveReadAndFilterSpecies512 doubleOptimizedReadAndFilterSpecies512 = new DoubleVectorSelectiveReadAndFilterSpecies512();
            int outputPositionCount = doubleOptimizedReadAndFilterSpecies512.firstLevelReadAndFilter(filter, inputStream, positions, positionCount, outputPositions);
            assertEquals(outputPositionCount, 8);
            assertEquals(outputPositions[0], 0);
            assertEquals(outputPositions[1], 1);
            assertEquals(outputPositions[2], 2);
            assertEquals(outputPositions[3], 3);
            assertEquals(outputPositions[4], 8);
            assertEquals(outputPositions[5], 9);
            assertEquals(outputPositions[6], 10);
            assertEquals(outputPositions[7], 11);
            assertEquals(outputPositions[8], 0);
            assertEquals(outputPositions[9], 0);
            assertEquals(outputPositions[10], 0);
            assertEquals(outputPositions[11], 0);
            assertEquals(outputPositions[12], 0);
            assertEquals(outputPositions[13], 0);
            assertEquals(outputPositions[14], 0);
            assertEquals(outputPositions[15], 0);
        }
        catch (IOException e) {
            assertTrue(false, "IOException");
        }
    }

    @Test
    public void testWithOutpuRequiredBothVectorAndTailPart()
    {
        try {
            double[] doubleArr = new double[20];
            for (int i = 1; i < 21; i++) {
                doubleArr[i - 1] = (double) ((i * 10) % 80);
            }
            ByteBuffer bb = ByteBuffer.allocate(doubleArr.length * 8);
            bb.order(ByteOrder.LITTLE_ENDIAN);
            for (double d : doubleArr) {
                bb.putDouble(d);
            }
            Mockito.when(inputStream.readDoubleDataInBytes(anyInt())).thenReturn(bb.array());
            Mockito.when(inputStream.next()).thenReturn(10.0, 20.0, 30.0, 40.0);
            positions = new int[20];
            for (int i = 0; i < 20; i++) {
                positions[i] = i;
            }
            positionCount = 20;
            outputPositions = new int[20];
            long[] values = new long[20];

            DoubleVectorSelectiveReadAndFilterSpecies512 doubleOptimizedReadAndFilterSpecies512 = new DoubleVectorSelectiveReadAndFilterSpecies512();
            int outputPositionCount = doubleOptimizedReadAndFilterSpecies512.firstLevelReadAndFilterWithOutputRequired(filter,
                    inputStream,
                    positions,
                    positionCount,
                    outputPositions,
                    values);
            assertEquals(outputPositionCount, 12);
            assertEquals(outputPositions[0], 0);
            assertEquals(outputPositions[1], 1);
            assertEquals(outputPositions[2], 2);
            assertEquals(outputPositions[3], 3);
            assertEquals(outputPositions[4], 8);
            assertEquals(outputPositions[5], 9);
            assertEquals(outputPositions[6], 10);
            assertEquals(outputPositions[7], 11);
            assertEquals(outputPositions[8], 16);
            assertEquals(outputPositions[9], 17);
            assertEquals(outputPositions[10], 18);
            assertEquals(outputPositions[11], 19);
            assertEquals(outputPositions[12], 0);
            assertEquals(outputPositions[13], 0);
            assertEquals(outputPositions[14], 0);
            assertEquals(outputPositions[15], 0);
            assertEquals(outputPositions[16], 0);
            assertEquals(outputPositions[17], 0);
            assertEquals(outputPositions[18], 0);
            assertEquals(outputPositions[19], 0);

            for (int i = 0; i < 12; i++) {
                assertTrue(values[i] <= Double.doubleToLongBits(40) && values[i] >= Double.doubleToLongBits(10));
            }
            for (int i = 12; i < 20; i++) {
                assertFalse(values[i] <= Double.doubleToLongBits(40) && values[i] >= Double.doubleToLongBits(10));
            }
        }
        catch (IOException e) {
            assertTrue(false, "IOException");
        }
    }

    @Test
    public void testWithOutpuRequiredOnlyTailPart()
    {
        try {
            double[] doubleArr = new double[4];
            for (int i = 1; i < 4; i++) {
                doubleArr[i - 1] = (double) ((i * 10) % 80);
            }
            ByteBuffer bb = ByteBuffer.allocate(doubleArr.length * 8);
            bb.order(ByteOrder.LITTLE_ENDIAN);
            for (double d : doubleArr) {
                bb.putDouble(d);
            }
            Mockito.when(inputStream.readDoubleDataInBytes(anyInt())).thenReturn(bb.array());
            Mockito.when(inputStream.next()).thenReturn(10.0, 20.0, 30.0, 40.0);
            positions = new int[4];
            for (int i = 0; i < 4; i++) {
                positions[i] = i;
            }
            positionCount = 4;
            outputPositions = new int[4];
            long[] values = new long[4];

            DoubleVectorSelectiveReadAndFilterSpecies512 doubleOptimizedReadAndFilterSpecies512 = new DoubleVectorSelectiveReadAndFilterSpecies512();

            int outputPositionCount = doubleOptimizedReadAndFilterSpecies512.firstLevelReadAndFilterWithOutputRequired(filter,
                    inputStream,
                    positions,
                    positionCount,
                    outputPositions,
                    values);
            assertEquals(outputPositionCount, 4);
            assertEquals(outputPositions[0], 0);
            assertEquals(outputPositions[1], 1);
            assertEquals(outputPositions[2], 2);
            assertEquals(outputPositions[3], 3);

            for (int i = 0; i < 4; i++) {
                assertTrue(values[i] <= Double.doubleToLongBits(40) && values[i] >= Double.doubleToLongBits(10));
            }
        }
        catch (IOException e) {
            assertTrue(false, "IOException");
        }
    }

    @Test
    public void testWithOutpuRequiredOnlyVectorPart()
    {
        try {
            double[] doubleArr = new double[16];
            for (int i = 1; i < 17; i++) {
                doubleArr[i - 1] = (double) ((i * 10) % 80);
            }
            ByteBuffer bb = ByteBuffer.allocate(doubleArr.length * 8);
            bb.order(ByteOrder.LITTLE_ENDIAN);
            for (double d : doubleArr) {
                bb.putDouble(d);
            }
            Mockito.when(inputStream.readDoubleDataInBytes(anyInt())).thenReturn(bb.array());
            Mockito.when(inputStream.next()).thenReturn(10.0, 20.0, 30.0, 40.0);
            positions = new int[16];
            for (int i = 0; i < 16; i++) {
                positions[i] = i;
            }
            positionCount = 16;
            outputPositions = new int[16];
            long[] values = new long[16];

            DoubleVectorSelectiveReadAndFilterSpecies512 doubleOptimizedReadAndFilterSpecies512 = new DoubleVectorSelectiveReadAndFilterSpecies512();

            int outputPositionCount = doubleOptimizedReadAndFilterSpecies512.firstLevelReadAndFilterWithOutputRequired(filter,
                    inputStream,
                    positions,
                    positionCount,
                    outputPositions,
                    values);
            assertEquals(outputPositionCount, 8);
            assertEquals(outputPositions[0], 0);
            assertEquals(outputPositions[1], 1);
            assertEquals(outputPositions[2], 2);
            assertEquals(outputPositions[3], 3);
            assertEquals(outputPositions[4], 8);
            assertEquals(outputPositions[5], 9);
            assertEquals(outputPositions[6], 10);
            assertEquals(outputPositions[7], 11);
            assertEquals(outputPositions[8], 0);
            assertEquals(outputPositions[9], 0);
            assertEquals(outputPositions[10], 0);
            assertEquals(outputPositions[11], 0);
            assertEquals(outputPositions[12], 0);
            assertEquals(outputPositions[13], 0);
            assertEquals(outputPositions[14], 0);
            assertEquals(outputPositions[15], 0);

            for (int i = 0; i < 8; i++) {
                assertTrue(values[i] <= Double.doubleToLongBits(40) && values[i] >= Double.doubleToLongBits(10));
            }
            for (int i = 8; i < 16; i++) {
                assertFalse(values[i] <= Double.doubleToLongBits(40) && values[i] >= Double.doubleToLongBits(10));
            }
        }
        catch (IOException e) {
            assertTrue(false, "IOException");
        }
    }

    @Test
    public void testWithOutpuRequiredBothVectorAndTailPartNonePass()
    {
        try {
            filter = (TupleDomainFilterVector) DoubleRangeVector.of(100.0, false, false, 140.0, false, false, false);
            // Both vector part and tail part of logic is executed
            double[] doubleArr = new double[20];
            for (int i = 1; i < 21; i++) {
                doubleArr[i - 1] = (double) ((i * 10) % 80);
            }
            ByteBuffer bb = ByteBuffer.allocate(doubleArr.length * 8);
            bb.order(ByteOrder.LITTLE_ENDIAN);
            for (double d : doubleArr) {
                bb.putDouble(d);
            }
            Mockito.when(inputStream.readDoubleDataInBytes(anyInt())).thenReturn(bb.array());
            Mockito.when(inputStream.next()).thenReturn(10.0, 20.0, 30.0, 40.0);
            positions = new int[20];
            for (int i = 0; i < 20; i++) {
                positions[i] = i;
            }
            positionCount = 20;
            outputPositions = new int[20];
            long[] values = new long[20];

            DoubleVectorSelectiveReadAndFilterSpecies512 doubleOptimizedReadAndFilterSpecies512 = new DoubleVectorSelectiveReadAndFilterSpecies512();

            int outputPositionCount = doubleOptimizedReadAndFilterSpecies512.firstLevelReadAndFilterWithOutputRequired(filter,
                    inputStream,
                    positions,
                    positionCount,
                    outputPositions,
                    values);
            assertEquals(outputPositionCount, 0);
            for (int i = 0; i < 20; i++) {
                assertEquals(outputPositions[i], 0);
            }
        }
        catch (IOException e) {
            assertTrue(false, "IOException");
        }
    }
}
