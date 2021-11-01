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
package com.facebook.presto.orc;

import com.facebook.presto.orc.DictionaryCompressionOptimizer.DictionaryColumn;
import com.google.common.collect.ImmutableSet;
import com.google.common.math.IntMath;
import io.airlift.units.DataSize;
import io.airlift.units.DataSize.Unit;
import org.testng.annotations.Test;

import java.util.List;
import java.util.OptionalInt;
import java.util.Set;
import java.util.stream.Collectors;

import static com.facebook.airlift.testing.Assertions.assertGreaterThanOrEqual;
import static com.facebook.airlift.testing.Assertions.assertLessThan;
import static com.facebook.presto.orc.DictionaryCompressionOptimizer.estimateIndexBytesPerValue;
import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.Math.min;
import static java.lang.Math.toIntExact;
import static java.math.RoundingMode.CEILING;
import static java.util.Objects.requireNonNull;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class TestDictionaryCompressionOptimizer
{
    private static final int DICTIONARY_ALMOST_FULL_MEMORY_RANGE = megabytes(4);
    private static final int CHUNK_ROW_COUNT = 1024;

    @Test
    public void testNoDictionariesBytesLimit()
    {
        int stripeMaxBytes = megabytes(100);
        int bytesPerRow = 1024;
        int expectedMaxRowCount = stripeMaxBytes / bytesPerRow;
        DataSimulator simulator = new DataSimulator(0, stripeMaxBytes, expectedMaxRowCount * 2, megabytes(16), bytesPerRow);

        for (int loop = 0; loop < 3; loop++) {
            assertFalse(simulator.isDictionaryMemoryFull());
            assertEquals(simulator.getRowCount(), 0);
            assertEquals(simulator.getBufferedBytes(), 0);

            simulator.advanceToNextStateChange();

            // since there are no dictionary columns, the simulator should advance until the strip is full
            assertFalse(simulator.isDictionaryMemoryFull());
            assertGreaterThanOrEqual(simulator.getBufferedBytes(), (long) stripeMaxBytes);
            assertGreaterThanOrEqual(simulator.getRowCount(), expectedMaxRowCount);

            simulator.finalOptimize();

            assertFalse(simulator.isDictionaryMemoryFull());
            assertGreaterThanOrEqual(simulator.getBufferedBytes(), (long) stripeMaxBytes);
            assertGreaterThanOrEqual(simulator.getRowCount(), expectedMaxRowCount);

            simulator.reset();
        }
    }

    @Test
    public void testSingleDictionaryColumnRowLimit()
    {
        int bytesPerEntry = 1024;
        TestDictionaryColumn column = dictionaryColumn(bytesPerEntry, 1024);

        // construct a simulator that will hit the row limit first
        int stripeMaxBytes = megabytes(1000);
        int expectedMaxRowCount = 1_000_000;
        DataSimulator simulator = new DataSimulator(0, stripeMaxBytes, expectedMaxRowCount, megabytes(16), 0, column);

        for (int loop = 0; loop < 3; loop++) {
            assertFalse(simulator.isDictionaryMemoryFull());
            assertFalse(column.isDirectEncoded());
            assertEquals(simulator.getRowCount(), 0);
            assertEquals(simulator.getBufferedBytes(), 0);

            simulator.advanceToNextStateChange();

            // since there dictionary columns is only 1 MB, the simulator should advance until the strip is full
            assertFalse(simulator.isDictionaryMemoryFull());
            assertFalse(column.isDirectEncoded());
            assertLessThan(simulator.getBufferedBytes(), (long) stripeMaxBytes);
            assertGreaterThanOrEqual(simulator.getRowCount(), expectedMaxRowCount);

            simulator.finalOptimize();

            assertFalse(simulator.isDictionaryMemoryFull());
            assertFalse(column.isDirectEncoded());
            assertLessThan(simulator.getBufferedBytes(), (long) stripeMaxBytes);
            assertGreaterThanOrEqual(simulator.getRowCount(), expectedMaxRowCount);

            simulator.reset();
        }
    }

    @Test
    public void testSingleDictionaryColumnByteLimit()
    {
        int bytesPerEntry = 1024;
        int dictionaryEntries = 1024;
        TestDictionaryColumn column = dictionaryColumn(bytesPerEntry, dictionaryEntries);

        // construct a simulator that will hit the row limit first
        int stripeMaxBytes = megabytes(100);
        int bytesPerRow = estimateIndexBytesPerValue(dictionaryEntries);
        int expectedMaxRowCount = stripeMaxBytes / bytesPerRow;
        DataSimulator simulator = new DataSimulator(0, stripeMaxBytes, expectedMaxRowCount * 10, megabytes(16), 0, column);

        for (int loop = 0; loop < 3; loop++) {
            assertFalse(simulator.isDictionaryMemoryFull());
            assertFalse(column.isDirectEncoded());
            assertEquals(simulator.getRowCount(), 0);
            assertEquals(simulator.getBufferedBytes(), 0);

            simulator.advanceToNextStateChange();

            // since there dictionary columns is only 1 MB, the simulator should advance until the strip is full
            assertFalse(simulator.isDictionaryMemoryFull());
            assertFalse(column.isDirectEncoded());
            assertGreaterThanOrEqual(simulator.getBufferedBytes(), (long) stripeMaxBytes);
            assertLessThan(simulator.getRowCount(), expectedMaxRowCount);

            simulator.finalOptimize();

            assertFalse(simulator.isDictionaryMemoryFull());
            assertFalse(column.isDirectEncoded());
            assertGreaterThanOrEqual(simulator.getBufferedBytes(), (long) stripeMaxBytes);
            assertLessThan(simulator.getRowCount(), expectedMaxRowCount);

            simulator.reset();
        }
    }

    @Test
    public void testSingleDictionaryColumnMemoryLimit()
    {
        int bytesPerEntry = 1024;
        int dictionaryMaxMemoryBytes = megabytes(32);
        double uniquePercentage = 0.5;
        TestDictionaryColumn column = directColumn(bytesPerEntry, uniquePercentage);

        // construct a simulator that will hit the dictionary (low) memory limit by estimating the number of rows at the memory limit, and then setting large limits around this value
        int stripeMaxBytes = megabytes(100);
        int dictionaryMaxMemoryBytesLow = dictionaryMaxMemoryBytes - DICTIONARY_ALMOST_FULL_MEMORY_RANGE;
        int expectedMaxRowCount = (int) (dictionaryMaxMemoryBytesLow / bytesPerEntry / uniquePercentage);
        DataSimulator simulator = new DataSimulator(0, stripeMaxBytes, expectedMaxRowCount * 2, dictionaryMaxMemoryBytes, 0, column);

        for (int loop = 0; loop < 3; loop++) {
            assertFalse(simulator.isDictionaryMemoryFull());
            assertFalse(column.isDirectEncoded());
            assertEquals(simulator.getRowCount(), 0);
            assertEquals(simulator.getBufferedBytes(), 0);

            simulator.advanceToNextStateChange();

            // the simulator should advance until memory is full
            assertTrue(simulator.isDictionaryMemoryFull());
            assertFalse(column.isDirectEncoded());
            assertLessThan(simulator.getBufferedBytes(), (long) stripeMaxBytes);
            assertGreaterThanOrEqual(simulator.getRowCount(), expectedMaxRowCount);

            simulator.finalOptimize();

            assertTrue(simulator.isDictionaryMemoryFull());
            assertFalse(column.isDirectEncoded());
            assertLessThan(simulator.getBufferedBytes(), (long) stripeMaxBytes);
            assertGreaterThanOrEqual(simulator.getRowCount(), expectedMaxRowCount);

            simulator.reset();
        }
    }

    @Test
    public void testDirectConversionOnDictionaryFull()
    {
        int bytesPerEntry = 1024;
        int dictionaryMaxMemoryBytes = megabytes(8);
        double uniquePercentage = 0.2;
        TestDictionaryColumn column = directColumn(bytesPerEntry, uniquePercentage);

        // construct a simulator that will flip the column to direct and then hit the bytes limit
        int stripeMaxBytes = megabytes(100);
        int expectedRowCountAtFlip = (int) (dictionaryMaxMemoryBytes / bytesPerEntry / uniquePercentage);
        int expectedMaxRowCountAtFull = stripeMaxBytes / bytesPerEntry;
        DataSimulator simulator = new DataSimulator(stripeMaxBytes / 2, stripeMaxBytes, expectedMaxRowCountAtFull * 2, dictionaryMaxMemoryBytes, 0, column);

        for (int loop = 0; loop < 3; loop++) {
            assertFalse(simulator.isDictionaryMemoryFull());
            assertFalse(column.isDirectEncoded());
            assertEquals(simulator.getRowCount(), 0);
            assertEquals(simulator.getBufferedBytes(), 0);

            simulator.advanceToNextStateChange();

            // the simulator should advance until the dictionary column is converted to direct
            assertFalse(simulator.isDictionaryMemoryFull());
            assertTrue(column.isDirectEncoded());
            assertLessThan(simulator.getBufferedBytes(), (long) stripeMaxBytes);
            assertGreaterThanOrEqual(simulator.getRowCount(), expectedRowCountAtFlip);

            simulator.advanceToNextStateChange();

            // the simulator should advance until the stripe is full
            assertFalse(simulator.isDictionaryMemoryFull());
            assertTrue(column.isDirectEncoded());
            assertGreaterThanOrEqual(simulator.getBufferedBytes(), (long) stripeMaxBytes);
            assertGreaterThanOrEqual(simulator.getRowCount(), expectedMaxRowCountAtFull);

            simulator.finalOptimize();

            assertFalse(simulator.isDictionaryMemoryFull());
            assertTrue(column.isDirectEncoded());
            assertGreaterThanOrEqual(simulator.getBufferedBytes(), (long) stripeMaxBytes);
            assertGreaterThanOrEqual(simulator.getRowCount(), expectedMaxRowCountAtFull);

            simulator.reset();
        }
    }

    @Test
    public void testNotDirectConversionOnDictionaryFull()
    {
        int bytesPerEntry = 1024;
        int dictionaryMaxMemoryBytes = megabytes(8);
        double uniquePercentage = 0.01;
        TestDictionaryColumn column = directColumn(bytesPerEntry, uniquePercentage);

        // construct a simulator that will be full because of dictionary memory limit;
        // the column cannot not be converted to direct encoding because of stripe size limit
        int stripeMaxBytes = megabytes(100);
        int expectedMaxRowCount = (int) (dictionaryMaxMemoryBytes / bytesPerEntry / uniquePercentage);
        DataSimulator simulator = new DataSimulator(stripeMaxBytes / 2, stripeMaxBytes, expectedMaxRowCount * 2, dictionaryMaxMemoryBytes, 0, column);

        for (int loop = 0; loop < 3; loop++) {
            assertFalse(simulator.isDictionaryMemoryFull());
            assertFalse(column.isDirectEncoded());
            assertEquals(simulator.getRowCount(), 0);
            assertEquals(simulator.getBufferedBytes(), 0);

            simulator.advanceToNextStateChange();

            // the simulator should advance until memory is full
            assertTrue(simulator.isDictionaryMemoryFull());
            assertFalse(column.isDirectEncoded());
            assertLessThan(simulator.getBufferedBytes(), (long) stripeMaxBytes);
            assertGreaterThanOrEqual(simulator.getRowCount(), expectedMaxRowCount);

            simulator.finalOptimize();

            assertTrue(simulator.isDictionaryMemoryFull());
            assertFalse(column.isDirectEncoded());
            assertLessThan(simulator.getBufferedBytes(), (long) stripeMaxBytes);
            assertGreaterThanOrEqual(simulator.getRowCount(), expectedMaxRowCount);

            simulator.reset();
        }
    }

    @Test
    public void testSingleDirectBytesLimit()
    {
        int bytesPerEntry = 1024;
        int dictionaryMaxMemoryBytes = megabytes(16);
        TestDictionaryColumn column = directColumn(bytesPerEntry, 1.0);

        // construct a simulator that will flip the column to direct and then hit the bytes limit
        int stripeMaxBytes = megabytes(100);
        int expectedRowCountAtFlip = ((dictionaryMaxMemoryBytes - DICTIONARY_ALMOST_FULL_MEMORY_RANGE) / bytesPerEntry);
        int expectedMaxRowCountAtFull = stripeMaxBytes / bytesPerEntry;
        DataSimulator simulator = new DataSimulator(0, stripeMaxBytes, expectedMaxRowCountAtFull, dictionaryMaxMemoryBytes, 0, column);

        for (int loop = 0; loop < 3; loop++) {
            assertFalse(simulator.isDictionaryMemoryFull());
            assertFalse(column.isDirectEncoded());
            assertEquals(simulator.getRowCount(), 0);
            assertEquals(simulator.getBufferedBytes(), 0);

            simulator.advanceToNextStateChange();

            // the simulator should advance until the dictionary column is flipped
            assertFalse(simulator.isDictionaryMemoryFull());
            assertTrue(column.isDirectEncoded());
            assertLessThan(simulator.getBufferedBytes(), (long) stripeMaxBytes);
            assertGreaterThanOrEqual(simulator.getRowCount(), expectedRowCountAtFlip);

            simulator.advanceToNextStateChange();

            // the simulator should advance until the stripe is full
            assertFalse(simulator.isDictionaryMemoryFull());
            assertTrue(column.isDirectEncoded());
            assertGreaterThanOrEqual(simulator.getBufferedBytes(), (long) stripeMaxBytes);
            assertGreaterThanOrEqual(simulator.getRowCount(), expectedMaxRowCountAtFull);

            simulator.finalOptimize();

            assertFalse(simulator.isDictionaryMemoryFull());
            assertTrue(column.isDirectEncoded());
            assertGreaterThanOrEqual(simulator.getBufferedBytes(), (long) stripeMaxBytes);
            assertGreaterThanOrEqual(simulator.getRowCount(), expectedMaxRowCountAtFull);

            simulator.reset();
        }
    }

    @Test
    public void testDictionaryAndDirectBytesLimit()
    {
        int bytesPerEntry = 1024;
        int dictionaryMaxMemoryBytes = megabytes(8);
        double directUniquePercentage = 0.75;
        TestDictionaryColumn directColumn = directColumn(bytesPerEntry, directUniquePercentage);
        TestDictionaryColumn dictionaryColumn = dictionaryColumn(bytesPerEntry, 1024);

        // construct a simulator that will flip the column to direct and then hit the bytes limit
        int stripeMaxBytes = megabytes(100);
        int expectedRowCountAtFlip = (int) (dictionaryMaxMemoryBytes / (bytesPerEntry * 2 * directUniquePercentage));
        int expectedMaxRowCountAtFull = stripeMaxBytes / (bytesPerEntry * 2);
        DataSimulator simulator = new DataSimulator(stripeMaxBytes / 2, stripeMaxBytes, expectedMaxRowCountAtFull * 2, dictionaryMaxMemoryBytes, 0, directColumn, dictionaryColumn);

        for (int loop = 0; loop < 3; loop++) {
            assertFalse(simulator.isDictionaryMemoryFull());
            assertFalse(directColumn.isDirectEncoded());
            assertFalse(dictionaryColumn.isDirectEncoded());
            assertEquals(simulator.getRowCount(), 0);
            assertEquals(simulator.getBufferedBytes(), 0);

            simulator.advanceToNextStateChange();

            // the simulator should advance until the dictionary column is flipped
            assertFalse(simulator.isDictionaryMemoryFull());
            assertTrue(directColumn.isDirectEncoded());
            assertFalse(dictionaryColumn.isDirectEncoded());
            assertLessThan(simulator.getBufferedBytes(), (long) stripeMaxBytes);
            assertGreaterThanOrEqual(simulator.getRowCount(), expectedRowCountAtFlip);

            simulator.advanceToNextStateChange();

            // the simulator should advance until the stripe is full
            assertFalse(simulator.isDictionaryMemoryFull());
            assertTrue(directColumn.isDirectEncoded());
            assertFalse(dictionaryColumn.isDirectEncoded());
            assertGreaterThanOrEqual(simulator.getBufferedBytes(), (long) stripeMaxBytes);
            assertGreaterThanOrEqual(simulator.getRowCount(), expectedMaxRowCountAtFull);

            simulator.finalOptimize();

            assertFalse(simulator.isDictionaryMemoryFull());
            assertTrue(directColumn.isDirectEncoded());
            assertFalse(dictionaryColumn.isDirectEncoded());
            assertGreaterThanOrEqual(simulator.getBufferedBytes(), (long) stripeMaxBytes);
            assertGreaterThanOrEqual(simulator.getRowCount(), expectedMaxRowCountAtFull);

            simulator.reset();
        }
    }

    @Test
    public void testWideDictionaryAndNarrowDirectBytesLimit()
    {
        int dictionaryMaxMemoryBytes = megabytes(32);
        int directBytesPerEntry = 100;
        double directUniquePercentage = 0.75;
        TestDictionaryColumn directColumn = directColumn(directBytesPerEntry, directUniquePercentage);

        int dictionaryBytesPerEntry = 1024 * 1024;
        int dictionaryEntries = 10;
        TestDictionaryColumn dictionaryColumn = dictionaryColumn(dictionaryBytesPerEntry, dictionaryEntries);

        // construct a simulator that will flip the column to direct and then hit the bytes limit
        int stripeMaxBytes = megabytes(2000);
        int dictionaryMaxMemoryBytesLow = dictionaryMaxMemoryBytes - DICTIONARY_ALMOST_FULL_MEMORY_RANGE;
        int expectedRowCountAtFlip = (int) ((dictionaryMaxMemoryBytesLow - (dictionaryEntries * dictionaryBytesPerEntry)) / (directBytesPerEntry * directUniquePercentage));
        int maxRowCount = 10_000_000;
        DataSimulator simulator = new DataSimulator(0, stripeMaxBytes, maxRowCount, dictionaryMaxMemoryBytes, 0, directColumn, dictionaryColumn);

        for (int loop = 0; loop < 3; loop++) {
            assertFalse(simulator.isDictionaryMemoryFull());
            assertFalse(directColumn.isDirectEncoded());
            assertFalse(dictionaryColumn.isDirectEncoded());
            assertEquals(simulator.getRowCount(), 0);
            assertEquals(simulator.getBufferedBytes(), 0);

            simulator.advanceToNextStateChange();

            // the simulator should advance until the dictionary column is flipped
            assertFalse(simulator.isDictionaryMemoryFull());
            assertTrue(directColumn.isDirectEncoded());
            assertFalse(dictionaryColumn.isDirectEncoded());
            assertLessThan(simulator.getBufferedBytes(), (long) stripeMaxBytes);
            assertGreaterThanOrEqual(simulator.getRowCount(), expectedRowCountAtFlip);

            simulator.advanceToNextStateChange();

            // the simulator should advance until the stripe is full
            assertFalse(simulator.isDictionaryMemoryFull());
            assertTrue(directColumn.isDirectEncoded());
            assertFalse(dictionaryColumn.isDirectEncoded());
            assertLessThan(simulator.getBufferedBytes(), (long) stripeMaxBytes);
            assertGreaterThanOrEqual(simulator.getRowCount(), maxRowCount);

            simulator.finalOptimize();

            assertFalse(simulator.isDictionaryMemoryFull());
            assertTrue(directColumn.isDirectEncoded());
            assertFalse(dictionaryColumn.isDirectEncoded());
            assertLessThan(simulator.getBufferedBytes(), (long) stripeMaxBytes);
            assertGreaterThanOrEqual(simulator.getRowCount(), maxRowCount);

            simulator.reset();
        }
    }

    @Test
    public void testDictionaryUsefulCheck()
    {
        int largeBytesPerEntry = 768;
        TestDictionaryColumn largeDirectColumn = directColumn(largeBytesPerEntry, 1);
        int smallBytePerEntry = 256;
        TestDictionaryColumn smallDirectColumn = directColumn(smallBytePerEntry, 1);

        int dictionaryUsefulCheckPerChunkFrequency = 5;
        int dictionaryUsefulCheckColumnSizeBytes = megabytes(10);

        // Set Stripe Size high, so that dictionary should be abandoned much before.
        int stripeMinBytes = megabytes(275);
        int stripeMaxBytes = megabytes(300);

        int dictionaryMaxMemoryBytes = megabytes(64);
        int maxRowCount = 10_000_000;

        // This test should abandon dictionary due to dictionary usefulness check. It has 2 columns first column is 768 bytes
        // second column is 256 bytes, so each row is 1KB. When large column reaches 10 MB, the large column dictionary will
        // be abandoned. Once the large column dictionary is abandoned, there is only small column with dictionary. Small column
        // will reach 10 MB and will get abandoned as well. The constant (dictionaryUsefulCheckPerChunkFrequency + 1) * CHUNK_ROW_COUNT
        // is added as the check is happening once every dictionaryUsefulCheckPerChunkFrequency.

        int largeColumnAbandonRowCount = IntMath.divide(dictionaryUsefulCheckColumnSizeBytes, largeBytesPerEntry, CEILING);
        int largeColumnAbandonUpperBound = largeColumnAbandonRowCount + (dictionaryUsefulCheckPerChunkFrequency + 1) * CHUNK_ROW_COUNT;
        int smallColumnAbandonRowCount = IntMath.divide(dictionaryUsefulCheckColumnSizeBytes, smallBytePerEntry, CEILING);
        int smallColumnAbandonUpperBound = smallColumnAbandonRowCount + (dictionaryUsefulCheckPerChunkFrequency + 1) * CHUNK_ROW_COUNT;

        DataSimulator simulator = new DataSimulator(stripeMinBytes,
                stripeMaxBytes,
                maxRowCount,
                dictionaryMaxMemoryBytes,
                0,
                DICTIONARY_ALMOST_FULL_MEMORY_RANGE,
                dictionaryUsefulCheckPerChunkFrequency,
                dictionaryUsefulCheckColumnSizeBytes,
                largeDirectColumn,
                smallDirectColumn);

        for (int loop = 0; loop < 3; loop++) {
            assertFalse(simulator.isDictionaryMemoryFull());
            assertFalse(largeDirectColumn.isDirectEncoded());
            assertFalse(smallDirectColumn.isDirectEncoded());
            assertEquals(simulator.getRowCount(), 0);
            assertEquals(simulator.getBufferedBytes(), 0);

            simulator.advanceToNextStateChange();

            // the simulator should advance until the first large column switches to direct encoding.
            assertFalse(simulator.isDictionaryMemoryFull());
            assertTrue(largeDirectColumn.isDirectEncoded());
            assertFalse(smallDirectColumn.isDirectEncoded());
            assertGreaterThanOrEqual(simulator.getRowCount(), largeColumnAbandonRowCount);
            assertLessThan(simulator.getRowCount(), largeColumnAbandonUpperBound);
            assertLessThan(simulator.getRowCount(), smallColumnAbandonRowCount);

            simulator.advanceToNextStateChange();

            // the simulator should advance until the second column switches to direct encoding.
            assertFalse(simulator.isDictionaryMemoryFull());
            assertTrue(largeDirectColumn.isDirectEncoded());
            assertTrue(smallDirectColumn.isDirectEncoded());
            assertLessThan(simulator.getBufferedBytes(), (long) stripeMaxBytes);
            assertGreaterThanOrEqual(simulator.getRowCount(), smallColumnAbandonRowCount);
            assertLessThan(simulator.getRowCount(), smallColumnAbandonUpperBound);

            simulator.finalOptimize();

            assertFalse(simulator.isDictionaryMemoryFull());
            simulator.reset();
        }
    }

    @Test
    public void testIsDictionaryUsefulCheckRequired()
    {
        TestDictionaryColumn directColumn = directColumn(1024, 1);
        int dictionaryColumnSizeCheckBytes = megabytes(1);
        int dictionaryUsefulCheckPerChunkFrequency = 3;
        DataSimulator simulator = new DataSimulator(megabytes(100),
                megabytes(200),
                10_000_000,
                megabytes(100),
                0,
                DICTIONARY_ALMOST_FULL_MEMORY_RANGE,
                dictionaryUsefulCheckPerChunkFrequency,
                dictionaryColumnSizeCheckBytes,
                directColumn);

        for (int loop = 0; loop < 3; loop++) {
            assertFalse(simulator.isUsefulCheckRequired(dictionaryColumnSizeCheckBytes + 1));
            assertFalse(simulator.isUsefulCheckRequired(dictionaryColumnSizeCheckBytes));
            // Calling with 1 byte less should not increment the counter
            assertFalse(simulator.isUsefulCheckRequired(dictionaryColumnSizeCheckBytes - 1));

            // 3rd time, it should return true as dictionaryUsefulCheckPerChunkFrequency is set to 3.
            assertTrue(simulator.isUsefulCheckRequired(dictionaryColumnSizeCheckBytes));
        }
    }

    private static int megabytes(int size)
    {
        return toIntExact(new DataSize(size, Unit.MEGABYTE).toBytes());
    }

    private static class DataSimulator
    {
        private final int stripeMaxBytes;
        private final int stripeMaxRowCount;

        private final int otherColumnsBytesPerRow;
        private final Set<TestDictionaryColumn> dictionaryColumns;

        private final DictionaryCompressionOptimizer optimizer;

        private int rowCount;

        public DataSimulator(
                int stripeMinBytes,
                int stripeMaxBytes,
                int stripeMaxRowCount,
                int dictionaryMemoryMaxBytes,
                int otherColumnsBytesPerRow,
                TestDictionaryColumn... dictionaryColumns)
        {
            this(stripeMinBytes,
                    stripeMaxBytes,
                    stripeMaxRowCount,
                    dictionaryMemoryMaxBytes,
                    otherColumnsBytesPerRow,
                    DICTIONARY_ALMOST_FULL_MEMORY_RANGE,
                    dictionaryMemoryMaxBytes,
                    0,
                    dictionaryColumns);
        }

        public DataSimulator(
                int stripeMinBytes,
                int stripeMaxBytes,
                int stripeMaxRowCount,
                int dictionaryMemoryMaxBytes,
                int otherColumnsBytesPerRow,
                int dictionaryAlmostFullRangeBytes,
                int dictionaryUsefulCheckPerChunkFrequency,
                int dictionaryUsefulCheckColumnSizeBytes,
                TestDictionaryColumn... dictionaryColumns)
        {
            this.stripeMaxBytes = stripeMaxBytes;
            this.stripeMaxRowCount = stripeMaxRowCount;
            this.otherColumnsBytesPerRow = otherColumnsBytesPerRow;
            this.dictionaryColumns = ImmutableSet.copyOf(dictionaryColumns);

            this.optimizer = new DictionaryCompressionOptimizer(
                    this.dictionaryColumns,
                    stripeMinBytes,
                    stripeMaxBytes,
                    stripeMaxRowCount,
                    dictionaryMemoryMaxBytes,
                    dictionaryAlmostFullRangeBytes,
                    dictionaryUsefulCheckColumnSizeBytes,
                    dictionaryUsefulCheckPerChunkFrequency);
        }

        public void advanceToNextStateChange()
        {
            List<Boolean> directColumnFlags = getDirectColumnFlags();
            while (!optimizer.isFull(getBufferedBytes()) && getBufferedBytes() < stripeMaxBytes && getRowCount() < stripeMaxRowCount && directColumnFlags.equals(getDirectColumnFlags())) {
                rowCount += CHUNK_ROW_COUNT;
                for (TestDictionaryColumn dictionaryColumn : dictionaryColumns) {
                    dictionaryColumn.advanceTo(rowCount);
                }
                optimizer.optimize(toIntExact(getBufferedBytes()), getRowCount());
            }
        }

        public boolean isDictionaryMemoryFull()
        {
            return optimizer.isFull(getBufferedBytes());
        }

        public void finalOptimize()
        {
            optimizer.finalOptimize(toIntExact(getBufferedBytes()));
        }

        private List<Boolean> getDirectColumnFlags()
        {
            return dictionaryColumns.stream()
                    .map(TestDictionaryColumn::isDirectEncoded)
                    .collect(Collectors.toList());
        }

        public void reset()
        {
            rowCount = 0;
            optimizer.reset();
            for (TestDictionaryColumn dictionaryColumn : dictionaryColumns) {
                dictionaryColumn.reset();
            }
        }

        public long getBufferedBytes()
        {
            return (long) rowCount * otherColumnsBytesPerRow +
                    dictionaryColumns.stream()
                            .mapToLong(TestDictionaryColumn::getBufferedBytes)
                            .sum();
        }

        public int getRowCount()
        {
            return rowCount;
        }

        public boolean isUsefulCheckRequired(int dictionaryMemoryBytes)
        {
            return optimizer.isUsefulCheckRequired(dictionaryMemoryBytes);
        }
    }

    private static TestDictionaryColumn directColumn(int bytesPerEntry, double uniquePercentage)
    {
        return new TestDictionaryColumn(bytesPerEntry, uniquePercentage, OptionalInt.empty(), 1, 0);
    }

    private static TestDictionaryColumn dictionaryColumn(int bytesPerEntry, int maxDictionaryEntries)
    {
        return dictionaryColumn(bytesPerEntry, maxDictionaryEntries, 0.5);
    }

    private static TestDictionaryColumn dictionaryColumn(int bytesPerEntry, int maxDictionaryEntries, double uniquePercentage)
    {
        return new TestDictionaryColumn(bytesPerEntry, uniquePercentage, OptionalInt.of(maxDictionaryEntries), 1, 0);
    }

    private static class TestDictionaryColumn
            implements DictionaryColumn
    {
        private final int bytesPerEntry;

        private final double uniquePercentage;
        private final OptionalInt maxDictionaryEntries;

        private final double valuesPerRow;
        private final double nullRate;

        private int rowCount;
        private boolean direct;

        private TestDictionaryColumn(int bytesPerEntry, double uniquePercentage, OptionalInt maxDictionaryEntries, double valuesPerRow, double nullRate)
        {
            checkArgument(bytesPerEntry >= 0, "bytesPerEntry is negative");
            this.bytesPerEntry = bytesPerEntry;

            checkArgument(uniquePercentage >= 0 && uniquePercentage <= 1, "bytesPerEntry must be between 0 and 1");
            this.uniquePercentage = uniquePercentage;

            this.maxDictionaryEntries = requireNonNull(maxDictionaryEntries, "maxDictionaryEntries is null");
            maxDictionaryEntries.ifPresent(value -> checkArgument(value >= 0, "maxDictionaryEntries is negative"));

            checkArgument(valuesPerRow >= 0, "valuesPerRow is negative");
            this.valuesPerRow = valuesPerRow;

            checkArgument(nullRate >= 0 && nullRate <= 1, "nullRate must be between 0 and 1");
            this.nullRate = nullRate;
        }

        public void reset()
        {
            rowCount = 0;
            direct = false;
        }

        public void advanceTo(int rowCount)
        {
            assertTrue(rowCount >= this.rowCount);
            this.rowCount = rowCount;
        }

        public long getBufferedBytes()
        {
            if (direct) {
                return (long) (rowCount * valuesPerRow * bytesPerEntry);
            }
            int dictionaryEntries = getDictionaryEntries();
            int bytesPerValue = estimateIndexBytesPerValue(dictionaryEntries);
            return (dictionaryEntries * bytesPerEntry) + (getNonNullValueCount() * bytesPerValue);
        }

        @Override
        public long getValueCount()
        {
            return (long) (rowCount * valuesPerRow);
        }

        @Override
        public long getNonNullValueCount()
        {
            return (long) (getValueCount() * (1 - nullRate));
        }

        @Override
        public long getRawBytes()
        {
            return (long) bytesPerEntry * getNonNullValueCount();
        }

        @Override
        public int getDictionaryEntries()
        {
            int dictionaryEntries = (int) (getNonNullValueCount() * uniquePercentage);
            return min(dictionaryEntries, maxDictionaryEntries.orElse(dictionaryEntries));
        }

        @Override
        public int getDictionaryBytes()
        {
            return getDictionaryEntries() * bytesPerEntry;
        }

        @Override
        public int getIndexBytes()
        {
            return toIntExact(estimateIndexBytesPerValue(getDictionaryEntries()) * getNonNullValueCount());
        }

        @Override
        public OptionalInt tryConvertToDirect(int maxDirectBytes)
        {
            assertFalse(direct);
            long directBytes = (long) (rowCount * valuesPerRow * bytesPerEntry);
            if (directBytes <= maxDirectBytes) {
                direct = true;
                return OptionalInt.of(toIntExact(directBytes));
            }
            else {
                return OptionalInt.empty();
            }
        }

        @Override
        public boolean isDirectEncoded()
        {
            return direct;
        }
    }
}
