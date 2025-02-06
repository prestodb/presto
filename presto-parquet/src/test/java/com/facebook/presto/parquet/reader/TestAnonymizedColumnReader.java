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
package com.facebook.presto.parquet.reader;

import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.BlockBuilder;
import com.facebook.presto.common.type.VarcharType;
import com.facebook.presto.parquet.PrimitiveField;
import com.facebook.presto.parquet.RichColumnDescriptor;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import org.apache.parquet.anonymization.AnonymizationManager;
import org.apache.parquet.hadoop.metadata.ColumnPath;
import org.apache.parquet.schema.PrimitiveType;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BINARY;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT64;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertThrows;

public class TestAnonymizedColumnReader
{
    private PrimitiveField binaryField;
    private PrimitiveField longField;

    private FakePrimitiveColumnReader delegateReader;
    private FakeAnonymizationManager anonymizationManager;

    @BeforeMethod
    public void setUp()
    {
        delegateReader = new FakePrimitiveColumnReader();
        anonymizationManager = new FakeAnonymizationManager();

        PrimitiveType binaryPrimitiveType = new PrimitiveType(
                PrimitiveType.Repetition.OPTIONAL,
                BINARY,
                "string_col");

        PrimitiveType longPrimitiveType = new PrimitiveType(
                PrimitiveType.Repetition.OPTIONAL,
                INT64,
                "long_col");

        binaryField = new PrimitiveField(
                VarcharType.VARCHAR,
                0,
                0,
                false,
                new RichColumnDescriptor(
                        new org.apache.parquet.column.ColumnDescriptor(
                                new String[]{"string_col"}, BINARY, 0, 0),
                        binaryPrimitiveType),
                0);

        longField = new PrimitiveField(
                VarcharType.VARCHAR,
                0,
                0,
                false,
                new RichColumnDescriptor(
                        new org.apache.parquet.column.ColumnDescriptor(
                                new String[]{"long_col"}, INT64, 0, 0),
                        longPrimitiveType),
                1);
    }

    /**
     * Test that anonymization occurs for a BINARY column
     * when the manager indicates it should be anonymized.
     */
    @Test
    public void testAnonymizeStringColumn() throws IOException
    {
        // Indicate the manager should anonymize "string_col"
        anonymizationManager.setShouldAnonymize("string_col", true);
        // Provide a simple transformation for demonstration
        anonymizationManager.setAnonymizeMapping("string_col", "alice", "masked_alice");
        anonymizationManager.setAnonymizeMapping("string_col", "bob", "masked_bob");

        // Build a block with two string values
        BlockBuilder blockBuilder = VarcharType.VARCHAR.createBlockBuilder(null, 2);
        VarcharType.VARCHAR.writeSlice(blockBuilder, Slices.utf8Slice("alice"));
        VarcharType.VARCHAR.writeSlice(blockBuilder, Slices.utf8Slice("bob"));
        Block inputBlock = blockBuilder.build();

        ColumnChunk delegateChunk = new ColumnChunk(inputBlock, new int[2], new int[2]);
        delegateReader.setChunkToReturn(delegateChunk);

        AnonymizedColumnReader anonymizedReader = new AnonymizedColumnReader(delegateReader, anonymizationManager);
        ColumnChunk resultChunk = anonymizedReader.read(binaryField);

        assertEquals(delegateReader.getReadCallCount(), 1);

        Block resultBlock = resultChunk.getBlock();
        assertEquals(resultBlock.getPositionCount(), 2);
        Slice slice0 = VarcharType.VARCHAR.getSlice(resultBlock, 0);
        Slice slice1 = VarcharType.VARCHAR.getSlice(resultBlock, 1);

        assertEquals(slice0.toStringUtf8(), "masked_alice");
        assertEquals(slice1.toStringUtf8(), "masked_bob");
    }

    /**
     * Test that if manager says "do not anonymize," the data is unchanged.
     */
    @Test
    public void testNoAnonymizationOnStringColumn() throws IOException
    {
        // Mark "string_col" as not to be anonymized
        anonymizationManager.setShouldAnonymize("string_col", false);

        // Build a block with one string
        BlockBuilder blockBuilder = VarcharType.VARCHAR.createBlockBuilder(null, 1);
        VarcharType.VARCHAR.writeSlice(blockBuilder, Slices.utf8Slice("some_value"));
        Block inputBlock = blockBuilder.build();

        ColumnChunk delegateChunk = new ColumnChunk(inputBlock, new int[1], new int[1]);
        delegateReader.setChunkToReturn(delegateChunk);
        AnonymizedColumnReader anonymizedReader = new AnonymizedColumnReader(delegateReader, anonymizationManager);
        ColumnChunk resultChunk = anonymizedReader.read(binaryField);

        assertEquals(delegateReader.getReadCallCount(), 1);

        // Check data is intact
        Block resultBlock = resultChunk.getBlock();
        assertEquals(resultBlock.getPositionCount(), 1);
        Slice slice = VarcharType.VARCHAR.getSlice(resultBlock, 0);
        assertEquals(slice.toStringUtf8(), "some_value");
    }

    /**
     * Test that reading a non-BINARY field that "should be anonymized"
     * triggers an exception in the current implementation.
     */
    @Test
    public void testAnonymizeUnsupportedTypeThrowsException() throws IOException
    {
        // Indicate we want to anonymize "long_col"
        anonymizationManager.setShouldAnonymize("long_col", true);

        // Create a block with one row, for demonstration
        BlockBuilder blockBuilder = VarcharType.VARCHAR.createBlockBuilder(null, 1);
        VarcharType.VARCHAR.writeSlice(blockBuilder, Slices.utf8Slice("12345"));
        Block inputBlock = blockBuilder.build();

        // Provide to delegate
        ColumnChunk delegateChunk = new ColumnChunk(inputBlock, new int[1], new int[1]);
        delegateReader.setChunkToReturn(delegateChunk);

        // Wrap in anonymized reader
        AnonymizedColumnReader anonymizedReader = new AnonymizedColumnReader(delegateReader, anonymizationManager);

        // We expect an UnsupportedOperationException because AnonymizedColumnReader
        // currently only supports BINARY.
        assertThrows(UnsupportedOperationException.class, () -> anonymizedReader.read(longField));
    }

    /**
     * Fake implementation of PrimitiveColumnReader that lets us
     * specify a single ColumnChunk to return and track how many times
     * read(...) has been called.
     */
    private static class FakePrimitiveColumnReader
            implements PrimitiveColumnReader
    {
        private ColumnChunk chunkToReturn;
        private int readCallCount;

        public void setChunkToReturn(ColumnChunk chunk)
        {
            this.chunkToReturn = chunk;
        }

        public int getReadCallCount()
        {
            return readCallCount;
        }

        @Override
        public ColumnChunk read(PrimitiveField field) throws IOException
        {
            readCallCount++;
            return chunkToReturn;
        }
    }

    /**
     * A simple test/dummy AnonymizationManager that holds a map
     * of (column -> shouldAnonymize), plus an optional map of
     * (column + originalValue -> anonymizedValue).
     */
    private static class FakeAnonymizationManager
            implements AnonymizationManager
    {
        // For each column name, store whether it should be anonymized
        private final Map<String, Boolean> anonymizeMap = new HashMap<>();
        // For each (columnName + originalValue), store the anonymized text
        private final Map<String, Map<String, String>> columnValueMapping = new HashMap<>();

        public void setShouldAnonymize(String columnName, boolean shouldAnonymize)
        {
            anonymizeMap.put(columnName, shouldAnonymize);
        }

        public void setAnonymizeMapping(String columnName, String original, String anonymized)
        {
            columnValueMapping.computeIfAbsent(columnName, k -> new HashMap<>()).put(original, anonymized);
        }

        @Override
        public boolean shouldAnonymize(ColumnPath columnPath)
        {
            // e.g., columnPath.toDotString() => "string_col" / "long_col"
            return anonymizeMap.getOrDefault(columnPath.toDotString(), false);
        }

        @Override
        public String anonymize(ColumnPath columnPath, String value)
        {
            String columnName = columnPath.toDotString();
            if (!columnValueMapping.containsKey(columnName)) {
                // If no mapping is defined, just do a trivial replacement
                return "MASKED_" + value;
            }
            return columnValueMapping.get(columnName).getOrDefault(value, "MASKED_" + value);
        }
    }
}
