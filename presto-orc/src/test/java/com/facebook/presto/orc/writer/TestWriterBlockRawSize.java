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
package com.facebook.presto.orc.writer;

import com.facebook.presto.common.Page;
import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.BlockBuilder;
import com.facebook.presto.common.block.RowBlock;
import com.facebook.presto.common.type.ArrayType;
import com.facebook.presto.common.type.FixedWidthType;
import com.facebook.presto.common.type.RowType;
import com.facebook.presto.common.type.TimestampType;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.orc.ColumnWriterOptions;
import com.facebook.presto.orc.DwrfEncryptionInfo;
import com.facebook.presto.orc.FileOrcDataSource;
import com.facebook.presto.orc.OrcEncoding;
import com.facebook.presto.orc.OrcTester;
import com.facebook.presto.orc.OrcWriter;
import com.facebook.presto.orc.OrcWriterOptions;
import com.facebook.presto.orc.OrcWriterStats;
import com.facebook.presto.orc.TempFile;
import com.facebook.presto.orc.metadata.Footer;
import com.facebook.presto.orc.metadata.OrcType;
import com.facebook.presto.orc.metadata.StripeInformation;
import com.google.common.collect.ImmutableList;
import io.airlift.units.DataSize;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.OptionalLong;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.BooleanType.BOOLEAN;
import static com.facebook.presto.common.type.DoubleType.DOUBLE;
import static com.facebook.presto.common.type.IntegerType.INTEGER;
import static com.facebook.presto.common.type.RealType.REAL;
import static com.facebook.presto.common.type.SmallintType.SMALLINT;
import static com.facebook.presto.common.type.TinyintType.TINYINT;
import static com.facebook.presto.common.type.VarbinaryType.VARBINARY;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static com.facebook.presto.orc.OrcEncoding.DWRF;
import static com.facebook.presto.orc.OrcTester.HIVE_STORAGE_TIME_ZONE;
import static com.facebook.presto.orc.OrcTester.createOrcWriter;
import static com.facebook.presto.orc.TestOrcMapNullKey.createMapType;
import static com.facebook.presto.orc.metadata.CompressionKind.ZSTD;
import static io.airlift.slice.Slices.utf8Slice;
import static io.airlift.units.DataSize.Unit.KILOBYTE;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class TestWriterBlockRawSize
{
    private static final int NUM_ELEMENTS = 100;
    private static final int COLUMN_INDEX = 1;
    private static final ColumnWriterOptions COLUMN_WRITER_OPTIONS = ColumnWriterOptions.builder()
            .setCompressionKind(ZSTD)
            .setCompressionMaxBufferSize(new DataSize(256, KILOBYTE))
            .setIntegerDictionaryEncodingEnabled(true)
            .build();

    private static ColumnWriter createColumnWriter(Type type)
    {
        List<OrcType> orcTypes = OrcType.createOrcRowType(0, ImmutableList.of("test_size_col"), ImmutableList.of(type));
        ColumnWriter columnWriter = ColumnWriters.createColumnWriter(
                COLUMN_INDEX,
                orcTypes,
                type,
                COLUMN_WRITER_OPTIONS,
                DWRF,
                HIVE_STORAGE_TIME_ZONE,
                DwrfEncryptionInfo.UNENCRYPTED,
                DWRF.createMetadataWriter());
        columnWriter.beginRowGroup();
        return columnWriter;
    }

    @DataProvider(name = "IntegerTypes")
    public Object[][] integerTypesProvider()
    {
        return new Object[][] {{TINYINT}, {SMALLINT}, {INTEGER}, {BIGINT}};
    }

    @DataProvider(name = "StringTypes")
    public Object[][] stringTypesProvider()
    {
        return new Object[][] {{VARBINARY}, {VARCHAR}};
    }

    @DataProvider(name = "FractionalTypes")
    public Object[][] fractionalTypesProvider()
    {
        return new Object[][] {{REAL}, {DOUBLE}};
    }

    @Test(dataProvider = "IntegerTypes")
    public void testIntegerWriter(Type type)
    {
        BlockBuilder blockBuilder = type.createBlockBuilder(null, NUM_ELEMENTS * 2);
        for (int i = 0; i < NUM_ELEMENTS; i++) {
            blockBuilder.appendNull();
            type.writeLong(blockBuilder, i);
        }
        Block block = blockBuilder.build();

        ColumnWriter columnWriter = createColumnWriter(type);
        long rawSize = columnWriter.writeBlock(block);
        long expectedSize = NUM_ELEMENTS * (1 + ((FixedWidthType) type).getFixedSize());
        assertEquals(rawSize, expectedSize);

        verifyDictionaryColumnWriter(expectedSize, block, columnWriter);
    }

    @Test(dataProvider = "StringTypes")
    public void testStringType(Type type)
    {
        BlockBuilder blockBuilder = type.createBlockBuilder(null, NUM_ELEMENTS * 2);
        long expectedSize = 0;
        for (int i = 0; i < NUM_ELEMENTS; i++) {
            blockBuilder.appendNull();
            String value = Integer.toString(i);
            type.writeSlice(blockBuilder, utf8Slice(value));
            expectedSize += (1 + value.length());
        }
        Block block = blockBuilder.build();

        ColumnWriter columnWriter = createColumnWriter(type);
        long rawSize = columnWriter.writeBlock(block);
        assertEquals(rawSize, expectedSize);

        verifyDictionaryColumnWriter(expectedSize, block, columnWriter);
    }

    private void verifyDictionaryColumnWriter(long expectedSize, Block block, ColumnWriter columnWriter)
    {
        if (columnWriter instanceof DictionaryColumnWriter) {
            DictionaryColumnWriter dictionaryColumnWriter = (DictionaryColumnWriter) columnWriter;
            OptionalInt convertToDirect = dictionaryColumnWriter.tryConvertToDirect(Integer.MAX_VALUE);
            assertTrue(convertToDirect.isPresent());
            long rawSize = dictionaryColumnWriter.writeBlock(block);
            assertEquals(rawSize, expectedSize);
        }
    }

    @Test
    public void testBooleanWriter()
    {
        ColumnWriter columnWriter = createColumnWriter(BOOLEAN);
        BlockBuilder blockBuilder = BOOLEAN.createBlockBuilder(null, NUM_ELEMENTS * 2);
        for (int i = 0; i < NUM_ELEMENTS; i++) {
            blockBuilder.appendNull();
            BOOLEAN.writeBoolean(blockBuilder, i % 2 == 0);
        }

        long rawSize = columnWriter.writeBlock(blockBuilder.build());
        long expectedSize = NUM_ELEMENTS * 2;
        assertEquals(rawSize, expectedSize);
    }

    @Test(dataProvider = "FractionalTypes")
    public void testFractionalTypes(Type type)
    {
        ColumnWriter columnWriter = createColumnWriter(type);
        BlockBuilder blockBuilder = type.createBlockBuilder(null, NUM_ELEMENTS * 2);
        for (int i = 0; i < NUM_ELEMENTS; i++) {
            blockBuilder.appendNull();
            if (type == REAL) {
                type.writeLong(blockBuilder, Float.floatToIntBits(i));
            }
            else {
                type.writeDouble(blockBuilder, (float) i);
            }
        }

        long rawSize = columnWriter.writeBlock(blockBuilder.build());
        long expectedSize = NUM_ELEMENTS * (1 + ((FixedWidthType) type).getFixedSize());
        assertEquals(rawSize, expectedSize);
    }

    @Test
    public void testArrayType()
    {
        Type elementType = INTEGER;
        Type arrayType = new ArrayType(elementType);
        ColumnWriter columnWriter = createColumnWriter(arrayType);

        BlockBuilder blockBuilder = arrayType.createBlockBuilder(null, NUM_ELEMENTS * 2);
        int totalChildElements = 0;
        for (int i = 0; i < NUM_ELEMENTS; i++) {
            blockBuilder.appendNull();
            BlockBuilder elementBlockBuilder = blockBuilder.beginBlockEntry();
            for (int j = 0; j < i; j++) {
                elementType.writeLong(elementBlockBuilder, j);
            }
            blockBuilder.closeEntry();
            totalChildElements += i;
        }

        long rawSize = columnWriter.writeBlock(blockBuilder.build());
        long expectedSize = NUM_ELEMENTS + (totalChildElements * ((FixedWidthType) elementType).getFixedSize());
        assertEquals(rawSize, expectedSize);
    }

    @Test
    public void testMapType()
    {
        Type elementType = INTEGER;
        Type arrayType = createMapType(elementType, elementType);
        ColumnWriter columnWriter = createColumnWriter(arrayType);

        BlockBuilder blockBuilder = arrayType.createBlockBuilder(null, NUM_ELEMENTS * 2);
        int totalChildElements = 0;
        for (int i = 0; i < NUM_ELEMENTS; i++) {
            blockBuilder.appendNull();
            BlockBuilder elementBlockBuilder = blockBuilder.beginBlockEntry();
            for (int j = 0; j < i; j++) {
                elementType.writeLong(elementBlockBuilder, j); // key
                elementType.writeLong(elementBlockBuilder, j); // value
            }
            blockBuilder.closeEntry();
            totalChildElements += i;
        }

        long rawSize = columnWriter.writeBlock(blockBuilder.build());
        long expectedSize = NUM_ELEMENTS + (totalChildElements * 2 * ((FixedWidthType) elementType).getFixedSize());
        assertEquals(rawSize, expectedSize);
    }

    @Test
    public void testRowType()
    {
        Type elementType = INTEGER;
        Type rowType = RowType.anonymous(ImmutableList.of(elementType, elementType));
        ColumnWriter columnWriter = createColumnWriter(rowType);

        BlockBuilder blockBuilder = elementType.createBlockBuilder(null, NUM_ELEMENTS);
        boolean[] isNull = new boolean[NUM_ELEMENTS * 2];
        for (int i = 0; i < NUM_ELEMENTS; i++) {
            elementType.writeLong(blockBuilder, i);
            isNull[i * 2] = true;
        }

        Block elementBlock = blockBuilder.build();
        Block[] elementBlocks = new Block[] {elementBlock, elementBlock};
        Block rowBlock = RowBlock.fromFieldBlocks(NUM_ELEMENTS * 2, Optional.of(isNull), elementBlocks);
        long rawSize = columnWriter.writeBlock(rowBlock);
        long expectedSize = NUM_ELEMENTS + (NUM_ELEMENTS * 2 * ((FixedWidthType) elementType).getFixedSize());
        assertEquals(rawSize, expectedSize);
    }

    @Test
    public void testTimestampType()
    {
        Type timestampType = TimestampType.TIMESTAMP;
        ColumnWriter columnWriter = createColumnWriter(timestampType);

        BlockBuilder blockBuilder = timestampType.createBlockBuilder(null, NUM_ELEMENTS * 2);
        for (int i = 0; i < NUM_ELEMENTS; i++) {
            blockBuilder.appendNull();
            timestampType.writeLong(blockBuilder, i);
        }

        long rawSize = columnWriter.writeBlock(blockBuilder.build());
        long expectedSize = NUM_ELEMENTS * (1 + Long.BYTES + Integer.BYTES);
        assertEquals(rawSize, expectedSize);
    }

    @Test
    public void testFileMetadataRawSize()
            throws IOException
    {
        Type type = INTEGER;
        List<Type> types = ImmutableList.of(type);

        int numBlocksPerRowGroup = 3;
        int numBlocksPerStripe = numBlocksPerRowGroup * 5;
        int numStripes = 4;
        int numBlocksPerFile = numBlocksPerStripe * numStripes + 1;

        BlockBuilder blockBuilder = type.createBlockBuilder(null, NUM_ELEMENTS * 2);
        for (int i = 0; i < NUM_ELEMENTS; i++) {
            blockBuilder.appendNull();
            type.writeLong(blockBuilder, i);
        }
        long blockRawSize = ((FixedWidthType) type).getFixedSize() * NUM_ELEMENTS + NUM_ELEMENTS;
        Block block = blockBuilder.build();
        Block[] blocks = new Block[] {block};

        OrcWriterOptions writerOptions = OrcWriterOptions.builder()
                .withRowGroupMaxRowCount(block.getPositionCount() * numBlocksPerRowGroup)
                .withStripeMaxRowCount(block.getPositionCount() * numBlocksPerStripe)
                .build();

        for (OrcEncoding encoding : OrcEncoding.values()) {
            try (TempFile tempFile = new TempFile()) {
                OrcWriter writer = createOrcWriter(tempFile.getFile(), encoding, ZSTD, Optional.empty(), types, writerOptions, new OrcWriterStats());
                for (int i = 0; i < numBlocksPerFile; i++) {
                    writer.write(new Page(blocks));
                }
                writer.close();
                writer.validate(new FileOrcDataSource(
                        tempFile.getFile(),
                        new DataSize(1, MEGABYTE),
                        new DataSize(1, MEGABYTE),
                        new DataSize(1, MEGABYTE),
                        true));

                Footer footer = OrcTester.getFileMetadata(tempFile.getFile(), encoding).getFooter();
                verifyValue(encoding, footer.getRawSize(), blockRawSize * numBlocksPerFile);
                assertEquals(footer.getStripes().size(), numStripes + 1);

                int numBlocksRemaining = numBlocksPerFile;
                for (StripeInformation stripeInfo : footer.getStripes()) {
                    int numBlocksInStripe = Math.min(numBlocksRemaining, numBlocksPerStripe);
                    verifyValue(encoding, stripeInfo.getRawDataSize(), blockRawSize * numBlocksInStripe);
                    numBlocksRemaining -= numBlocksInStripe;
                }
            }
        }
    }

    private void verifyValue(OrcEncoding encoding, OptionalLong actual, long expected)
    {
        if (DWRF == encoding) {
            assertEquals(actual, OptionalLong.of(expected));
        }
        else {
            assertEquals(actual, OptionalLong.empty());
        }
    }
}
