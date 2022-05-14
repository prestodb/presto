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

import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.BlockBuilder;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.orc.ColumnWriterOptions;
import com.facebook.presto.orc.metadata.CompressionKind;
import com.facebook.presto.orc.metadata.OrcType;
import com.facebook.presto.orc.stream.StreamDataOutput;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slices;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.List;
import java.util.Optional;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.BooleanType.BOOLEAN;
import static com.facebook.presto.common.type.DoubleType.DOUBLE;
import static com.facebook.presto.common.type.IntegerType.INTEGER;
import static com.facebook.presto.common.type.RealType.REAL;
import static com.facebook.presto.common.type.SmallintType.SMALLINT;
import static com.facebook.presto.common.type.TimestampType.TIMESTAMP;
import static com.facebook.presto.common.type.TinyintType.TINYINT;
import static com.facebook.presto.common.type.VarbinaryType.VARBINARY;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static com.facebook.presto.orc.DwrfEncryptionInfo.UNENCRYPTED;
import static com.facebook.presto.orc.OrcEncoding.DWRF;
import static com.facebook.presto.orc.OrcTester.arrayType;
import static com.facebook.presto.orc.OrcTester.mapType;
import static com.facebook.presto.orc.OrcTester.rowType;
import static com.facebook.presto.orc.metadata.OrcType.toOrcType;
import static com.facebook.presto.orc.writer.ColumnWriters.createColumnWriter;
import static org.joda.time.DateTimeZone.UTC;
import static org.testng.Assert.assertEquals;

public class TestColumnWriters
{
    private static final int SEQUENCE = 98005;

    @DataProvider(name = "dataForSequenceIdTest")
    public Object[][] dataForSequenceIdTest()
    {
        Block stringBlock = VARCHAR.createBlockBuilder(null, 2)
                .appendNull()
                .writeBytes(Slices.utf8Slice("123456789"), 0, 9)
                .closeEntry()
                .build();

        Type mapType = mapType(INTEGER, INTEGER);
        BlockBuilder mapBlockBuilder = mapType.createBlockBuilder(null, 3);
        mapBlockBuilder.appendNull();
        mapBlockBuilder.beginBlockEntry().writeInt(1).closeEntry().writeInt(2).closeEntry();
        mapBlockBuilder.closeEntry();
        Block mapBlock = mapBlockBuilder.build();

        Type arrayType = arrayType(INTEGER);
        BlockBuilder arrayBlockBuilder = arrayType.createBlockBuilder(null, 2);
        arrayBlockBuilder.appendNull();
        arrayBlockBuilder.beginBlockEntry().writeInt(1).writeInt(2);
        arrayBlockBuilder.closeEntry();
        arrayBlockBuilder.beginBlockEntry().appendNull();
        arrayBlockBuilder.closeEntry();
        Block arrayBlock = arrayBlockBuilder.build();

        Type rowType = rowType(INTEGER);
        BlockBuilder rowBlockBuilder = rowType.createBlockBuilder(null, 2);
        rowBlockBuilder.appendNull();
        rowBlockBuilder.beginBlockEntry().writeInt(1).closeEntry();
        rowBlockBuilder.closeEntry();
        rowBlockBuilder.beginBlockEntry().appendNull();
        rowBlockBuilder.closeEntry();
        Block rowBlock = rowBlockBuilder.build();

        return new Object[][] {
                {toOrcTypes(BOOLEAN), BOOLEAN, BOOLEAN.createFixedSizeBlockBuilder(2).appendNull().writeByte(1).build()},
                {toOrcTypes(TINYINT), TINYINT, TINYINT.createFixedSizeBlockBuilder(2).appendNull().writeByte(1).build()},
                {toOrcTypes(SMALLINT), SMALLINT, SMALLINT.createFixedSizeBlockBuilder(2).appendNull().writeShort(1).build()},
                {toOrcTypes(INTEGER), INTEGER, INTEGER.createFixedSizeBlockBuilder(2).appendNull().writeInt(1).build()},
                {toOrcTypes(BIGINT), BIGINT, BIGINT.createFixedSizeBlockBuilder(2).appendNull().writeLong(1).build()},
                {toOrcTypes(DOUBLE), DOUBLE, DOUBLE.createFixedSizeBlockBuilder(2).appendNull().writeLong(1).build()},
                {toOrcTypes(REAL), REAL, REAL.createFixedSizeBlockBuilder(2).appendNull().writeInt(1).build()},
                {toOrcTypes(TIMESTAMP), TIMESTAMP, TIMESTAMP.createFixedSizeBlockBuilder(2).appendNull().writeLong(1).build()},
                {toOrcTypes(VARCHAR), VARCHAR, stringBlock},
                {toOrcTypes(VARBINARY), VARBINARY, stringBlock},
                {toOrcTypes(arrayType), arrayType, arrayBlock},
                {toOrcTypes(mapType), mapType, mapBlock},
                {toOrcTypes(rowType), rowType, rowBlock},
        };
    }

    @Test(dataProvider = "dataForSequenceIdTest")
    public void testSequenceIdPassedAllColumnWriters(List<OrcType> orcTypes, Type type, Block block)
            throws IOException
    {
        ColumnWriterOptions columnWriterOptions = ColumnWriterOptions.builder()
                .setCompressionKind(CompressionKind.ZLIB)
                .build();
        int nodeId = 0;
        ColumnWriter columnWriter = createColumnWriter(
                nodeId,
                SEQUENCE,
                orcTypes,
                type,
                columnWriterOptions,
                DWRF,
                UTC,
                UNENCRYPTED,
                DWRF.createMetadataWriter());

        columnWriter.beginRowGroup();
        columnWriter.writeBlock(block);
        columnWriter.finishRowGroup();
        columnWriter.close();

        ImmutableList<StreamDataOutput> streams = ImmutableList.<StreamDataOutput>builder()
                .addAll(columnWriter.getIndexStreams(Optional.empty()))
                .addAll(columnWriter.getDataStreams())
                .build();

        for (StreamDataOutput stream : streams) {
            assertEquals(stream.getStream().getSequence(), SEQUENCE);
        }
    }

    private static List<OrcType> toOrcTypes(Type type)
    {
        return toOrcType(0, type);
    }
}
