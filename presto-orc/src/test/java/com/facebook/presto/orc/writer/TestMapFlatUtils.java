
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
import com.google.common.collect.ImmutableList;

import java.util.List;

import static com.facebook.presto.common.type.IntegerType.INTEGER;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static com.facebook.presto.orc.DwrfEncryptionInfo.UNENCRYPTED;
import static com.facebook.presto.orc.OrcEncoding.DWRF;
import static org.joda.time.DateTimeZone.UTC;

public class TestMapFlatUtils
{
    private static final Integer BATCH_ROWS = 10000;

    private TestMapFlatUtils() {};

    public static FlatMapValueWriterFactory createFlatMapValueWriterFactory(Type valueType)
    {
        List<OrcType> orcTypes = OrcType.createOrcRowType(
                0,
                ImmutableList.of("firstColumn"),
                ImmutableList.of(valueType));
        ColumnWriterOptions columnWriterOptions = ColumnWriterOptions.builder()
                .setCompressionKind(CompressionKind.NONE)
                .build();
        return new FlatMapValueWriterFactory(
                        1,
                        orcTypes,
                        valueType,
                        columnWriterOptions,
                        DWRF,
                        UTC,
                        UNENCRYPTED,
                        DWRF.createMetadataWriter());
    }

    public static FlatMapValueColumnWriter createFlatMapValueColumnWriter(Type valueType)
    {
        FlatMapValueWriterFactory flatMapValueWriterFactory = createFlatMapValueWriterFactory(valueType);
        return flatMapValueWriterFactory.getFlatMapValueColumnWriter(1);
    }

    public static Block[] createNumericDataBlock(Type type, List<Long> values)
    {
        int row = 0;
        int batchId = 0;
        Block[] blocks = new Block[values.size() / BATCH_ROWS + 1];
        while (row < values.size()) {
            int end = Math.min(row + BATCH_ROWS, values.size());
            BlockBuilder blockBuilder = type.createBlockBuilder(null, BATCH_ROWS);
            while (row < end) {
                Long value = values.get(row++);
                if (value == null) {
                    blockBuilder.appendNull();
                }
                else {
                    type.writeLong(blockBuilder, value.longValue());
                }
            }
            blocks[batchId] = blockBuilder.build();
            batchId++;
        }
        return blocks;
    }

    public static Block[] createStringDataBlock(Type type, List<String> values)
    {
        int row = 0;
        int batchId = 0;
        Block[] blocks = new Block[values.size() / BATCH_ROWS + 1];
        while (row < values.size()) {
            int end = Math.min(row + BATCH_ROWS, values.size());
            BlockBuilder blockBuilder = type.createBlockBuilder(null, BATCH_ROWS);
            while (row < end) {
                String value = values.get(row++);
                if (value == null) {
                    blockBuilder.appendNull();
                }
                else {
                    VARCHAR.writeString(blockBuilder, value);
                }
            }
            blocks[batchId] = blockBuilder.build();
            batchId++;
        }
        return blocks;
    }

    public static Block[] createIntegerBlock()
    {
        ImmutableList.Builder<Long> values = new ImmutableList.Builder<>();
        for (long row = 0; row < 10; row++) {
            values.add(row);
        }
        return createNumericDataBlock(INTEGER, values.build());
    }

    public static Block[] createStringBlock()
    {
        ImmutableList.Builder<String> values = new ImmutableList.Builder<>();
        for (long row = 0; row < 10; row++) {
            values.add(String.valueOf(row));
        }
        return createStringDataBlock(VARCHAR, values.build());
    }
}
