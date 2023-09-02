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
package io.trino.parquet.writer.repdef;

import io.trino.spi.block.Block;
import io.trino.spi.block.ColumnarArray;
import io.trino.spi.block.ColumnarMap;
import io.trino.spi.block.ColumnarRow;
import org.apache.parquet.column.values.ValuesWriter;

import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class DefLevelWriterProviders
{
    private DefLevelWriterProviders() {}

    public static DefLevelWriterProvider of(Block block, int maxDefinitionLevel)
    {
        return new PrimitiveDefLevelWriterProvider(block, maxDefinitionLevel);
    }

    public static DefLevelWriterProvider of(ColumnarRow columnarRow, int maxDefinitionLevel)
    {
        return new ColumnRowDefLevelWriterProvider(columnarRow, maxDefinitionLevel);
    }

    public static DefLevelWriterProvider of(ColumnarArray columnarArray, int maxDefinitionLevel)
    {
        return new ColumnArrayDefLevelWriterProvider(columnarArray, maxDefinitionLevel);
    }

    public static DefLevelWriterProvider of(ColumnarMap columnarMap, int maxDefinitionLevel)
    {
        return new ColumnMapDefLevelWriterProvider(columnarMap, maxDefinitionLevel);
    }

    static class PrimitiveDefLevelWriterProvider
            implements DefLevelWriterProvider
    {
        private final Block block;
        private final int maxDefinitionLevel;

        PrimitiveDefLevelWriterProvider(Block block, int maxDefinitionLevel)
        {
            this.block = requireNonNull(block, "block is null");
            this.maxDefinitionLevel = maxDefinitionLevel;
        }

        @Override
        public DefinitionLevelWriter getDefinitionLevelWriter(Optional<DefinitionLevelWriter> nestedWriter, ValuesWriter encoder)
        {
            checkArgument(nestedWriter.isEmpty(), "nestedWriter should be empty for primitive definition level writer");
            return new DefinitionLevelWriter()
            {
                private int offset;

                @Override
                public ValuesCount writeDefinitionLevels()
                {
                    return writeDefinitionLevels(block.getPositionCount());
                }

                @Override
                public ValuesCount writeDefinitionLevels(int positionsCount)
                {
                    checkValidPosition(offset, positionsCount, block.getPositionCount());
                    int nonNullsCount = 0;
                    if (!block.mayHaveNull()) {
                        for (int position = offset; position < offset + positionsCount; position++) {
                            encoder.writeInteger(maxDefinitionLevel);
                        }
                        nonNullsCount = positionsCount;
                    }
                    else {
                        for (int position = offset; position < offset + positionsCount; position++) {
                            int isNull = block.isNull(position) ? 1 : 0;
                            encoder.writeInteger(maxDefinitionLevel - isNull);
                            nonNullsCount += isNull ^ 1;
                        }
                    }
                    offset += positionsCount;
                    return new ValuesCount(positionsCount, nonNullsCount);
                }
            };
        }
    }

    static class ColumnRowDefLevelWriterProvider
            implements DefLevelWriterProvider
    {
        private final ColumnarRow columnarRow;
        private final int maxDefinitionLevel;

        ColumnRowDefLevelWriterProvider(ColumnarRow columnarRow, int maxDefinitionLevel)
        {
            this.columnarRow = requireNonNull(columnarRow, "columnarRow is null");
            this.maxDefinitionLevel = maxDefinitionLevel;
        }

        @Override
        public DefinitionLevelWriter getDefinitionLevelWriter(Optional<DefinitionLevelWriter> nestedWriterOptional, ValuesWriter encoder)
        {
            checkArgument(nestedWriterOptional.isPresent(), "nestedWriter should be present for column row definition level writer");
            return new DefinitionLevelWriter()
            {
                private final DefinitionLevelWriter nestedWriter = nestedWriterOptional.orElseThrow();

                private int offset;

                @Override
                public ValuesCount writeDefinitionLevels()
                {
                    return writeDefinitionLevels(columnarRow.getPositionCount());
                }

                @Override
                public ValuesCount writeDefinitionLevels(int positionsCount)
                {
                    checkValidPosition(offset, positionsCount, columnarRow.getPositionCount());
                    if (!columnarRow.mayHaveNull()) {
                        offset += positionsCount;
                        return nestedWriter.writeDefinitionLevels(positionsCount);
                    }
                    int maxDefinitionValuesCount = 0;
                    int totalValuesCount = 0;
                    for (int position = offset; position < offset + positionsCount; ) {
                        if (columnarRow.isNull(position)) {
                            encoder.writeInteger(maxDefinitionLevel - 1);
                            totalValuesCount++;
                            position++;
                        }
                        else {
                            int consecutiveNonNullsCount = 1;
                            position++;
                            while (position < offset + positionsCount && !columnarRow.isNull(position)) {
                                position++;
                                consecutiveNonNullsCount++;
                            }
                            ValuesCount valuesCount = nestedWriter.writeDefinitionLevels(consecutiveNonNullsCount);
                            maxDefinitionValuesCount += valuesCount.maxDefinitionLevelValuesCount();
                            totalValuesCount += valuesCount.totalValuesCount();
                        }
                    }
                    offset += positionsCount;
                    return new ValuesCount(totalValuesCount, maxDefinitionValuesCount);
                }
            };
        }
    }

    static class ColumnMapDefLevelWriterProvider
            implements DefLevelWriterProvider
    {
        private final ColumnarMap columnarMap;
        private final int maxDefinitionLevel;

        ColumnMapDefLevelWriterProvider(ColumnarMap columnarMap, int maxDefinitionLevel)
        {
            this.columnarMap = requireNonNull(columnarMap, "columnarMap is null");
            this.maxDefinitionLevel = maxDefinitionLevel;
        }

        @Override
        public DefinitionLevelWriter getDefinitionLevelWriter(Optional<DefinitionLevelWriter> nestedWriterOptional, ValuesWriter encoder)
        {
            checkArgument(nestedWriterOptional.isPresent(), "nestedWriter should be present for column map definition level writer");
            return new DefinitionLevelWriter()
            {
                private final DefinitionLevelWriter nestedWriter = nestedWriterOptional.orElseThrow();

                private int offset;

                @Override
                public ValuesCount writeDefinitionLevels()
                {
                    return writeDefinitionLevels(columnarMap.getPositionCount());
                }

                @Override
                public ValuesCount writeDefinitionLevels(int positionsCount)
                {
                    checkValidPosition(offset, positionsCount, columnarMap.getPositionCount());
                    int maxDefinitionValuesCount = 0;
                    int totalValuesCount = 0;
                    if (!columnarMap.mayHaveNull()) {
                        for (int position = offset; position < offset + positionsCount; ) {
                            int mapLength = columnarMap.getEntryCount(position);
                            if (mapLength == 0) {
                                encoder.writeInteger(maxDefinitionLevel - 1);
                                totalValuesCount++;
                                position++;
                            }
                            else {
                                int consecutiveNonEmptyArrayLength = mapLength;
                                position++;
                                while (position < offset + positionsCount) {
                                    mapLength = columnarMap.getEntryCount(position);
                                    if (mapLength == 0) {
                                        break;
                                    }
                                    position++;
                                    consecutiveNonEmptyArrayLength += mapLength;
                                }
                                ValuesCount valuesCount = nestedWriter.writeDefinitionLevels(consecutiveNonEmptyArrayLength);
                                maxDefinitionValuesCount += valuesCount.maxDefinitionLevelValuesCount();
                                totalValuesCount += valuesCount.totalValuesCount();
                            }
                        }
                    }
                    else {
                        for (int position = offset; position < offset + positionsCount; position++) {
                            if (columnarMap.isNull(position)) {
                                encoder.writeInteger(maxDefinitionLevel - 2);
                                totalValuesCount++;
                                continue;
                            }
                            int mapLength = columnarMap.getEntryCount(position);
                            if (mapLength == 0) {
                                encoder.writeInteger(maxDefinitionLevel - 1);
                                totalValuesCount++;
                            }
                            else {
                                ValuesCount valuesCount = nestedWriter.writeDefinitionLevels(mapLength);
                                maxDefinitionValuesCount += valuesCount.maxDefinitionLevelValuesCount();
                                totalValuesCount += valuesCount.totalValuesCount();
                            }
                        }
                    }
                    offset += positionsCount;
                    return new ValuesCount(totalValuesCount, maxDefinitionValuesCount);
                }
            };
        }
    }

    static class ColumnArrayDefLevelWriterProvider
            implements DefLevelWriterProvider
    {
        private final ColumnarArray columnarArray;
        private final int maxDefinitionLevel;

        ColumnArrayDefLevelWriterProvider(ColumnarArray columnarArray, int maxDefinitionLevel)
        {
            this.columnarArray = requireNonNull(columnarArray, "columnarArray is null");
            this.maxDefinitionLevel = maxDefinitionLevel;
        }

        @Override
        public DefinitionLevelWriter getDefinitionLevelWriter(Optional<DefinitionLevelWriter> nestedWriterOptional, ValuesWriter encoder)
        {
            checkArgument(nestedWriterOptional.isPresent(), "nestedWriter should be present for column map definition level writer");
            return new DefinitionLevelWriter()
            {
                private final DefinitionLevelWriter nestedWriter = nestedWriterOptional.orElseThrow();

                private int offset;

                @Override
                public ValuesCount writeDefinitionLevels()
                {
                    return writeDefinitionLevels(columnarArray.getPositionCount());
                }

                @Override
                public ValuesCount writeDefinitionLevels(int positionsCount)
                {
                    checkValidPosition(offset, positionsCount, columnarArray.getPositionCount());
                    int maxDefinitionValuesCount = 0;
                    int totalValuesCount = 0;
                    if (!columnarArray.mayHaveNull()) {
                        for (int position = offset; position < offset + positionsCount; ) {
                            int arrayLength = columnarArray.getLength(position);
                            if (arrayLength == 0) {
                                encoder.writeInteger(maxDefinitionLevel - 1);
                                totalValuesCount++;
                                position++;
                            }
                            else {
                                int consecutiveNonEmptyArrayLength = arrayLength;
                                position++;
                                while (position < offset + positionsCount) {
                                    arrayLength = columnarArray.getLength(position);
                                    if (arrayLength == 0) {
                                        break;
                                    }
                                    position++;
                                    consecutiveNonEmptyArrayLength += arrayLength;
                                }
                                ValuesCount valuesCount = nestedWriter.writeDefinitionLevels(consecutiveNonEmptyArrayLength);
                                maxDefinitionValuesCount += valuesCount.maxDefinitionLevelValuesCount();
                                totalValuesCount += valuesCount.totalValuesCount();
                            }
                        }
                    }
                    else {
                        for (int position = offset; position < offset + positionsCount; position++) {
                            if (columnarArray.isNull(position)) {
                                encoder.writeInteger(maxDefinitionLevel - 2);
                                totalValuesCount++;
                                continue;
                            }
                            int arrayLength = columnarArray.getLength(position);
                            if (arrayLength == 0) {
                                encoder.writeInteger(maxDefinitionLevel - 1);
                                totalValuesCount++;
                            }
                            else {
                                ValuesCount valuesCount = nestedWriter.writeDefinitionLevels(arrayLength);
                                maxDefinitionValuesCount += valuesCount.maxDefinitionLevelValuesCount();
                                totalValuesCount += valuesCount.totalValuesCount();
                            }
                        }
                    }
                    offset += positionsCount;
                    return new ValuesCount(totalValuesCount, maxDefinitionValuesCount);
                }
            };
        }
    }

    private static void checkValidPosition(int offset, int positionsCount, int totalPositionsCount)
    {
        if (offset < 0 || positionsCount < 0 || offset + positionsCount > totalPositionsCount) {
            throw new IndexOutOfBoundsException(format("Invalid offset %s and positionsCount %s in block with %s positions", offset, positionsCount, totalPositionsCount));
        }
    }
}
