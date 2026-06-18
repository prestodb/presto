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
package com.facebook.presto.parquet.batchreader;

import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.IntArrayBlock;
import com.facebook.presto.common.block.RunLengthEncodedBlock;
import com.facebook.presto.parquet.RichColumnDescriptor;
import com.facebook.presto.parquet.batchreader.decoders.ValuesDecoder.Int32ValuesDecoder;
import com.facebook.presto.parquet.reader.ColumnChunk;

import java.io.IOException;
import java.util.Optional;

public class Int32NestedBatchReader
        extends AbstractNestedBatchReader
{
    public Int32NestedBatchReader(RichColumnDescriptor columnDescriptor)
    {
        super(columnDescriptor);
    }

    @Override
    protected ColumnChunk readNestedWithNull()
            throws IOException
    {
        int maxDefinitionLevel = columnDescriptor.getMaxDefinitionLevel();
        RepetitionLevelDecodingContext repetitionLevelDecodingContext = readRepetitionLevels(nextBatchSize);
        DefinitionLevelDecodingContext definitionLevelDecodingContext = readDefinitionLevels(repetitionLevelDecodingContext.getDLValuesDecoderContexts(), repetitionLevelDecodingContext.getRepetitionLevels().length);

        int[] definitionLevels = definitionLevelDecodingContext.getDefinitionLevels();
        int newBatchSize = 0;
        int batchNonNullCount = 0;
        for (ValuesDecoderContext valuesDecoderContext : definitionLevelDecodingContext.getValuesDecoderContexts()) {
            int nonNullCount = 0;
            int valueCount = 0;
            for (int i = valuesDecoderContext.getStart(); i < valuesDecoderContext.getEnd(); i++) {
                nonNullCount += (definitionLevels[i] == maxDefinitionLevel ? 1 : 0);
                valueCount += (definitionLevels[i] >= maxDefinitionLevel - 1 ? 1 : 0);
            }
            batchNonNullCount += nonNullCount;
            newBatchSize += valueCount;
            valuesDecoderContext.setNonNullCount(nonNullCount);
            valuesDecoderContext.setValueCount(valueCount);
        }

        if (batchNonNullCount == 0) {
            Block block = RunLengthEncodedBlock.create(field.getType(), null, newBatchSize);
            return new ColumnChunk(block, definitionLevels, repetitionLevelDecodingContext.getRepetitionLevels());
        }

        int[] values = new int[newBatchSize];
        boolean[] isNull = new boolean[newBatchSize];
        int offset = 0;
        for (ValuesDecoderContext valuesDecoderContext : definitionLevelDecodingContext.getValuesDecoderContexts()) {
            ((Int32ValuesDecoder) valuesDecoderContext.getValuesDecoder()).readNext(values, offset, valuesDecoderContext.getNonNullCount());

            int valueDestinationIndex = offset + valuesDecoderContext.getValueCount() - 1;
            int valueSourceIndex = offset + valuesDecoderContext.getNonNullCount() - 1;
            int definitionLevelIndex = valuesDecoderContext.getEnd() - 1;

            while (valueDestinationIndex >= offset) {
                if (definitionLevels[definitionLevelIndex] == maxDefinitionLevel) {
                    values[valueDestinationIndex--] = values[valueSourceIndex--];
                }
                else if (definitionLevels[definitionLevelIndex] == maxDefinitionLevel - 1) {
                    values[valueDestinationIndex] = 0;
                    isNull[valueDestinationIndex] = true;
                    valueDestinationIndex--;
                }
                definitionLevelIndex--;
            }
            offset += valuesDecoderContext.getValueCount();
        }

        boolean hasNoNull = batchNonNullCount == newBatchSize;
        Block block = new IntArrayBlock(newBatchSize, hasNoNull ? Optional.empty() : Optional.of(isNull), values);
        return new ColumnChunk(block, definitionLevels, repetitionLevelDecodingContext.getRepetitionLevels());
    }

    @Override
    protected ColumnChunk readNestedNoNull()
            throws IOException
    {
        int maxDefinitionLevel = columnDescriptor.getMaxDefinitionLevel();
        RepetitionLevelDecodingContext repetitionLevelDecodingContext = readRepetitionLevels(nextBatchSize);
        DefinitionLevelDecodingContext definitionLevelDecodingContext = readDefinitionLevels(repetitionLevelDecodingContext.getDLValuesDecoderContexts(), repetitionLevelDecodingContext.getRepetitionLevels().length);

        int[] definitionLevels = definitionLevelDecodingContext.getDefinitionLevels();
        int newBatchSize = 0;
        for (ValuesDecoderContext valuesDecoderContext : definitionLevelDecodingContext.getValuesDecoderContexts()) {
            int valueCount = 0;
            for (int i = valuesDecoderContext.getStart(); i < valuesDecoderContext.getEnd(); i++) {
                valueCount += (definitionLevels[i] == maxDefinitionLevel ? 1 : 0);
            }
            newBatchSize += valueCount;
            valuesDecoderContext.setNonNullCount(valueCount);
            valuesDecoderContext.setValueCount(valueCount);
        }

        int[] values = new int[newBatchSize];
        int offset = 0;
        for (ValuesDecoderContext valuesDecoderContext : definitionLevelDecodingContext.getValuesDecoderContexts()) {
            ((Int32ValuesDecoder) valuesDecoderContext.getValuesDecoder()).readNext(values, offset, valuesDecoderContext.getNonNullCount());
            offset += valuesDecoderContext.getValueCount();
        }

        Block block = new IntArrayBlock(newBatchSize, Optional.empty(), values);
        return new ColumnChunk(block, definitionLevels, repetitionLevelDecodingContext.getRepetitionLevels());
    }

    @Override
    protected void seek()
            throws IOException
    {
        if (readOffset == 0) {
            return;
        }
        int maxDefinitionLevel = columnDescriptor.getMaxDefinitionLevel();
        RepetitionLevelDecodingContext repetitionLevelDecodingContext = readRepetitionLevels(readOffset);
        DefinitionLevelDecodingContext definitionLevelDecodingContext = readDefinitionLevels(repetitionLevelDecodingContext.getDLValuesDecoderContexts(), repetitionLevelDecodingContext.getRepetitionLevels().length);

        int[] definitionLevels = definitionLevelDecodingContext.getDefinitionLevels();
        for (ValuesDecoderContext valuesDecoderContext : definitionLevelDecodingContext.getValuesDecoderContexts()) {
            int valueCount = 0;
            for (int i = valuesDecoderContext.getStart(); i < valuesDecoderContext.getEnd(); i++) {
                valueCount += (definitionLevels[i] == maxDefinitionLevel ? 1 : 0);
            }
            Int32ValuesDecoder intValuesDecoder = (Int32ValuesDecoder) valuesDecoderContext.getValuesDecoder();
            intValuesDecoder.skip(valueCount);
        }
    }
}
