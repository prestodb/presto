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
        RepetitionLevelDecodingInfo repetitionLevelDecodingInfo = readRepetitionLevels(nextBatchSize);
        DefinitionLevelDecodingInfo definitionLevelDecodingInfo = readDefinitionLevels(repetitionLevelDecodingInfo.getDLValuesDecoderInfos(), repetitionLevelDecodingInfo.getRepetitionLevels().length);

        int[] definitionLevels = definitionLevelDecodingInfo.getDefinitionLevels();
        int newBatchSize = 0;
        int batchNonNullCount = 0;
        for (ValuesDecoderInfo valuesDecoderInfo : definitionLevelDecodingInfo.getValuesDecoderInfos()) {
            int nonNullCount = 0;
            int valueCount = 0;
            for (int i = valuesDecoderInfo.getStart(); i < valuesDecoderInfo.getEnd(); i++) {
                nonNullCount += (definitionLevels[i] == maxDefinitionLevel ? 1 : 0);
                valueCount += (definitionLevels[i] >= maxDefinitionLevel - 1 ? 1 : 0);
            }
            batchNonNullCount += nonNullCount;
            newBatchSize += valueCount;
            valuesDecoderInfo.setNonNullCount(nonNullCount);
            valuesDecoderInfo.setValueCount(valueCount);
        }

        if (batchNonNullCount == 0) {
            Block block = RunLengthEncodedBlock.create(field.getType(), null, newBatchSize);
            return new ColumnChunk(block, definitionLevels, repetitionLevelDecodingInfo.getRepetitionLevels());
        }

        int[] values = new int[newBatchSize];
        boolean[] isNull = new boolean[newBatchSize];
        int offset = 0;
        for (ValuesDecoderInfo valuesDecoderInfo : definitionLevelDecodingInfo.getValuesDecoderInfos()) {
            ((Int32ValuesDecoder) valuesDecoderInfo.getValuesDecoder()).readNext(values, offset, valuesDecoderInfo.getNonNullCount());

            int valueDestinationIndex = offset + valuesDecoderInfo.getValueCount() - 1;
            int valueSourceIndex = offset + valuesDecoderInfo.getNonNullCount() - 1;
            int definitionLevelIndex = valuesDecoderInfo.getEnd() - 1;

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
            offset += valuesDecoderInfo.getValueCount();
        }

        boolean hasNoNull = batchNonNullCount == newBatchSize;
        Block block = new IntArrayBlock(newBatchSize, hasNoNull ? Optional.empty() : Optional.of(isNull), values);
        return new ColumnChunk(block, definitionLevels, repetitionLevelDecodingInfo.getRepetitionLevels());
    }

    @Override
    protected ColumnChunk readNestedNoNull()
            throws IOException
    {
        int maxDefinitionLevel = columnDescriptor.getMaxDefinitionLevel();
        RepetitionLevelDecodingInfo repetitionLevelDecodingInfo = readRepetitionLevels(nextBatchSize);
        DefinitionLevelDecodingInfo definitionLevelDecodingInfo = readDefinitionLevels(repetitionLevelDecodingInfo.getDLValuesDecoderInfos(), repetitionLevelDecodingInfo.getRepetitionLevels().length);

        int[] definitionLevels = definitionLevelDecodingInfo.getDefinitionLevels();
        int newBatchSize = 0;
        for (ValuesDecoderInfo valuesDecoderInfo : definitionLevelDecodingInfo.getValuesDecoderInfos()) {
            int valueCount = 0;
            for (int i = valuesDecoderInfo.getStart(); i < valuesDecoderInfo.getEnd(); i++) {
                valueCount += (definitionLevels[i] == maxDefinitionLevel ? 1 : 0);
            }
            newBatchSize += valueCount;
            valuesDecoderInfo.setNonNullCount(valueCount);
            valuesDecoderInfo.setValueCount(valueCount);
        }

        int[] values = new int[newBatchSize];
        int offset = 0;
        for (ValuesDecoderInfo valuesDecoderInfo : definitionLevelDecodingInfo.getValuesDecoderInfos()) {
            ((Int32ValuesDecoder) valuesDecoderInfo.getValuesDecoder()).readNext(values, offset, valuesDecoderInfo.getNonNullCount());
            offset += valuesDecoderInfo.getValueCount();
        }

        Block block = new IntArrayBlock(newBatchSize, Optional.empty(), values);
        return new ColumnChunk(block, definitionLevels, repetitionLevelDecodingInfo.getRepetitionLevels());
    }

    @Override
    protected void seek()
            throws IOException
    {
        if (readOffset == 0) {
            return;
        }
        int maxDefinitionLevel = columnDescriptor.getMaxDefinitionLevel();
        RepetitionLevelDecodingInfo repetitionLevelDecodingInfo = readRepetitionLevels(readOffset);
        DefinitionLevelDecodingInfo definitionLevelDecodingInfo = readDefinitionLevels(repetitionLevelDecodingInfo.getDLValuesDecoderInfos(), repetitionLevelDecodingInfo.getRepetitionLevels().length);

        int[] definitionLevels = definitionLevelDecodingInfo.getDefinitionLevels();
        for (ValuesDecoderInfo valuesDecoderInfo : definitionLevelDecodingInfo.getValuesDecoderInfos()) {
            int valueCount = 0;
            for (int i = valuesDecoderInfo.getStart(); i < valuesDecoderInfo.getEnd(); i++) {
                valueCount += (definitionLevels[i] == maxDefinitionLevel ? 1 : 0);
            }
            Int32ValuesDecoder intValuesDecoder = (Int32ValuesDecoder) valuesDecoderInfo.getValuesDecoder();
            intValuesDecoder.skip(valueCount);
        }
    }
}
