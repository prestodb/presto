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
import com.facebook.presto.common.block.RunLengthEncodedBlock;
import com.facebook.presto.common.block.VariableWidthBlock;
import com.facebook.presto.parquet.RichColumnDescriptor;
import com.facebook.presto.parquet.batchreader.decoders.ValuesDecoder.BinaryValuesDecoder;
import com.facebook.presto.parquet.batchreader.decoders.ValuesDecoder.BinaryValuesDecoder.ValueBuffer;
import com.facebook.presto.parquet.reader.ColumnChunk;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public class BinaryNestedBatchReader
        extends AbstractNestedBatchReader
{
    public BinaryNestedBatchReader(RichColumnDescriptor columnDescriptor)
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

        List<ValueBuffer> valueBuffers = new ArrayList<>();
        int bufferSize = 0;
        for (ValuesDecoderInfo valuesDecoderInfo : definitionLevelDecodingInfo.getValuesDecoderInfos()) {
            ValueBuffer valueBuffer = ((BinaryValuesDecoder) valuesDecoderInfo.getValuesDecoder()).readNext(valuesDecoderInfo.getNonNullCount());
            bufferSize += valueBuffer.getBufferSize();
            valueBuffers.add(valueBuffer);
        }

        byte[] byteBuffer = new byte[bufferSize];
        int[] offsets = new int[newBatchSize + 1];
        int i = 0;
        int bufferIndex = 0;
        int offsetIndex = 0;
        for (ValuesDecoderInfo valuesDecoderInfo : definitionLevelDecodingInfo.getValuesDecoderInfos()) {
            ValueBuffer value = valueBuffers.get(i);
            bufferIndex = ((BinaryValuesDecoder) valuesDecoderInfo.getValuesDecoder()).readIntoBuffer(byteBuffer, bufferIndex, offsets, offsetIndex, value);
            offsetIndex += valuesDecoderInfo.getValueCount();
            i++;
        }

        boolean[] isNull = new boolean[newBatchSize];
        int offset = 0;
        for (ValuesDecoderInfo valuesDecoderInfo : definitionLevelDecodingInfo.getValuesDecoderInfos()) {
            int destinationIndex = offset + valuesDecoderInfo.getValueCount() - 1;
            int sourceIndex = offset + valuesDecoderInfo.getNonNullCount() - 1;
            int definitionLevelIndex = valuesDecoderInfo.getEnd() - 1;

            offsets[destinationIndex + 1] = offsets[sourceIndex + 1];
            while (destinationIndex >= offset) {
                if (definitionLevels[definitionLevelIndex] == maxDefinitionLevel) {
                    offsets[destinationIndex--] = offsets[sourceIndex--];
                }
                else if (definitionLevels[definitionLevelIndex] == maxDefinitionLevel - 1) {
                    offsets[destinationIndex] = offsets[sourceIndex + 1];
                    isNull[destinationIndex] = true;
                    destinationIndex--;
                }
                definitionLevelIndex--;
            }
            offset += valuesDecoderInfo.getValueCount();
        }

        Slice buffer = Slices.wrappedBuffer(byteBuffer, 0, bufferSize);
        boolean hasNoNull = batchNonNullCount == newBatchSize;
        Block block = new VariableWidthBlock(newBatchSize, buffer, offsets, hasNoNull ? Optional.empty() : Optional.of(isNull));
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

        List<ValueBuffer> valueBuffers = new ArrayList<>();
        int bufferSize = 0;
        for (ValuesDecoderInfo valuesDecoderInfo : definitionLevelDecodingInfo.getValuesDecoderInfos()) {
            ValueBuffer valueBuffer = ((BinaryValuesDecoder) valuesDecoderInfo.getValuesDecoder()).readNext(valuesDecoderInfo.getNonNullCount());
            bufferSize += valueBuffer.getBufferSize();
            valueBuffers.add(valueBuffer);
        }

        byte[] byteBuffer = new byte[bufferSize];
        int[] offsets = new int[newBatchSize + 1];
        int i = 0;
        int bufferIndex = 0;
        int offsetIndex = 0;
        for (ValuesDecoderInfo valuesDecoderInfo : definitionLevelDecodingInfo.getValuesDecoderInfos()) {
            ValueBuffer value = valueBuffers.get(i);
            bufferIndex = ((BinaryValuesDecoder) valuesDecoderInfo.getValuesDecoder()).readIntoBuffer(byteBuffer, bufferIndex, offsets, offsetIndex, value);
            offsetIndex += valuesDecoderInfo.getValueCount();
            i++;
        }

        Slice buffer = Slices.wrappedBuffer(byteBuffer, 0, bufferSize);
        Block block = new VariableWidthBlock(newBatchSize, buffer, offsets, Optional.empty());
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
            BinaryValuesDecoder binaryValuesDecoder = ((BinaryValuesDecoder) valuesDecoderInfo.getValuesDecoder());
            binaryValuesDecoder.skip(valueCount);
        }
    }
}
