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
import com.facebook.presto.parquet.batchreader.decoders.ValuesDecoder.BinaryValuesDecoder.ReadChunk;
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
        int maxDL = columnDescriptor.getMaxDefinitionLevel();
        RepetitionLevelDecodingInfo repetitionLevelDecodingInfo = readRLs(nextBatchSize);

        DefinitionLevelDecodingInfo definitionLevelDecodingInfo = readDLs(repetitionLevelDecodingInfo.getDLValuesDecoderInfos(), repetitionLevelDecodingInfo.getRepetitionLevels().length);

        int[] dls = definitionLevelDecodingInfo.getDLs();
        int newBatchSize = 0;
        int batchNonNullCount = 0;
        for (ValuesDecoderInfo valuesDecoderInfo : definitionLevelDecodingInfo.getValuesDecoderInfos()) {
            int nonNullCount = 0;
            int valueCount = 0;
            for (int i = valuesDecoderInfo.getStart(); i < valuesDecoderInfo.getEnd(); i++) {
                nonNullCount += (dls[i] == maxDL ? 1 : 0);
                valueCount += (dls[i] >= maxDL - 1 ? 1 : 0);
            }
            batchNonNullCount += nonNullCount;
            newBatchSize += valueCount;
            valuesDecoderInfo.setNonNullCount(nonNullCount);
            valuesDecoderInfo.setValueCount(valueCount);
        }

        if (batchNonNullCount == 0) {
            Block block = RunLengthEncodedBlock.create(field.getType(), null, newBatchSize);
            return new ColumnChunk(block, dls, repetitionLevelDecodingInfo.getRepetitionLevels());
        }

        List<ReadChunk> readChunkList = new ArrayList<>();
        int bufferSize = 0;

        for (ValuesDecoderInfo valuesDecoderInfo : definitionLevelDecodingInfo.getValuesDecoderInfos()) {
            ReadChunk readChunk = ((BinaryValuesDecoder) valuesDecoderInfo.getValuesDecoder()).readNext(valuesDecoderInfo.getNonNullCount());
            bufferSize += readChunk.getBufferSize();
            readChunkList.add(readChunk);
        }

        byte[] byteBuffer = new byte[bufferSize];
        int[] offsets = new int[newBatchSize + 1];

        int i = 0;
        int bufferIdx = 0;
        int offsetIdx = 0;
        for (ValuesDecoderInfo valuesDecoderInfo : definitionLevelDecodingInfo.getValuesDecoderInfos()) {
            ReadChunk readChunk = readChunkList.get(i);

            bufferIdx = ((BinaryValuesDecoder) valuesDecoderInfo.getValuesDecoder()).readIntoBuffer(byteBuffer, bufferIdx, offsets, offsetIdx, readChunk);
            offsetIdx += valuesDecoderInfo.getValueCount();
            i++;
        }

        boolean[] isNull = new boolean[newBatchSize];

        int offset = 0;
        for (ValuesDecoderInfo valuesDecoderInfo : definitionLevelDecodingInfo.getValuesDecoderInfos()) {
            int destIdx = offset + valuesDecoderInfo.getValueCount() - 1;
            int srcIdx = offset + valuesDecoderInfo.getNonNullCount() - 1;
            int dlIdx = valuesDecoderInfo.getEnd() - 1;

            offsets[destIdx + 1] = offsets[srcIdx + 1];
            while (destIdx >= offset) {
                if (dls[dlIdx] == maxDL) {
                    offsets[destIdx--] = offsets[srcIdx--];
                }
                else if (dls[dlIdx] == maxDL - 1) {
                    offsets[destIdx] = offsets[srcIdx + 1];
                    isNull[destIdx] = true;
                    destIdx--;
                }
                dlIdx--;
            }
            offset += valuesDecoderInfo.getValueCount();
        }

        Slice buffer = Slices.wrappedBuffer(byteBuffer, 0, bufferSize);
        boolean hasNoNull = batchNonNullCount == newBatchSize;
        Block block = new VariableWidthBlock(newBatchSize, buffer, offsets, hasNoNull ? Optional.empty() : Optional.of(isNull));
        return new ColumnChunk(block, dls, repetitionLevelDecodingInfo.getRepetitionLevels());
    }

    @Override
    protected ColumnChunk readNestedNoNull()
            throws IOException
    {
        int maxDL = columnDescriptor.getMaxDefinitionLevel();
        RepetitionLevelDecodingInfo repetitionLevelDecodingInfo = readRLs(nextBatchSize);

        DefinitionLevelDecodingInfo definitionLevelDecodingInfo = readDLs(repetitionLevelDecodingInfo.getDLValuesDecoderInfos(), repetitionLevelDecodingInfo.getRepetitionLevels().length);

        int[] dls = definitionLevelDecodingInfo.getDLs();
        int newBatchSize = 0;
        for (ValuesDecoderInfo valuesDecoderInfo : definitionLevelDecodingInfo.getValuesDecoderInfos()) {
            int valueCount = 0;
            for (int i = valuesDecoderInfo.getStart(); i < valuesDecoderInfo.getEnd(); i++) {
                valueCount += (dls[i] == maxDL ? 1 : 0);
            }
            newBatchSize += valueCount;
            valuesDecoderInfo.setNonNullCount(valueCount);
            valuesDecoderInfo.setValueCount(valueCount);
        }

        List<ReadChunk> readChunkList = new ArrayList<>();
        int bufferSize = 0;

        for (ValuesDecoderInfo valuesDecoderInfo : definitionLevelDecodingInfo.getValuesDecoderInfos()) {
            ReadChunk readChunk = ((BinaryValuesDecoder) valuesDecoderInfo.getValuesDecoder()).readNext(valuesDecoderInfo.getNonNullCount());
            bufferSize += readChunk.getBufferSize();
            readChunkList.add(readChunk);
        }

        byte[] byteBuffer = new byte[bufferSize];
        int[] offsets = new int[newBatchSize + 1];

        int i = 0;
        int bufferIdx = 0;
        int offsetIdx = 0;
        for (ValuesDecoderInfo valuesDecoderInfo : definitionLevelDecodingInfo.getValuesDecoderInfos()) {
            ReadChunk readChunk = readChunkList.get(i);

            bufferIdx = ((BinaryValuesDecoder) valuesDecoderInfo.getValuesDecoder()).readIntoBuffer(byteBuffer, bufferIdx, offsets, offsetIdx, readChunk);
            offsetIdx += valuesDecoderInfo.getValueCount();
            i++;
        }

        Slice buffer = Slices.wrappedBuffer(byteBuffer, 0, bufferSize);
        Block block = new VariableWidthBlock(newBatchSize, buffer, offsets, Optional.empty());
        return new ColumnChunk(block, dls, repetitionLevelDecodingInfo.getRepetitionLevels());
    }

    @Override
    protected void skip(int skipSize)
            throws IOException
    {
        int maxDL = columnDescriptor.getMaxDefinitionLevel();

        RepetitionLevelDecodingInfo repetitionLevelDecodingInfo = readRLs(skipSize);
        DefinitionLevelDecodingInfo definitionLevelDecodingInfo = readDLs(repetitionLevelDecodingInfo.getDLValuesDecoderInfos(), repetitionLevelDecodingInfo.getRepetitionLevels().length);

        int[] dls = definitionLevelDecodingInfo.getDLs();
        for (ValuesDecoderInfo valuesDecoderInfo : definitionLevelDecodingInfo.getValuesDecoderInfos()) {
            int valueCount = 0;
            for (int i = valuesDecoderInfo.getStart(); i < valuesDecoderInfo.getEnd(); i++) {
                valueCount += (dls[i] == maxDL ? 1 : 0);
            }
            BinaryValuesDecoder binaryValuesDecoder = ((BinaryValuesDecoder) valuesDecoderInfo.getValuesDecoder());
            binaryValuesDecoder.skip(valueCount);
        }
    }
}
