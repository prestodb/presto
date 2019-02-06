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
package com.facebook.presto.orc.reader;

import com.facebook.presto.orc.Filter;
import com.facebook.presto.orc.QualifyingSet;
import com.facebook.presto.orc.stream.BooleanInputStream;
import com.facebook.presto.orc.stream.InputStreamSource;
import com.facebook.presto.orc.stream.LongInputStream;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.type.Type;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.Arrays;

import static com.facebook.presto.orc.stream.MissingInputStreamSource.missingStreamSource;

abstract class ColumnReader
        implements StreamReader
{
    protected InputStreamSource<BooleanInputStream> presentStreamSource = missingStreamSource(BooleanInputStream.class);

    @Nullable
    protected BooleanInputStream presentStream;

    QualifyingSet inputQualifyingSet;
    QualifyingSet outputQualifyingSet;
    Block block;
    int outputChannel = -1;
    Filter filter;
    int columnIndex;
    Type type;
    int expectNumValues = 10000;
    // First row number in row group that is not processed due to
    // reaching target size. This must occur as a position in
    // inputQualifyingSet. -1 if all inputQualifyingSet is processed.
    int truncationRow = -1;

    boolean rowGroupOpen;

    // position of first unprocessed row from the start of the row group.
    int posInRowGroup;

    //Present flag for each row between posInRowGroup and end of inputQualifyingSet.
    boolean[] present;

    // Number of values in 'present'.
    int numPresent;

    // Lengths for present rows from posInRowGroup to end of inputQualifyingSet.
    int[] lengths;

    // Number of elements in lengths.
    int numLengths;

    // Index of length of first unprocessed element in 'lemgths'.
    int lengthIdx;

    // Number of values in Block to be returned by getBlock.
    int numValues;

    // Number of result rows in scan() so far.
    int numResults;

    // Number of bytes the next scan() may add to the result.
    long resultSizeBudget = 8 * 10000;

    // Null flags for retrieved values. At least numValues + numResults elements.
    boolean[] valueIsNull;

    public QualifyingSet getInputQualifyingSet()
    {
        return inputQualifyingSet;
    }

    public void setInputQualifyingSet(QualifyingSet qualifyingSet)
    {
        inputQualifyingSet = qualifyingSet;
    }

    public QualifyingSet getOutputQualifyingSet()
    {
        return outputQualifyingSet;
    }

    public void setOutputQualifyingSet(QualifyingSet set)
    {
        outputQualifyingSet = set;
    }

    @Override
    public QualifyingSet getOrCreateOutputQualifyingSet()
    {
        if (outputQualifyingSet == null) {
            outputQualifyingSet = new QualifyingSet();
        }
        return outputQualifyingSet;
    }

    @Override
    public void setFilterAndChannel(Filter filter, int channel, int columnIndex, Type type)
    {
        this.filter = filter;
        outputChannel = channel;
        this.columnIndex = columnIndex;
        this.type = type;
    }

    @Override
    public int getChannel()
    {
        return outputChannel;
    }

    @Override
    public Filter getFilter()
    {
        return filter;
    }

    @Override
    public int getColumnIndex()
    {
        return columnIndex;
    }

    @Override
    public int getNumValues()
    {
        return numValues;
    }

    @Override
    public void setResultSizeBudget(long bytes)
    {
        resultSizeBudget = bytes;
    }

    @Override
    public int getPosition()
    {
        return posInRowGroup;
    }

    @Override
    public int getTruncationRow()
    {
        return truncationRow;
    }

    @Override
    public int getResultSizeInBytes()
    {
        int fixedSize = getFixedWidth();
        if (fixedSize != -1) {
            return numValues * fixedSize;
        }
        throw new UnsupportedOperationException("Variable width streams must implement getResultSizeInBytes()");
    }

    public void compactQualifyingSet(int[] surviving, int numSurviving)
    {
        if (outputQualifyingSet == null) {
            return;
        }
        outputQualifyingSet.compactPositionsAndErrors(surviving, numSurviving);
    }

    protected void beginScan(BooleanInputStream presentStream, LongInputStream lengthStream)
            throws IOException
    {
        numResults = 0;
        truncationRow = -1;
        QualifyingSet input = inputQualifyingSet;
        QualifyingSet output = outputQualifyingSet;
        if (filter != null && output == null) {
            outputQualifyingSet = new QualifyingSet();
        }
        int numInput = input.getPositionCount();
        int end = input.getEnd();
        int rowsInRange = end - posInRowGroup;
        int neededLengths = 0;
        if (presentStream == null) {
            neededLengths = rowsInRange;
        }
        else {
            if (present == null || present.length < rowsInRange) {
                present = new boolean[rowsInRange];
            }
            if (numPresent > 0 && rowsInRange > numPresent) {
                throw new IllegalArgumentException("The present stream should be read in full the first time");
            }
            if (numPresent == 0) {
                presentStream.getSetBits(rowsInRange, present);
                numPresent = rowsInRange;
            }
            if (lengthStream != null) {
                for (int i = 0; i < rowsInRange; i++) {
                    if (present[i]) {
                        neededLengths++;
                    }
                }
            }
        }
        if (lengthStream == null || neededLengths <= numLengths) {
            return;
        }
        neededLengths -= numLengths;
        if (lengths == null) {
            lengths = new int[neededLengths + numLengths];
        }
        else if (lengths.length < numLengths + neededLengths) {
            lengths = Arrays.copyOf(lengths, numLengths + neededLengths + 100);
        }
        lengthStream.nextIntVector(neededLengths, lengths, numLengths);
        numLengths += neededLengths;
    }

    protected void endScan(BooleanInputStream presentStream)
    {
        // The reader is positioned at inputQualifyingSet.end() or
        // truncationRow. posInRowGroup is where the calling scan()
        // started.
        int initialPosInRowGroup = posInRowGroup;
        int end = inputQualifyingSet.getEnd();
        posInRowGroup = truncationRow != -1 ? truncationRow : end;
        if (presentStream != null) {
            int numAdvanced = posInRowGroup - initialPosInRowGroup;
            if (numAdvanced < numPresent) {
                System.arraycopy(present, posInRowGroup - initialPosInRowGroup, present, 0, numPresent - numAdvanced);
            }
            numPresent -= numAdvanced;
        }
        if (lengths != null) {
            if (lengthIdx < numLengths) {
                System.arraycopy(lengths, lengthIdx, lengths, 0, numLengths - lengthIdx);
            }
            numLengths -= lengthIdx;
            lengthIdx = 0;
        }
        if (outputQualifyingSet != null) {
            outputQualifyingSet.setEnd(posInRowGroup);
            outputQualifyingSet.setPositionCount(numResults);
        }
        if (outputChannel != -1) {
            numValues += numResults;
        }
    }

    protected void openRowGroup()
            throws IOException
    {
        posInRowGroup = 0;
        numLengths = 0;
        numPresent = 0;
        lengthIdx = 0;
        rowGroupOpen = true;
    }

    // Returns the number of non-null rows to skip to go from 'begin' to 'end'. 'begin' and 'end' are offsets from 'posInRowGroup'.
    protected int countPresent(int begin, int end)
    {
        if (presentStream == null) {
            return end - begin;
        }
        int count = 0;
        for (int i = begin; i < end; i++) {
            if (present[i]) {
                count++;
            }
        }
        return count;
    }

    protected void checkEnoughValues(int numFirstRows)
    {
        if (numValues < numFirstRows) {
            throw new IllegalArgumentException("Reader does not have enough rows");
        }
    }
}
