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
import com.facebook.presto.orc.stream.LongInputStream;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.type.Type;

import java.io.IOException;
import java.util.Arrays;

abstract class ColumnReader
        implements StreamReader
{
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
    // Lengths for the rows of the input QualifyingSet.
    int[] lengths;
    // Number of elements in lengths.
    int numLengths;
    // Index of length of first unprocessed element in 'lemgths'.
    int lengthIdx;
    //Present flag for each row in input QualifyingSet.
    boolean[] present;

    // Number of values in 'present'.
    int numPresent;

    // position of first unprocessed row from the start of the row group.
    int posInRowGroup;

    // Number of values in Block to be returned by getBlock.
    int numValues;

    // Number of result rows in scan() so far.
    int numResults;

    // Number of bytes the next scan() may add to the result.
    long resultSizeBudget = 8 * 10000;

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
        if (!rowGroupOpen) {
            openRowGroup();
        }
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
            presentStream.getSetBits(rowsInRange, present);
            numPresent = rowsInRange;
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
        // The reader is positioned at inputQualifyingSet.end() or truncationRow.
        int end = inputQualifyingSet.getEnd(); // getNonTruncatedEnd();
        posInRowGroup = truncationRow != -1 ? truncationRow : end;

        if (presentStream != null) {
            if (posInRowGroup != end) {
                System.arraycopy(present, posInRowGroup, present, 0, end - posInRowGroup);
            }
            numPresent -= end - posInRowGroup;
        }
        if (lengths != null) {
            if (lengthIdx < numLengths) {
                System.arraycopy(lengths, lengthIdx, lengths, 0, numLengths - lengthIdx);
            }
            numLengths -= lengthIdx;
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
}
