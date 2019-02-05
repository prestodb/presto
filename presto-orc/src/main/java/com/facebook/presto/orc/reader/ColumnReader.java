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
import com.facebook.presto.spi.type.Type;

import java.io.IOException;
import java.util.Arrays;

import static java.util.Objects.requireNonNull;

abstract class ColumnReader
        implements StreamReader
{
    protected final int expectNumValues = 10000;

    protected QualifyingSet inputQualifyingSet;
    protected QualifyingSet outputQualifyingSet;
    protected int channel = -1;
    protected Filter filter;
    protected int columnIndex;
    protected Type type;
    // First row number in row group that is not processed due to
    // reaching target size. This must occur as a position in
    // inputQualifyingSet. -1 if all inputQualifyingSet is processed.
    protected int truncationRow = -1;

    protected boolean rowGroupOpen;
    // Lengths for the rows of the input QualifyingSet.
    protected int[] lengths;
    // Number of elements in lengths.
    protected int numLengths;
    // Index of length of first unprocessed element in 'lengths'.
    protected int lengthIdx;
    // Present flag for each row in input QualifyingSet.
    protected boolean[] present;

    // Number of values in 'present'.
    private int numPresent;

    // position of first unprocessed row from the start of the row group.
    protected int positionInRowGroup;

    // Number of values in Block to be returned by getBlock. These values
    // may accumulate across multiple scan() invocations.
    protected int numValues;

    // Number of result rows in scan() so far.
    protected int numResults;

    // Number of bytes the next scan() may add to the result.
    protected long resultSizeBudget = 8 * 10000;

    @Override
    public QualifyingSet getInputQualifyingSet()
    {
        return inputQualifyingSet;
    }

    @Override
    public void setInputQualifyingSet(QualifyingSet qualifyingSet)
    {
        inputQualifyingSet = requireNonNull(qualifyingSet, "qualifyingSet is null");
    }

    @Override
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
        this.channel = channel;
        // TODO Remove columnIndex as it is used only in ColumnGroupReader::toString
        this.columnIndex = columnIndex;
        this.type = requireNonNull(type, "type is null");
    }

    @Override
    public int getChannel()
    {
        return channel;
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
        return positionInRowGroup;
    }

    // TODO Rename to trancationPosition
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

    protected void compactQualifyingSet(int[] surviving, int numSurviving)
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
            throw new IllegalStateException("beginScan called before openRowGroup");
        }
        numResults = 0;
        truncationRow = -1;
        if (filter != null && outputQualifyingSet == null) {
            outputQualifyingSet = new QualifyingSet();
        }
        int rowsInRange = inputQualifyingSet.getEnd() - positionInRowGroup;

        int numValuesPresent = 0;
        if (presentStream == null) {
            numValuesPresent = rowsInRange;
        }
        else {
            if (present == null || present.length < rowsInRange) {
                present = new boolean[rowsInRange];
            }
            if (numPresent > 0 && rowsInRange > numPresent) {
                throw new IllegalArgumentException("The present stream should be read in full the first time");
            }
            numValuesPresent = presentStream.getSetBits(rowsInRange, present);
            numPresent = rowsInRange;
        }

        if (lengthStream == null || numValuesPresent <= numLengths) {
            return;
        }

        if (lengths == null) {
            lengths = new int[numValuesPresent];
        }
        else if (lengths.length < numValuesPresent) {
            lengths = Arrays.copyOf(lengths, numValuesPresent + 100);
        }
        lengthStream.nextIntVector(numValuesPresent, lengths, 0);
        numLengths = numValuesPresent;
    }

    protected void endScan(BooleanInputStream presentStream)
    {
        // The reader is positioned at inputQualifyingSet.end() or truncationRow.
        int end = inputQualifyingSet.getEnd(); // getNonTruncatedEnd();
        positionInRowGroup = truncationRow != -1 ? truncationRow : end;

        if (presentStream != null) {
            if (positionInRowGroup != end) {
                System.arraycopy(present, positionInRowGroup, present, 0, end - positionInRowGroup);
            }
            numPresent -= end - positionInRowGroup;
        }
        if (lengths != null) {
            if (lengthIdx < numLengths) {
                System.arraycopy(lengths, lengthIdx, lengths, 0, numLengths - lengthIdx);
            }
            numLengths -= lengthIdx;
        }
        if (outputQualifyingSet != null) {
            outputQualifyingSet.setEnd(positionInRowGroup);
            outputQualifyingSet.setPositionCount(numResults);
        }
        if (channel != -1) {
            numValues += numResults;
        }
    }

    protected void openRowGroup()
            throws IOException
    {
        positionInRowGroup = 0;
        numLengths = 0;
        numPresent = 0;
        lengthIdx = 0;
        rowGroupOpen = true;
    }
}
