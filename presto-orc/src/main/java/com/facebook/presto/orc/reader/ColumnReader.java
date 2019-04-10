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

import com.facebook.presto.orc.BatchTooLargeException;
import com.facebook.presto.orc.Filter;
import com.facebook.presto.orc.QualifyingSet;
import com.facebook.presto.orc.stream.BooleanInputStream;
import com.facebook.presto.orc.stream.InputStreamSource;
import com.facebook.presto.orc.stream.LongInputStream;
import com.facebook.presto.spi.type.Type;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.Arrays;
import java.util.OptionalInt;

import static com.facebook.presto.orc.stream.MissingInputStreamSource.missingStreamSource;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

abstract class ColumnReader
        implements StreamReader
{
    protected InputStreamSource<BooleanInputStream> presentStreamSource = missingStreamSource(BooleanInputStream.class);

    @Nullable
    protected BooleanInputStream presentStream;

    protected QualifyingSet inputQualifyingSet;
    protected QualifyingSet outputQualifyingSet;
    protected int outputChannel = -1;
    protected boolean outputChannelSet;
    protected Filter filter;
    protected boolean deterministicFilter;
    protected int columnIndex;
    protected Type type;

    protected boolean rowGroupOpen;

    // position of first unprocessed row from the start of the row group.
    protected int posInRowGroup;

    //Present flag for each row between posInRowGroup and end of inputQualifyingSet.
    protected boolean[] present;

    // Number of values in 'present'.
    protected int numPresent;

    // Lengths for present rows from posInRowGroup to end of inputQualifyingSet.
    protected int[] lengths;

    // Number of elements in lengths.
    protected int numLengths;

    // Index of length of first unprocessed element in 'lengths'.
    protected int lengthIdx;

    // Number of values in Block to be returned by getBlock.
    protected int numValues;

    // Number of result rows in scan() so far.
    protected int numResults;

    // Number of bytes the next scan() may add to the result.
    protected long resultSizeBudget = 8 * 10000;

    // Null flags for retrieved values. At least numValues + numResults elements.
    protected boolean[] valueIsNull;

    private final OptionalInt fixedValueSize;

    protected ColumnReader(OptionalInt fixedValueSize)
    {
        this.fixedValueSize = requireNonNull(fixedValueSize, "fixedValueSize is null");
    }

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
        this.deterministicFilter = filter != null && filter.isDeterministic();
        this.outputChannel = channel;
        this.outputChannelSet = channel != -1;
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
    public int getResultSizeInBytes()
    {
        if (fixedValueSize.isPresent()) {
            return numValues * fixedValueSize.getAsInt();
        }

        throw new UnsupportedOperationException("Variable width readers must implement getResultSizeInBytes()");
    }

    @Override
    public int getAverageResultSize()
    {
        if (fixedValueSize.isPresent()) {
            return fixedValueSize.getAsInt();
        }

        throw new UnsupportedOperationException("Variable width readers must implement getAverageResultSize()");
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
        numResults = 0;
        if (filter != null && outputQualifyingSet == null) {
            outputQualifyingSet = new QualifyingSet();
        }
        int rowsInRange = inputQualifyingSet.getEnd() - posInRowGroup;
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

        if (lengths == null) {
            lengths = new int[neededLengths];
        }
        else if (lengths.length < neededLengths) {
            lengths = Arrays.copyOf(lengths, neededLengths + 100);
        }
        lengthStream.nextIntVector(neededLengths - numLengths, lengths, numLengths);
        numLengths = neededLengths;
    }

    protected void processAllNulls()
    {
        if (deterministicFilter && !filter.testNull()) {
            return;
        }

        int[] inputPositions = inputQualifyingSet.getPositions();
        for (int i = 0; i < inputQualifyingSet.getPositionCount(); i++) {
            if (filter != null) {
                if (!deterministicFilter && !filter.testNull()) {
                    continue;
                }
                outputQualifyingSet.append(inputPositions[i], i);
            }
            addNullResult();
        }
    }

    protected void addNullResult()
    {
        if (!outputChannelSet) {
            return;
        }

        int position = numValues + numResults;
        ensureValuesCapacity(position + 1, true);
        valueIsNull[position] = true;
        numResults++;
    }

    protected void ensureValuesCapacity(int capacity, boolean includeNulls)
    {
        throw new UnsupportedOperationException();
    }

    protected void endScan(BooleanInputStream presentStream)
    {
        // The reader is positioned at inputQualifyingSet.end()
        //  posInRowGroup is where the calling scan() started.
        int initialPosInRowGroup = posInRowGroup;
        posInRowGroup = inputQualifyingSet.getEnd();
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
        }
        if (outputChannelSet) {
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

    protected BatchTooLargeException batchTooLarge()
    {
        return new BatchTooLargeException();
    }

    protected void checkEnoughValues(int numFirstRows)
    {
        if (numValues < numFirstRows) {
            throw new IllegalArgumentException(format("Reader does not have enough rows: requested %s, available %s", numFirstRows, numValues));
        }
    }
}
