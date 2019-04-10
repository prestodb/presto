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

import com.facebook.presto.orc.QualifyingSet;
import com.facebook.presto.orc.stream.BooleanInputStream;
import com.facebook.presto.orc.stream.LongInputStream;

import java.io.IOException;
import java.util.Arrays;
import java.util.OptionalInt;

import static com.facebook.presto.orc.ResizedArrays.resize;
import static com.google.common.base.Verify.verify;

abstract class NullWrappingColumnReader
        extends ColumnReader
{
    protected QualifyingSet innerQualifyingSet;
    protected boolean hasNulls;
    protected int innerPosInRowGroup;
    protected int numInnerRows;
    protected int[] nullsToAdd;
    protected int[] nullsToAddIndexes;
    protected int numNullsToAdd;
    // Number of elements retrieved from inner reader.
    protected int numInnerResults;
    private int[] tempInt;

    protected NullWrappingColumnReader(OptionalInt fixedValueSize)
    {
        super(fixedValueSize);
    }

    protected void beginScan(BooleanInputStream presentStream, LongInputStream lengthStream)
            throws IOException
    {
        super.beginScan(presentStream, lengthStream);
        numInnerResults = 0;
    }

    // Translates the positions of inputQualifyingSet between
    // beginPosition and endPosition into an inner qualifying set for
    // the non-null content.
    protected void makeInnerQualifyingSet()
    {
        if (presentStream == null) {
            numInnerRows = inputQualifyingSet.getPositionCount();
            hasNulls = false;
            return;
        }
        hasNulls = true;
        if (innerQualifyingSet == null) {
            innerQualifyingSet = new QualifyingSet();
        }
        int[] inputRows = inputQualifyingSet.getPositions();
        int numActive = inputQualifyingSet.getPositionCount();

        innerQualifyingSet.reset(numActive);
        int prevRow = 0;
        int prevInner = innerPosInRowGroup;
        numNullsToAdd = 0;
        boolean keepNulls = filter == null || (deterministicFilter && filter.testNull());
        for (int activeIdx = 0; activeIdx < numActive; activeIdx++) {
            int row = inputRows[activeIdx] - posInRowGroup;
            if (!present[row]) {
                if (keepNulls) {
                    addNullToKeep(inputRows[activeIdx], activeIdx);
                }
            }
            else {
                prevInner += countPresent(prevRow, row);
                prevRow = row;
                innerQualifyingSet.append(prevInner, activeIdx);
            }
        }
        numInnerRows = innerQualifyingSet.getPositionCount();
        int skip = countPresent(prevRow, inputQualifyingSet.getEnd() - posInRowGroup);
        innerQualifyingSet.setEnd(skip + prevInner);
    }

    protected void addNullToKeep(int position, int inputIndex)
    {
        if (nullsToAdd == null || nullsToAdd.length <= numNullsToAdd) {
            nullsToAdd = resize(nullsToAdd, numNullsToAdd);
        }

        if (nullsToAddIndexes == null || nullsToAddIndexes.length <= nullsToAdd.length) {
            nullsToAddIndexes = resize(nullsToAddIndexes, numNullsToAdd);
        }

        nullsToAdd[numNullsToAdd] = position;
        nullsToAddIndexes[numNullsToAdd] = inputIndex;
        numNullsToAdd++;
    }

    protected abstract void shiftUp(int from, int to);

    // When nulls are inserted into results, this is called for the
    // positions that are null, after moving the value with shiftUp().
    protected abstract void writeNull(int position);

    private void ensureValueIsNullCapacity(int capacity)
    {
        if (valueIsNull == null || valueIsNull.length < capacity) {
            valueIsNull = resize(valueIsNull, capacity);
        }
    }

    // Inserts nulls into the sequence of non-null results produced by
    // the inner reader. Because the inner reader may have run out of
    // budget before producing the full result, this only processes
    // nulls on rows beginRow to endRow, exclusive.
    protected void addNullsAfterScan(QualifyingSet output, int endRow)
    {
        if (numNullsToAdd == 0) {
            if (valueIsNull != null) {
                ensureValueIsNullCapacity(numValues + numInnerResults);
                Arrays.fill(valueIsNull, numValues, numValues + numInnerResults, false);
            }
            numResults = numInnerResults;
            return;
        }
        int savedNullsToAdd = numNullsToAdd;
        int lastNull = Arrays.binarySearch(nullsToAdd, 0, numNullsToAdd, endRow);
        if (lastNull < 0) {
            lastNull = -1 - lastNull;
        }
        numNullsToAdd = lastNull;
        if (numNullsToAdd == 0 && valueIsNull == null) {
            numResults = numInnerResults;
        }
        else {
            addNullsAfterRead(output);
        }
        int nullsLeft = savedNullsToAdd - numNullsToAdd;
        System.arraycopy(nullsToAdd, numNullsToAdd, nullsToAdd, 0, nullsLeft);
        System.arraycopy(nullsToAddIndexes, numNullsToAdd, nullsToAddIndexes, 0, nullsLeft);
        numResults = numInnerResults + numNullsToAdd;
        numNullsToAdd = nullsLeft;
    }

    private void addNullsAfterRead(QualifyingSet output)
    {
        ensureValueIsNullCapacity(numValues + numInnerResults + numNullsToAdd);
        int end = numValues + numInnerResults + numNullsToAdd;
        Arrays.fill(valueIsNull, numValues, end, false);
        if (numNullsToAdd == 0) {
            return;
        }
        int[] outRows = output.getPositions();
        int resultIdx = filter != null ? numInnerResults - 1 : output.getPositionCount() - 1;
        int nullIdx = numNullsToAdd - 1;
        // If there are filters, the qualifying rows are disjoin of
        // the null rows, else the null rows are a subset of the
        // qualifying.
        int targetIdx = numValues + numNullsToAdd + numInnerResults - 1;
        while (nullIdx >= 0 || resultIdx >= 0) {
            if (nullIdx < 0) {
                resultIdx--;
                targetIdx--;
                continue;
            }
            if (resultIdx < 0) {
                valueIsNull[targetIdx--] = true;
                nullIdx--;
                continue;
            }
            if (outRows[resultIdx] > nullsToAdd[nullIdx]) {
                resultIdx--;
                targetIdx--;
                continue;
            }
            if (outRows[resultIdx] == nullsToAdd[nullIdx]) {
                verify(filter == null);
                valueIsNull[targetIdx--] = true;
                resultIdx--;
                nullIdx--;
                continue;
            }
            verify(filter != null);
            valueIsNull[targetIdx--] = true;
            nullIdx--;
        }
        verify(targetIdx == numValues - 1);

        if (outputQualifyingSet != null) {
            outputQualifyingSet.insert(nullsToAdd, nullsToAddIndexes, numNullsToAdd);
        }

        if (outputChannelSet) {
            int sourceRow = numInnerResults - 1;

            for (int i = numInnerResults + numNullsToAdd - 1; i >= 0; i--) {
                if (!valueIsNull[i + numValues]) {
                    shiftUp(sourceRow + numValues, i + numValues);
                    sourceRow--;
                }
                else {
                    writeNull(i + numValues);
                }
            }
        }
    }

    @Override
    protected void openRowGroup()
            throws IOException
    {
        innerPosInRowGroup = 0;
        super.openRowGroup();
    }
}
