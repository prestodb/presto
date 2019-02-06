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

import static com.google.common.base.Verify.verify;

abstract class NullWrappingColumnReader
        extends ColumnReader
        implements StreamReader
{
    QualifyingSet innerQualifyingSet;
    boolean hasNulls;
    int innerPosInRowGroup;
    int numInnerRows;
    int[] nullsToAdd;
    int numNullsToAdd;
    // Number of elements retrieved from inner reader.
    int numInnerResults;

    protected void beginScan(BooleanInputStream presentStream, LongInputStream lengthStream)
            throws IOException
    {
        super.beginScan(presentStream, lengthStream);
        numInnerResults = 0;
    }

    // Translates the positions of inputQualifyingSet between
    // beginPosition and endPosition into an inner qualifying set for
    // the non-null content.
    protected void makeInnerQualifyingSets(int beginPosition, int endPosition)
    {
        if (presentStream == null) {
            numInnerRows = endPosition - beginPosition;
            hasNulls = false;
            return;
        }
        hasNulls = true;
        numInnerRows = 0;
        if (innerQualifyingSet == null) {
            innerQualifyingSet = new QualifyingSet();
        }
        QualifyingSet input = inputQualifyingSet;
        int[] inputRows = input.getPositions();
        int numActive = Math.min(endPosition, input.getPositionCount());
        QualifyingSet inner = innerQualifyingSet;
        int[] innerRows = inner.getMutablePositions(numActive);
        int[] innerToOuter = inner.getMutableInputNumbers(numActive);
        int prevRow = posInRowGroup;
        int prevInner = innerPosInRowGroup;
        numNullsToAdd = 0;
        boolean keepNulls = filter == null || filter.testNull();
        for (int activeIdx = beginPosition; activeIdx < endPosition; activeIdx++) {
            int row = inputRows[activeIdx] - posInRowGroup;
            if (!present[row]) {
                if (keepNulls) {
                    addNullToKeep(inputRows[activeIdx]);
                }
            }
            else {
                int distance = countPresent(prevRow, row);
                prevRow = row;
                innerToOuter[numInnerRows] = activeIdx;
                innerRows[numInnerRows++] = prevInner + distance;
                prevInner = innerRows[numInnerRows - 1];
            }
        }
        int end = endPosition < inputQualifyingSet.getPositionCount()
                ? inputRows[endPosition]
                : input.getEnd();
        int skip = countPresent(prevRow, end - posInRowGroup);
        innerQualifyingSet.setPositionCount(numInnerRows);
        innerQualifyingSet.setEnd(skip + prevInner);
    }

    private void addNullToKeep(int position)
    {
        if (nullsToAdd == null) {
            nullsToAdd = new int[100];
        }
        else if (nullsToAdd.length <= numNullsToAdd) {
            nullsToAdd = Arrays.copyOf(nullsToAdd, nullsToAdd.length * 2);
        }
        nullsToAdd[numNullsToAdd++] = position;
    }

    protected void shiftUp(int from, int to)
    {
        throw new UnsupportedOperationException("subclasses must implement");
    }

    // When nulls are inserted into results, this is called for the
    // positions that are null, after moving the value with shiftUp().
    protected void writeNull(int position)
    {
        throw new UnsupportedOperationException("subclass should implement");
    }

    private void ensureNulls(int size)
    {
        if (valueIsNull == null) {
            valueIsNull = new boolean[size];
            return;
        }
        if (valueIsNull.length < size) {
            valueIsNull = Arrays.copyOf(valueIsNull, Math.max(size, valueIsNull.length * 2));
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
                ensureNulls(numValues + numInnerResults);
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
        int end = lastNull == numNullsToAdd ? endRow : nullsToAdd[lastNull];
        numNullsToAdd = lastNull;
        if (numNullsToAdd == 0 && valueIsNull == null) {
            numResults = numInnerResults;
        }
        else {
            addNullsAfterRead(output, end);
        }
        int nullsLeft = savedNullsToAdd - numNullsToAdd;
        System.arraycopy(nullsToAdd, numNullsToAdd, nullsToAdd, 0, nullsLeft);
        numResults = numInnerResults + numNullsToAdd;
        numNullsToAdd = nullsLeft;
    }

    private void addNullsAfterRead(QualifyingSet output, int endRow)
    {
        ensureNulls(numValues + numInnerResults + numNullsToAdd);
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
        moveNonNullsAroundNulls();
    }

    private void moveNonNullsAroundNulls()
    {
        int sourceRow = numValues + numInnerResults - 1;
        int[] rows = null;
        int[] inputNumbers = null;

        int nullCtr = numNullsToAdd - 1;
        if (outputQualifyingSet != null) {
            rows = outputQualifyingSet.getMutablePositions(numInnerResults + numNullsToAdd);
            inputNumbers = outputQualifyingSet.getMutablePositions(numInnerResults + numNullsToAdd);
            outputQualifyingSet.setPositionCount(numInnerResults + numNullsToAdd);
        }
        for (int i = numValues + numInnerResults + numNullsToAdd - 1; i >= numValues; i--) {
            if (!valueIsNull[i]) {
                if (outputChannel != -1) {
                    shiftUp(sourceRow, i);
                }
                if (rows != null) {
                    rows[i - numValues] = rows[sourceRow - numValues];
                    inputNumbers[i - numValues] = inputNumbers[sourceRow - numValues];
                }
                sourceRow--;
            }
            else {
                if (outputChannel != -1) {
                    writeNull(i);
                }
                if (rows != null) {
                    rows[i - numValues] = nullsToAdd[nullCtr];
                    inputNumbers[i - numValues] = inputQualifyingSet.findPositionAtOrAbove(nullsToAdd[nullCtr]);
                    nullCtr--;
                }
            }
        }
    }

    @Override
    protected void openRowGroup()
            throws IOException
    {
        super.openRowGroup();
        innerPosInRowGroup = 0;
    }
}
