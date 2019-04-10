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
package com.facebook.presto.orc;

import com.facebook.presto.spi.PageSourceOptions.ErrorSet;
import com.facebook.presto.spi.PrestoException;

import java.util.Arrays;

import static com.facebook.presto.spi.StandardErrorCode.GENERIC_USER_ERROR;
import static com.google.common.base.Preconditions.checkArgument;

public class QualifyingSet
{
    // begin and end define the range of rows coverd. If a row >=
    // begin and < end and is not in positions rangeBegins[i] <= row <
    // rangeEnds[i] then row is not in the qualifying set.
    private int end;
    private int[] positions;
    private int positionCount;

    private int[] inputNumbers;
    private ErrorSet errorSet;

    static volatile int[] wholeRowGroup;
    static volatile int[] allZeros;
    private QualifyingSet parent;
    // True if the output of the scan whose input this is should be
    // expressed in row/input numbers of 'parent' of 'this'. If so,
    // 'inputNumbers' gives the translation. This is used when a
    // qualifying set is in terms of a non-null rows of a
    // struct/list/map but the results should be expressed in row
    // numbers that include the nulls.
    boolean translateResultToParentRows;

    static {
        wholeRowGroup = new int[10000];
        allZeros = new int[10000];
        Arrays.fill(allZeros, 0);
        for (int i = 0; i < 10000; i++) {
            wholeRowGroup[i] = i;
        }
    }

    private int[] ensureAllZeroesCapacity(int capacity)
    {
        int[] zeros = allZeros;
        if (zeros.length >= capacity) {
            return zeros;
        }

        int[] newZeros = new int[capacity];
        allZeros = newZeros;
        return newZeros;
    }

    private int[] ensureWholeRowGroupCapacity(int capacity)
    {
        int[] rowGroup = wholeRowGroup;
        if (rowGroup.length >= capacity) {
            return rowGroup;
        }

        // Thread safe.  If many concurrently create a new wholeRowGroup, many are created but all but one become garbage and everybody has a right size array.
        int[] newWholeRowGroup = new int[capacity];
        for (int i = 0; i < capacity; i++) {
            newWholeRowGroup[i] = i;
        }
        wholeRowGroup = newWholeRowGroup;
        return newWholeRowGroup;
    }

    public void setRange(int begin, int end)
    {
        checkArgument(begin >= 0, "begin must not be negative");
        checkArgument(begin < end, "begin must be less than end");
        this.end = end;
        int[] wholeRowGroup = ensureWholeRowGroupCapacity(end);
        if (positions == null || positions.length < end - begin) {
            positions = new int[end];
        }
        System.arraycopy(wholeRowGroup, begin, positions, 0, end - begin);

        int[] allZeroes = ensureAllZeroesCapacity(end);
        if (inputNumbers == null || inputNumbers.length < end - begin) {
            inputNumbers = Arrays.copyOf(allZeroes, end - begin);
        }
        else {
            System.arraycopy(allZeroes, begin, inputNumbers, 0, end - begin);
        }

        positionCount = end - begin;
    }

    public boolean isEmpty()
    {
        return positionCount == 0;
    }

    public int[] getPositions()
    {
        return positions;
    }

    public int[] getInputNumbers()
    {
        return inputNumbers;
    }

    public void reset(int capacity)
    {
        ensureCapacity(capacity);
        positionCount = 0;
    }

    public void ensureCapacity(int capacity)
    {
        if (positions == null) {
            positions = new int[capacity];
        }
        else if (positions.length < capacity) {
            positions = Arrays.copyOf(positions, capacity);
        }

        if (inputNumbers == null) {
            inputNumbers = new int[capacity];
        }
        else if (inputNumbers.length < capacity) {
            inputNumbers = Arrays.copyOf(inputNumbers, capacity);
        }
    }

    public void append(int position, int inputIndex)
    {
        positions[positionCount] = position;
        inputNumbers[positionCount] = inputIndex;
        positionCount++;
    }

    public void insert(int[] newPositions, int[] newInputIndexes, int newCount)
    {
        int originalIndex = positionCount - 1;
        int newIndex = newCount - 1;

        positionCount += newCount;
        ensureCapacity(positionCount);

        for (int i = positionCount - 1; i >= 0; i--) {
            if (newIndex == -1 || (originalIndex != -1 && positions[originalIndex] > newPositions[newIndex])) {
                positions[i] = positions[originalIndex];
                inputNumbers[i] = inputNumbers[originalIndex];
                originalIndex--;
            }
            else {
                positions[i] = newPositions[newIndex];
                inputNumbers[i] = newInputIndexes[newIndex];
                newIndex--;
            }
        }
    }

    // Use ensureCapacity + append
    @Deprecated
    public int[] getMutableInputNumbers(int minSize)
    {
        ensureCapacity(minSize);
        return inputNumbers;
    }

    public int getEnd()
    {
        return end;
    }

    public void setEnd(int end)
    {
        this.end = end;
    }

    public int getPositionCount()
    {
        return positionCount;
    }

    public int getTotalPositionCount()
    {
        return positionCount;
    }

    public void setPositionCount(int positionCount)
    {
        this.positionCount = positionCount;
    }

    public int findPositionAtOrAbove(int row)
    {
        int pos = Arrays.binarySearch(positions, 0, positionCount, row);
        return pos < 0 ? -1 - pos : pos;
    }

    public QualifyingSet getParent()
    {
        return parent;
    }

    public void setParent(QualifyingSet parent)
    {
        this.parent = parent;
    }

    public boolean getTranslateResultToParentRows()
    {
        return translateResultToParentRows;
    }

    public void setTranslateResultToParentRows(boolean translateResultToParentRows)
    {
        this.translateResultToParentRows = translateResultToParentRows;
    }

    public ErrorSet getErrorSet()
    {
        return errorSet;
    }

    public ErrorSet getOrCreateErrorSet()
    {
        if (errorSet == null) {
            errorSet = new ErrorSet();
        }
        return errorSet;
    }

    public void setErrorSet(ErrorSet errorSet)
    {
        this.errorSet = errorSet;
    }

    // Erases qulifying rows and corresponding input numbers below
    // position. If one of the erased positions has an error, throws
    // the error. This is used to remove a row that is past all
    // filters. Errors that were masked by subsequent filters will
    // have been compacted away before this is called.
    public void eraseBelowRow(int row)
    {
        if (positionCount == 0 || positions[positionCount - 1] < row) {
            if (errorSet != null) {
                Throwable error = errorSet.getFirstError(positionCount);
                if (error != null) {
                    throw new PrestoException(GENERIC_USER_ERROR, error);
                }
            }
            positionCount = 0;
            return;
        }
        int surviving = findPositionAtOrAbove(row);
        if (surviving == 0) {
            return;
        }
        if (errorSet != null) {
            Throwable error = errorSet.getFirstError(surviving);
            if (error != null) {
                throw new PrestoException(GENERIC_USER_ERROR, error);
            }
        }
        if (surviving == positionCount) {
            positionCount = 0;
            if (errorSet != null) {
                errorSet.clear();
            }
            return;
        }
        ensureCapacity(positionCount);
        int lowestSurvivingInput = translateResultToParentRows ? 0 : inputNumbers[surviving];
        for (int i = surviving; i < positionCount; i++) {
            positions[i - surviving] = positions[i];
            inputNumbers[i - surviving] = inputNumbers[i] - lowestSurvivingInput;
        }
        positionCount -= surviving;
        if (errorSet != null) {
            errorSet.erase(surviving);
        }
    }

    public void copyFrom(QualifyingSet other)
    {
        positionCount = other.positionCount;
        end = other.end;
        ensureCapacity(positionCount);
        System.arraycopy(other.positions, 0, positions, 0, positionCount);
        System.arraycopy(other.inputNumbers, 0, inputNumbers, 0, positionCount);
        parent = other.parent;
    }

    public void compactPositionsAndErrors(int[] surviving, int numSurviving)
    {
        for (int i = 0; i < numSurviving; i++) {
            positions[i] = positions[surviving[i]];
        }
        positionCount = numSurviving;
        if (errorSet != null && !errorSet.isEmpty()) {
            Throwable[] errors = errorSet.getErrors();
            int numErrors = errorSet.getPositionCount();
            int lastError = -1;
            for (int i = 0; i < numSurviving; i++) {
                if (surviving[i] >= numErrors) {
                    break;
                }
                errors[i] = errors[surviving[i]];
                if (errors[i] != null) {
                    lastError = i;
                }
            }
            errorSet.setErrors(errors, lastError + 1);
        }
    }

    public void compactInputNumbers(int[] surviving, int numSurviving)
    {
        for (int i = 0; i < numSurviving; i++) {
            inputNumbers[i] = inputNumbers[surviving[i]];
        }
    }

    public boolean hasErrors()
    {
        return errorSet != null && !errorSet.isEmpty();
    }

    public void check()
    {
        for (int i = 0; i < positionCount; i++) {
            int pos = positions[i];
            if (pos >= end) {
                throw new IllegalArgumentException("QualifyingSet contains past end");
            }
            if (i > 0 && positions[i - 1] >= pos) {
                throw new IllegalArgumentException("QualifyingSet contains positions out of order");
            }
        }
    }
}
