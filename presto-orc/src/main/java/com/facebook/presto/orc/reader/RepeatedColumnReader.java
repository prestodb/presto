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

import java.util.OptionalInt;

import static com.facebook.presto.orc.ResizedArrays.newIntArrayForReuse;
import static com.facebook.presto.orc.ResizedArrays.resize;
import static com.google.common.base.Verify.verify;

abstract class RepeatedColumnReader
        extends NullWrappingColumnReader
{
    // Starting offset of each result in the element reader's Block.
    protected int[] elementOffset = new int[1];
    // Length of each row in the input QualifyingSet.
    protected int[] elementLength = new int[1];
    // Start of each row of inputQualifyingSet in the inner  data.
    protected int[] elementStart = new int[1];

    // Used for compactValues of repeated content.
    protected int[] innerSurviving = new int[1];
    protected int numInnerSurviving;
    protected int innerSurvivingBase;

    // Number of rows of nested content read. This is after applying any pushed down filters.
    protected long numNestedRowsRead;
    // Number of arrays/maps read after applying pushed down filters.
    protected long numContainerRowsRead;

    RepeatedColumnReader()
    {
        super(OptionalInt.empty());
    }

    protected int getInnerPosition(int position)
    {
        return elementOffset[position];
    }

    private void computeInnerSurviving(int[]surviving, int base, int numSurviving)
    {
        innerSurvivingBase = elementOffset[base];
        if (numSurviving == 0) {
            numInnerSurviving = 0;
            return;
        }
        int numInner = 0;
        for (int i = 0; i < numSurviving; i++) {
            int position = surviving[i] + base;
            numInner += elementOffset[position + 1] - elementOffset[position];
        }
        if (innerSurviving.length < numInner) {
            innerSurviving = newIntArrayForReuse(numInner);
        }
        numInnerSurviving = numInner;
        int fill = 0;
        for (int i = 0; i < numSurviving; i++) {
            int position = surviving[i] + base;
            int startIndex = elementOffset[position];
            int endIndex = elementOffset[position + 1];
            for (int innerPosition = startIndex; innerPosition < endIndex; innerPosition++) {
                innerSurviving[fill++] = innerPosition - innerSurvivingBase;
            }
        }
    }

    @Override
    protected void makeInnerQualifyingSet()
    {
        hasNulls = presentStream != null;
        int nonNullRowIndex = 0;
        boolean nonDeterministic = filter != null && !deterministicFilter;
        if (innerQualifyingSet == null) {
            innerQualifyingSet = new QualifyingSet();
        }
        innerQualifyingSet.setParent(inputQualifyingSet);
        int[] inputRows = inputQualifyingSet.getPositions();
        int numActive = inputQualifyingSet.getPositionCount();
        if (elementLength.length < numActive) {
            elementLength = newIntArrayForReuse(numActive);
            elementStart = newIntArrayForReuse(numActive);
        }
        innerQualifyingSet.reset(countInnerActive());
        int prevRow = 0;
        int prevInner = innerPosInRowGroup;
        numNullsToAdd = 0;
        boolean keepNulls = filter == null || (!nonDeterministic && filter.testNull());
        for (int activeIndex = 0; activeIndex < numActive; activeIndex++) {
            int row = inputRows[activeIndex] - posInRowGroup;
            if (presentStream != null && !present[row]) {
                elementLength[activeIndex] = 0;
                elementStart[activeIndex] = prevInner;
                if (keepNulls || (nonDeterministic && testNullAt(row))) {
                    addNullToKeep(inputRows[activeIndex], activeIndex);
                }
            }
            else {
                prevInner += innerDistance(prevRow, row, nonNullRowIndex);
                nonNullRowIndex += countPresent(prevRow, row);
                prevRow = row;
                int length = lengths[nonNullRowIndex];
                elementLength[activeIndex] = length;
                elementStart[activeIndex] = prevInner;
                innerQualifyingSet.appendRange(prevInner, activeIndex, length);
            }
        }
        numInnerRows = innerQualifyingSet.getPositionCount();
        int skip = innerDistance(prevRow, inputQualifyingSet.getEnd() - posInRowGroup, nonNullRowIndex);
        innerQualifyingSet.setEnd(skip + prevInner);
        skip = countPresent(prevRow, inputQualifyingSet.getEnd() - posInRowGroup);
        lengthIdx = nonNullRowIndex + skip;
    }

    // Returns the number of nested rows to skip to go from
    // 'outerBegin' to 'outerEnd'. 'outerBegin' and 'outerEnd' are
    // offsets from 'posInRowGroup' of the map/list
    // reader. nonNullRowIndex is the number of non-null map/list rows
    // before outerBegin.
    private int innerDistance(int outerBegin, int outerEnd, int nonNullRowIndex)
    {
        int distance = 0;
        int numPresent = countPresent(outerBegin, outerEnd);
        for (int ctr = 0; ctr < numPresent; ctr++) {
            distance += lengths[nonNullRowIndex + ctr];
        }
        return distance;
    }

    private int countInnerActive()
    {
        int[] inputRows = inputQualifyingSet.getPositions();
        int numActive = inputQualifyingSet.getPositionCount();
        int nonNullRowIndex = 0;
        int total = 0;
        int prevRow = 0;
        for (int i = 0; i < numActive; i++) {
            int row = inputRows[i] - posInRowGroup;
            if (presentStream != null && !present[row]) {
                continue;
            }
            int distance = countPresent(prevRow, row);
            nonNullRowIndex += distance;
            total += lengths[nonNullRowIndex];
            prevRow = row;
        }
        return total;
    }

    @Override
    public void erase(int end)
    {
        if (outputChannel == -1 || numValues == 0) {
            return;
        }
        int innerEnd = getInnerPosition(end);
        numValues -= end;
        if (valueIsNull != null) {
            System.arraycopy(valueIsNull, end, valueIsNull, 0, numValues);
        }
        System.arraycopy(elementOffset, end, elementOffset, 0, numValues);
        for (int i = 0; i < numValues; i++) {
            verify(elementOffset[i] >= innerEnd);
            elementOffset[i] -= innerEnd;
        }
        eraseContent(innerEnd);
    }

    @Override
    public void compactValues(int[] surviving, int base, int numSurviving)
    {
        if (outputChannel != -1) {
            computeInnerSurviving(surviving, base, numSurviving);
            int elementBase = getInnerPosition(base);
            for (int i = 0; i < numSurviving; i++) {
                int survivingRow = surviving[i] + base;
                if (valueIsNull != null && valueIsNull[survivingRow]) {
                    valueIsNull[base + i] = true;
                    elementOffset[base + i] = elementBase;
                }
                else {
                    if (valueIsNull != null) {
                        valueIsNull[base + i] = false;
                    }
                    elementOffset[base + i] = elementBase;
                    elementBase += elementOffset[survivingRow + 1] - elementOffset[survivingRow];
                }
            }
            elementOffset[base + numSurviving] = elementBase;
            compactContent(innerSurviving, innerSurvivingBase, numInnerSurviving);
            numValues = base + numSurviving;
        }
        compactQualifyingSet(surviving, numSurviving);
    }

    protected abstract void eraseContent(int innerEnd);

    protected abstract void compactContent(int[] innerSurviving, int innerSurvivingBase, int numInnerSurviving);

    protected void ensureValuesCapacity(int numAdded)
    {
        if (valueIsNull == null || valueIsNull.length < numValues + numAdded) {
            valueIsNull = resize(valueIsNull, numValues + numAdded);
        }
        if (elementOffset == null || elementOffset.length < numValues + numAdded + 1) {
            elementOffset = resize(elementOffset, numValues + numAdded + 1);
        }
    }
}
