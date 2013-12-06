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
package com.facebook.presto.operator;

import com.facebook.presto.block.Block;
import com.facebook.presto.block.BlockAssertions;
import com.facebook.presto.tuple.TupleInfo;

import java.util.List;

import static com.facebook.presto.block.BlockAssertions.COMPOSITE_SEQUENCE_TUPLE_INFO;
import static com.facebook.presto.tuple.TupleInfo.SINGLE_BOOLEAN;
import static com.facebook.presto.tuple.TupleInfo.SINGLE_DOUBLE;
import static com.facebook.presto.tuple.TupleInfo.SINGLE_LONG;
import static com.facebook.presto.tuple.TupleInfo.SINGLE_VARBINARY;

public final class SequencePageBuilder
{
    private SequencePageBuilder()
    {
    }

    public static Page createSequencePage(List<TupleInfo> tupleInfos1, int length, int... initialValues)
    {
        Block[] blocks = new Block[initialValues.length];
        for (int i = 0; i < blocks.length; i++) {
            TupleInfo tupleInfo = tupleInfos1.get(i);
            int initialValue = initialValues[i];

            if (tupleInfo.equals(SINGLE_LONG)) {
                blocks[i] = BlockAssertions.createLongSequenceBlock(initialValue, initialValue + length);
            }
            else if (tupleInfo.equals(SINGLE_DOUBLE)) {
                blocks[i] = BlockAssertions.createDoubleSequenceBlock(initialValue, initialValue + length);
            }
            else if (tupleInfo.equals(SINGLE_VARBINARY)) {
                blocks[i] = BlockAssertions.createStringSequenceBlock(initialValue, initialValue + length);
            }
            else if (tupleInfo.equals(SINGLE_BOOLEAN)) {
                blocks[i] = BlockAssertions.createBooleanSequenceBlock(initialValue, initialValue + length);
            }
            else if (tupleInfo.equals(COMPOSITE_SEQUENCE_TUPLE_INFO)) {
                blocks[i] = BlockAssertions.createCompositeTupleSequenceBlock(initialValue, initialValue + length);
            }
            else {
                throw new IllegalStateException("Unsupported tuple info " + tupleInfo);
            }
        }

        return new Page(blocks);
    }
}
