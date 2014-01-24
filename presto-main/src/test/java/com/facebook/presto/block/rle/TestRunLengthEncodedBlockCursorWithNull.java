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
package com.facebook.presto.block.rle;

import com.facebook.presto.block.AbstractTestBlockCursor;
import com.facebook.presto.block.Block;
import com.facebook.presto.block.BlockBuilderStatus;
import com.facebook.presto.block.RandomAccessBlock;

import static com.facebook.presto.block.BlockAssertions.createStringsBlock;
import static com.facebook.presto.type.VarcharType.VARCHAR;

public class TestRunLengthEncodedBlockCursorWithNull
        extends AbstractTestBlockCursor
{
    @Override
    protected RunLengthEncodedBlockCursor createTestCursor()
    {
        RandomAccessBlock value = VARCHAR.createBlockBuilder(new BlockBuilderStatus())
                .appendNull()
                .build()
                .toRandomAccessBlock();

        return new RunLengthEncodedBlock(value, 11).cursor();
    }

    @Override
    protected Block createExpectedValues()
    {
        return createStringsBlock(
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null);
    }
}
