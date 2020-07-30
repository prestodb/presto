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
package com.facebook.presto.spark.execution;

import com.facebook.presto.common.Page;
import com.facebook.presto.common.PageBuilder;
import com.facebook.presto.common.block.BlockBuilder;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.spark.classloader_interface.MutablePartitionId;
import com.facebook.presto.spark.classloader_interface.PrestoSparkMutableRow;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.BasicSliceInput;
import io.airlift.slice.DynamicSliceOutput;
import io.airlift.slice.SliceOutput;
import scala.Tuple2;
import scala.collection.Iterator;

import javax.annotation.concurrent.GuardedBy;

import java.nio.ByteBuffer;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Verify.verify;
import static java.util.Objects.requireNonNull;

public class PrestoSparkMutableRowPageInput
        implements PrestoSparkPageInput
{
    private static final int TARGET_SIZE = 1024 * 1024;
    private static final int BUFFER_SIZE = (int) (TARGET_SIZE * 1.2f);
    private static final int MAX_ROWS_PER_PAGE = 20_000;

    private final List<Type> types;
    @GuardedBy("this")
    private final List<Iterator<Tuple2<MutablePartitionId, PrestoSparkMutableRow>>> rowIterators;
    @GuardedBy("this")
    private int currentIteratorIndex;

    public PrestoSparkMutableRowPageInput(List<Type> types, List<Iterator<Tuple2<MutablePartitionId, PrestoSparkMutableRow>>> rowIterators)
    {
        this.types = ImmutableList.copyOf(requireNonNull(types, "types is null"));
        this.rowIterators = requireNonNull(rowIterators, "rowIterators is null");
    }

    @Override
    public Page getNextPage()
    {
        SliceOutput output = new DynamicSliceOutput(types.isEmpty() ? 0 : BUFFER_SIZE);
        int rowCount = 0;
        synchronized (this) {
            while (currentIteratorIndex < rowIterators.size()) {
                Iterator<Tuple2<MutablePartitionId, PrestoSparkMutableRow>> iterator = rowIterators.get(currentIteratorIndex);
                while (iterator.hasNext() && output.size() <= TARGET_SIZE && rowCount <= MAX_ROWS_PER_PAGE) {
                    PrestoSparkMutableRow row = iterator.next()._2;
                    if (row.getBuffer() != null) {
                        ByteBuffer buffer = row.getBuffer();
                        output.writeBytes(buffer.array(), buffer.arrayOffset() + buffer.position(), buffer.remaining());
                    }
                    else if (row.getArray() != null) {
                        output.writeBytes(row.getArray(), row.getOffset(), row.getLength());
                    }
                    else {
                        throw new IllegalArgumentException("Unexpected PrestoSparkMutableRow: 'buffer' and 'array' fields are both null");
                    }
                    rowCount++;
                }
                if (!iterator.hasNext()) {
                    currentIteratorIndex++;
                }
                else {
                    break;
                }
            }
        }
        if (rowCount == 0) {
            return null;
        }
        return createPage(rowCount, output.slice().getInput(), types);
    }

    private static Page createPage(int rowCount, BasicSliceInput input, List<Type> types)
    {
        checkArgument(rowCount > 0, "rowCount must be greater than zero: %s", rowCount);
        if (input.length() == 0) {
            // zero column page
            verify(types.isEmpty(), "types is expected to be empty");
            return new Page(rowCount);
        }
        PageBuilder pageBuilder = new PageBuilder(types);
        while (input.isReadable()) {
            pageBuilder.declarePosition();
            for (int channel = 0; channel < types.size(); channel++) {
                BlockBuilder blockBuilder = pageBuilder.getBlockBuilder(channel);
                blockBuilder.readPositionFrom(input);
            }
        }
        Page page = pageBuilder.build();
        verify(page.getPositionCount() == rowCount, "unexpected row count: %s != %s", page.getPositionCount(), rowCount);
        return page;
    }
}
