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
import io.airlift.slice.DynamicSliceOutput;
import io.airlift.slice.SliceInput;
import io.airlift.slice.SliceOutput;
import scala.Tuple2;
import scala.collection.Iterator;

import javax.annotation.concurrent.GuardedBy;

import java.nio.ByteBuffer;
import java.util.List;

import static com.google.common.base.Verify.verify;
import static java.util.Objects.requireNonNull;

public class PrestoSparkMutableRowPageInput
        implements PrestoSparkPageInput
{
    private static final int TARGET_SIZE = 1024 * 1024;
    private static final int BUFFER_SIZE = (int) (TARGET_SIZE * 1.2f);
    private static final int MAX_ROWS_PER_ZERO_COLUMN_PAGE = 10_000;

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
        // zero columns page
        if (types.isEmpty()) {
            int rowCount = 0;
            synchronized (this) {
                while (true) {
                    if (currentIteratorIndex >= rowIterators.size()) {
                        break;
                    }
                    if (rowCount >= MAX_ROWS_PER_ZERO_COLUMN_PAGE) {
                        break;
                    }
                    Iterator<Tuple2<MutablePartitionId, PrestoSparkMutableRow>> currentIterator = rowIterators.get(currentIteratorIndex);
                    if (currentIterator.hasNext()) {
                        currentIterator.next();
                        rowCount++;
                    }
                    else {
                        currentIteratorIndex++;
                    }
                }
            }
            if (rowCount == 0) {
                return null;
            }
            return new Page(rowCount);
        }

        SliceOutput output = new DynamicSliceOutput(BUFFER_SIZE);
        int rowCount = 0;
        synchronized (this) {
            while (true) {
                if (currentIteratorIndex >= rowIterators.size()) {
                    break;
                }
                if (output.size() >= TARGET_SIZE) {
                    break;
                }
                Iterator<Tuple2<MutablePartitionId, PrestoSparkMutableRow>> currentIterator = rowIterators.get(currentIteratorIndex);
                if (currentIterator.hasNext()) {
                    PrestoSparkMutableRow row = currentIterator.next()._2;
                    ByteBuffer buffer = row.getBuffer();
                    output.writeBytes(buffer.array(), buffer.arrayOffset() + buffer.position(), buffer.remaining());
                    rowCount++;
                }
                else {
                    currentIteratorIndex++;
                }
            }
        }

        if (rowCount == 0) {
            return null;
        }

        SliceInput sliceInput = output.slice().getInput();
        PageBuilder pageBuilder = new PageBuilder(types);
        while (sliceInput.isReadable()) {
            pageBuilder.declarePosition();
            for (int channel = 0; channel < types.size(); channel++) {
                BlockBuilder blockBuilder = pageBuilder.getBlockBuilder(channel);
                blockBuilder.readPositionFrom(sliceInput);
            }
        }
        Page page = pageBuilder.build();
        verify(page.getPositionCount() == rowCount, "unexpected row count: %s != %s", page.getPositionCount(), rowCount);
        return page;
    }
}
