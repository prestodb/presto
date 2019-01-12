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
package io.prestosql.plugin.thrift.api.datatypes;

import io.prestosql.plugin.thrift.api.PrestoThriftBlock;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.connector.RecordCursor;
import io.prestosql.spi.connector.RecordSet;
import io.prestosql.spi.type.Type;

import java.util.function.BiFunction;

import static com.google.common.base.Preconditions.checkState;

final class PrestoThriftTypeUtils
{
    private PrestoThriftTypeUtils()
    {
    }

    public static PrestoThriftBlock fromLongBasedBlock(Block block, Type type, BiFunction<boolean[], long[], PrestoThriftBlock> result)
    {
        int positions = block.getPositionCount();
        if (positions == 0) {
            return result.apply(null, null);
        }
        boolean[] nulls = null;
        long[] longs = null;
        for (int position = 0; position < positions; position++) {
            if (block.isNull(position)) {
                if (nulls == null) {
                    nulls = new boolean[positions];
                }
                nulls[position] = true;
            }
            else {
                if (longs == null) {
                    longs = new long[positions];
                }
                longs[position] = type.getLong(block, position);
            }
        }
        return result.apply(nulls, longs);
    }

    public static PrestoThriftBlock fromLongBasedColumn(RecordSet recordSet, int columnIndex, int positions, BiFunction<boolean[], long[], PrestoThriftBlock> result)
    {
        if (positions == 0) {
            return result.apply(null, null);
        }
        boolean[] nulls = null;
        long[] longs = null;
        RecordCursor cursor = recordSet.cursor();
        for (int position = 0; position < positions; position++) {
            checkState(cursor.advanceNextPosition(), "cursor has less values than expected");
            if (cursor.isNull(columnIndex)) {
                if (nulls == null) {
                    nulls = new boolean[positions];
                }
                nulls[position] = true;
            }
            else {
                if (longs == null) {
                    longs = new long[positions];
                }
                longs[position] = cursor.getLong(columnIndex);
            }
        }
        checkState(!cursor.advanceNextPosition(), "cursor has more values than expected");
        return result.apply(nulls, longs);
    }

    public static PrestoThriftBlock fromIntBasedBlock(Block block, Type type, BiFunction<boolean[], int[], PrestoThriftBlock> result)
    {
        int positions = block.getPositionCount();
        if (positions == 0) {
            return result.apply(null, null);
        }
        boolean[] nulls = null;
        int[] ints = null;
        for (int position = 0; position < positions; position++) {
            if (block.isNull(position)) {
                if (nulls == null) {
                    nulls = new boolean[positions];
                }
                nulls[position] = true;
            }
            else {
                if (ints == null) {
                    ints = new int[positions];
                }
                ints[position] = (int) type.getLong(block, position);
            }
        }
        return result.apply(nulls, ints);
    }

    public static PrestoThriftBlock fromIntBasedColumn(RecordSet recordSet, int columnIndex, int positions, BiFunction<boolean[], int[], PrestoThriftBlock> result)
    {
        if (positions == 0) {
            return result.apply(null, null);
        }
        boolean[] nulls = null;
        int[] ints = null;
        RecordCursor cursor = recordSet.cursor();
        for (int position = 0; position < positions; position++) {
            checkState(cursor.advanceNextPosition(), "cursor has less values than expected");
            if (cursor.isNull(columnIndex)) {
                if (nulls == null) {
                    nulls = new boolean[positions];
                }
                nulls[position] = true;
            }
            else {
                if (ints == null) {
                    ints = new int[positions];
                }
                ints[position] = (int) cursor.getLong(columnIndex);
            }
        }
        checkState(!cursor.advanceNextPosition(), "cursor has more values than expected");
        return result.apply(nulls, ints);
    }

    public static int totalSize(boolean[] nulls, int[] sizes)
    {
        int numberOfRecords;
        if (nulls != null) {
            numberOfRecords = nulls.length;
        }
        else if (sizes != null) {
            numberOfRecords = sizes.length;
        }
        else {
            numberOfRecords = 0;
        }
        int total = 0;
        for (int i = 0; i < numberOfRecords; i++) {
            if (nulls == null || !nulls[i]) {
                total += sizes[i];
            }
        }
        return total;
    }

    public static int[] calculateOffsets(int[] sizes, boolean[] nulls, int totalRecords)
    {
        if (sizes == null) {
            return new int[totalRecords + 1];
        }
        int[] offsets = new int[totalRecords + 1];
        offsets[0] = 0;
        for (int i = 0; i < totalRecords; i++) {
            int size = nulls != null && nulls[i] ? 0 : sizes[i];
            offsets[i + 1] = offsets[i] + size;
        }
        return offsets;
    }

    public static boolean sameSizeIfPresent(boolean[] nulls, int[] sizes)
    {
        return nulls == null || sizes == null || nulls.length == sizes.length;
    }
}
