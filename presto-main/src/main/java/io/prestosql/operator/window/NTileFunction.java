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
package io.prestosql.operator.window;

import io.prestosql.spi.block.BlockBuilder;
import io.prestosql.spi.function.RankingWindowFunction;
import io.prestosql.spi.function.WindowFunctionSignature;

import java.util.List;

import static io.prestosql.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.util.Failures.checkCondition;

@WindowFunctionSignature(name = "ntile", returnType = "bigint", argumentTypes = "bigint")
public class NTileFunction
        extends RankingWindowFunction
{
    private final int valueChannel;
    private int rowCount;

    public NTileFunction(List<Integer> argumentChannels)
    {
        this.valueChannel = argumentChannels.get(0);
    }

    @Override
    public void reset()
    {
        rowCount = windowIndex.size();
    }

    @Override
    public void processRow(BlockBuilder output, boolean newPeerGroup, int peerGroupCount, int currentPosition)
    {
        if (windowIndex.isNull(valueChannel, currentPosition)) {
            output.appendNull();
        }
        else {
            long buckets = windowIndex.getLong(valueChannel, currentPosition);
            checkCondition(buckets > 0, INVALID_FUNCTION_ARGUMENT, "Buckets must be greater than 0");
            BIGINT.writeLong(output, bucket(buckets, currentPosition) + 1);
        }
    }

    private long bucket(long buckets, int currentRow)
    {
        if (rowCount < buckets) {
            return currentRow;
        }

        long remainderRows = rowCount % buckets;
        long rowsPerBucket = rowCount / buckets;

        // Remainder rows are assigned starting from the first bucket.
        // Thus, each of those buckets have an additional row.
        if (currentRow < ((rowsPerBucket + 1) * remainderRows)) {
            return currentRow / (rowsPerBucket + 1);
        }

        // Shift the remaining rows to account for the remainder rows.
        return (currentRow - remainderRows) / rowsPerBucket;
    }
}
