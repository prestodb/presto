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
package com.facebook.presto.operator.window;

import com.facebook.presto.spi.block.BlockBuilder;

import static com.facebook.presto.spi.type.BigintType.BIGINT;

@WindowFunctionSignature(name = "rank", returnType = "bigint")
public class RankFunction
        extends RankingWindowFunction
{
    private long rank;
    private long count;

    @Override
    public void reset()
    {
        rank = 0;
        count = 1;
    }

    @Override
    public void processRow(BlockBuilder output, boolean newPeerGroup, int peerGroupCount, int currentPosition)
    {
        if (newPeerGroup) {
            rank += count;
            count = 1;
        }
        else {
            count++;
        }
        BIGINT.writeLong(output, rank);
    }
}
