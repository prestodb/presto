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
package com.facebook.presto.operator.unnest;

import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.PageBuilderStatus;

/**
 * Unnester is a layer of abstraction between {@link UnnestOperator} and {@link UnnestBlockBuilder} to enable
 * translation of indices from input nested blocks to underlying element blocks.
 */
public interface Unnester
{
    int getChannelCount();

    void resetInput(Block block);

    void startNewOutput(PageBuilderStatus status, int expectedEntries);

    int getCurrentUnnestedLength();

    void processCurrentAndAdvance(int requiredOutputCount);

    Block[] buildOutputBlocksAndFlush();
}
