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

import com.facebook.presto.spi.block.BlockCursor;

public interface PagesHashStrategy
{
    int getChannelCount();

    void appendTo(int blockIndex, int blockPosition, PageBuilder pageBuilder, int outputChannelOffset);

    int hashPosition(int blockIndex, int blockPosition);

    boolean positionEqualsCursors(int blockIndex, int blockPosition, BlockCursor[] cursors);

    boolean positionEqualsPosition(int leftBlockIndex, int leftBlockPosition, int rightBlockIndex, int rightBlockPosition);
}
