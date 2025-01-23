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

import com.facebook.presto.common.Page;

public interface HashGenerator
{
    long hashPosition(int position, Page page);

    default int getPartition(int partitionCount, int position, Page page)
    {
        long rawHash = hashPosition(position, page);

        // This function reduces the 64 bit rawHash to [0, partitionCount) uniformly. It first reduces the rawHash to 32 bit
        // integer x then normalize it to x / 2^32 * partitionCount to reduce the range of x from [0, 2^32) to [0, partitionCount)
        return (int) ((Integer.toUnsignedLong(Long.hashCode(rawHash)) * partitionCount) >> 32);
    }
}
