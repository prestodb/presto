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

import com.facebook.presto.spi.Page;

import static com.google.common.base.Preconditions.checkState;

public interface HashGenerator
    extends PartitionGenerator
{
    int hashPosition(int position, Page page);

    default int getPartitionBucket(int partitionCount, int position, Page page)
    {
        int rawHash = hashPosition(position, page);

        // clear the sign bit
        rawHash &= 0x7fff_ffffL;

        int bucket = rawHash % partitionCount;
        checkState(bucket >= 0 && bucket < partitionCount);
        return bucket;
    }
}
