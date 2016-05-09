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
package com.facebook.presto.operator.exchange;

import com.facebook.presto.operator.HashGenerator;
import com.facebook.presto.spi.Page;
import io.airlift.slice.XxHash64;

import static com.google.common.base.Preconditions.checkArgument;

public class LocalPartitionGenerator
{
    private final HashGenerator hashGenerator;
    private final int hashMask;

    public LocalPartitionGenerator(HashGenerator hashGenerator, int partitionCount)
    {
        this.hashGenerator = hashGenerator;
        checkArgument(Integer.bitCount(partitionCount) == 1, "partitionCount must be a power of 2");
        hashMask = partitionCount - 1;
    }

    public int getPartition(int position, Page page)
    {
        long rawHash = getRawHash(position, page);
        return processRawHash(rawHash) & hashMask;
    }

    public long getRawHash(int position, Page page)
    {
        return hashGenerator.hashPosition(position, page);
    }

    public int getPartition(long rawHash)
    {
        return processRawHash(rawHash) & hashMask;
    }

    private static int processRawHash(long rawHash)
    {
        // mix the bits so we don't use the same hash used to distribute between stages
        return (int) XxHash64.hash(Long.reverse(rawHash));
    }
}
