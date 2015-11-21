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
import com.google.common.base.MoreObjects;

public class RoundRobinPartitionGenerator
    implements PartitionGenerator
{
    private int counter;

    public RoundRobinPartitionGenerator() {}

    @Override
    public int getPartitionBucket(int partitionCount, int position, Page page)
    {
        int bucket = counter % partitionCount;
        counter = (counter + 1) & 0x7fff_ffff;
        return bucket;
    }

    @Override
    public String toString()
    {
        return MoreObjects.toStringHelper(this)
                .add("counter", counter)
                .toString();
    }
}
