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
package com.facebook.presto.hive.parquet.memory;

public abstract class AbstractAggregatedMemoryContext
{
    // This class should remain exactly the same as AbstractAggregatedMemoryContext in com.facebook.presto.memory

    // AbstractMemoryContext class is only necessary because we need implementations that bridge
    // AggregatedMemoryContext with the existing memory tracking APIs in XxxxContext. Once they
    // are refactored, there will be only one implementation of this abstract class, and this class
    // can be removed.

    protected abstract void updateBytes(long bytes);

    public AggregatedMemoryContext newAggregatedMemoryContext()
    {
        return new AggregatedMemoryContext(this);
    }

    public LocalMemoryContext newLocalMemoryContext()
    {
        return new LocalMemoryContext(this);
    }
}
