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
package com.facebook.presto.orc;

import com.facebook.presto.memory.context.AggregatedMemoryContext;

import static com.facebook.presto.memory.context.AggregatedMemoryContext.newSimpleAggregatedMemoryContext;
import static java.util.Objects.requireNonNull;

/*
 * This class is a copy of HiveOrcAggregatedMemoryContext.
 * Any changes made to HiveOrcAggregatedMemoryContext should be made here as well
 */
public class TestingHiveOrcAggregatedMemoryContext
        implements OrcAggregatedMemoryContext
{
    private final AggregatedMemoryContext delegate;

    public TestingHiveOrcAggregatedMemoryContext()
    {
        this(newSimpleAggregatedMemoryContext());
    }

    private TestingHiveOrcAggregatedMemoryContext(AggregatedMemoryContext delegate)
    {
        this.delegate = requireNonNull(delegate, "delegate is null");
    }

    @Override
    public OrcAggregatedMemoryContext newOrcAggregatedMemoryContext()
    {
        return new TestingHiveOrcAggregatedMemoryContext(delegate.newAggregatedMemoryContext());
    }

    @Override
    public OrcLocalMemoryContext newOrcLocalMemoryContext(String allocationTag)
    {
        return new TestingHiveOrcLocalMemoryContext(delegate.newLocalMemoryContext(allocationTag));
    }

    @Override
    public long getBytes()
    {
        return delegate.getBytes();
    }

    @Override
    public void close()
    {
        delegate.close();
    }
}
