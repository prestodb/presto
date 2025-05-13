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
import com.facebook.presto.common.type.Type;
import com.facebook.presto.memory.context.AggregatedMemoryContext;
import com.facebook.presto.spiller.Spiller;
import com.facebook.presto.spiller.SpillerFactory;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ListenableFuture;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.util.concurrent.Futures.immediateFuture;

public class DummySpillerFactory
        implements SpillerFactory
{
    private long spillsCount;

    @Override
    public Spiller create(List<Type> types, SpillContext spillContext, AggregatedMemoryContext memoryContext)
    {
        return new Spiller()
        {
            private final List<Iterable<Page>> spills = new ArrayList<>();

            @Override
            public ListenableFuture<?> spill(Iterator<Page> pageIterator)
            {
                spillsCount++;
                spills.add(ImmutableList.copyOf(pageIterator));
                return immediateFuture(null);
            }

            @Override
            public List<Iterator<Page>> getSpills()
            {
                return spills.stream()
                        .map(Iterable::iterator)
                        .collect(toImmutableList());
            }

            @Override
            public void commit()
            {
            }

            @Override
            public void close()
            {
                spills.clear();
            }
        };
    }

    public long getSpillsCount()
    {
        return spillsCount;
    }
}
