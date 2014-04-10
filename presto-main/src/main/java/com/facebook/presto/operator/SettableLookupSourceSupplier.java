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

import com.facebook.presto.spi.type.Type;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;

import java.util.List;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

public final class SettableLookupSourceSupplier
        implements LookupSourceSupplier
{
    private final List<Type> types;
    private final SettableFuture<LookupSource> lookupSourceFuture = SettableFuture.create();

    public SettableLookupSourceSupplier(List<Type> types)
    {
        this.types = ImmutableList.copyOf(checkNotNull(types, "types is null"));
    }

    @Override
    public List<Type> getTypes()
    {
        return types;
    }

    @Override
    public ListenableFuture<LookupSource> getLookupSource()
    {
        return lookupSourceFuture;
    }

    public void setLookupSource(LookupSource lookupSource)
    {
        checkNotNull(lookupSource, "lookupSource is null");
        boolean wasSet = lookupSourceFuture.set(lookupSource);
        checkState(wasSet, "Lookup source already set");
    }
}
