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
package com.facebook.presto.orc.reader;

import com.facebook.presto.common.predicate.TupleDomainFilter;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.orc.OrcAggregatedMemoryContext;
import com.facebook.presto.orc.StreamDescriptor;

import javax.annotation.Nullable;

import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class SelectiveReaderContext
{
    private final StreamDescriptor streamDescriptor;
    private final boolean outputRequired;
    @Nullable
    private final Type outputType;

    @Nullable
    private final TupleDomainFilter filter;
    private final boolean nonDeterministicFilter;
    private final boolean nullsAllowed;

    private final OrcAggregatedMemoryContext systemMemoryContext;
    private final boolean isLowMemory;

    public SelectiveReaderContext(
            StreamDescriptor streamDescriptor,
            Optional<Type> outputType,
            Optional<TupleDomainFilter> filter,
            OrcAggregatedMemoryContext systemMemoryContext,
            boolean isLowMemory)
    {
        this.filter = requireNonNull(filter, "filter is null").orElse(null);
        this.streamDescriptor = requireNonNull(streamDescriptor, "streamDescriptor is null");
        this.outputRequired = requireNonNull(outputType, "outputType is null").isPresent();
        this.outputType = outputType.orElse(null);
        checkArgument(filter.isPresent() || outputRequired, "filter must be present if output is not required");
        this.systemMemoryContext = requireNonNull(systemMemoryContext, "systemMemoryContext is null");
        this.isLowMemory = isLowMemory;
        this.nonDeterministicFilter = this.filter != null && !this.filter.isDeterministic();
        this.nullsAllowed = this.filter == null || nonDeterministicFilter || this.filter.testNull();
    }

    public StreamDescriptor getStreamDescriptor()
    {
        return streamDescriptor;
    }

    public boolean isOutputRequired()
    {
        return outputRequired;
    }

    @Nullable
    public Type getOutputType()
    {
        return outputType;
    }

    @Nullable
    public TupleDomainFilter getFilter()
    {
        return filter;
    }

    public OrcAggregatedMemoryContext getSystemMemoryContext()
    {
        return systemMemoryContext;
    }

    public boolean isLowMemory()
    {
        return isLowMemory;
    }

    public boolean isNonDeterministicFilter()
    {
        return nonDeterministicFilter;
    }

    public boolean isNullsAllowed()
    {
        return nullsAllowed;
    }
}
