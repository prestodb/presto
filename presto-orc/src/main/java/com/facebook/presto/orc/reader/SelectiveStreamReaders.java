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

import com.facebook.presto.memory.context.AggregatedMemoryContext;
import com.facebook.presto.orc.StreamDescriptor;
import com.facebook.presto.orc.TupleDomainFilter;
import com.facebook.presto.orc.metadata.OrcType.OrcTypeKind;
import com.facebook.presto.spi.Subfield;
import com.facebook.presto.spi.type.Type;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import org.joda.time.DateTimeZone;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.String.format;

public final class SelectiveStreamReaders
{
    private SelectiveStreamReaders() {}

    public static SelectiveStreamReader createStreamReader(
            StreamDescriptor streamDescriptor,
            Map<Subfield, TupleDomainFilter> filters,
            Optional<Type> outputType,
            List<Subfield> requiredSubfields,
            DateTimeZone hiveStorageTimeZone,
            AggregatedMemoryContext systemMemoryContext)
    {
        OrcTypeKind type = streamDescriptor.getStreamType();
        switch (type) {
            case BOOLEAN: {
                checkArgument(requiredSubfields.isEmpty(), "Boolean stream reader doesn't support subfields");
                return new BooleanSelectiveStreamReader(streamDescriptor, getOptionalOnlyFilter(type, filters), outputType.isPresent(), systemMemoryContext.newLocalMemoryContext(SelectiveStreamReaders.class.getSimpleName()));
            }
            case BYTE: {
                checkArgument(requiredSubfields.isEmpty(), "Byte stream reader doesn't support subfields");
                return new ByteSelectiveStreamReader(streamDescriptor, getOptionalOnlyFilter(type, filters), outputType.isPresent(), systemMemoryContext.newLocalMemoryContext(SelectiveStreamReaders.class.getSimpleName()));
            }
            case SHORT:
            case INT:
            case LONG:
            case DATE: {
                checkArgument(requiredSubfields.isEmpty(), "Primitive type stream reader doesn't support subfields");
                return new LongSelectiveStreamReader(streamDescriptor, getOptionalOnlyFilter(type, filters), outputType, systemMemoryContext);
            }
            case FLOAT: {
                checkArgument(requiredSubfields.isEmpty(), "Float type stream reader doesn't support subfields");
                return new FloatSelectiveStreamReader(streamDescriptor, getOptionalOnlyFilter(type, filters), outputType.isPresent(), systemMemoryContext.newLocalMemoryContext(SelectiveStreamReaders.class.getSimpleName()));
            }
            case DOUBLE:
                checkArgument(requiredSubfields.isEmpty(), "Double stream reader doesn't support subfields");
                return new DoubleSelectiveStreamReader(streamDescriptor, getOptionalOnlyFilter(type, filters), outputType.isPresent(), systemMemoryContext.newLocalMemoryContext(SelectiveStreamReaders.class.getSimpleName()));
            case BINARY:
            case STRING:
            case VARCHAR:
            case CHAR:
                throw new IllegalArgumentException("Unsupported type: " + streamDescriptor.getStreamType());
            case TIMESTAMP: {
                checkArgument(requiredSubfields.isEmpty(), "Timestamp stream reader doesn't support subfields");
                return new TimestampSelectiveStreamReader(streamDescriptor, getOptionalOnlyFilter(type, filters), hiveStorageTimeZone, outputType.isPresent(), systemMemoryContext.newLocalMemoryContext(SelectiveStreamReaders.class.getSimpleName()));
            }
            case LIST:
                return new ListSelectiveStreamReader(streamDescriptor, filters, requiredSubfields, null, 0, outputType, hiveStorageTimeZone, systemMemoryContext);
            case STRUCT:
                return new StructSelectiveStreamReader(streamDescriptor, filters, requiredSubfields, outputType, hiveStorageTimeZone, systemMemoryContext);
            case MAP:
            case DECIMAL:
            case UNION:
            default:
                throw new IllegalArgumentException("Unsupported type: " + type);
        }
    }

    private static Optional<TupleDomainFilter> getOptionalOnlyFilter(OrcTypeKind type, Map<Subfield, TupleDomainFilter> filters)
    {
        if (filters.isEmpty()) {
            return Optional.empty();
        }

        checkArgument(filters.size() == 1, format("Stream reader for %s doesn't support multiple range filters", type));
        return Optional.of(Iterables.getOnlyElement(filters.values()));
    }

    public static SelectiveStreamReader createNestedStreamReader(
            StreamDescriptor streamDescriptor,
            int level,
            Optional<HierarchicalFilter> parentFilter,
            Optional<Type> outputType,
            DateTimeZone hiveStorageTimeZone,
            AggregatedMemoryContext systemMemoryContext)
    {
        switch (streamDescriptor.getStreamType()) {
            case BOOLEAN:
            case BYTE:
            case SHORT:
            case INT:
            case LONG:
            case DATE:
            case FLOAT:
            case DOUBLE:
            case BINARY:
            case STRING:
            case VARCHAR:
            case CHAR:
            case TIMESTAMP:
            case DECIMAL:
                Map<Subfield, TupleDomainFilter> elementFilters = ImmutableMap.of();
                if (parentFilter.isPresent()) {
                    TupleDomainFilter.PositionalFilter positionalFilter = parentFilter.get().getPositionalFilter();
                    if (positionalFilter != null) {
                        elementFilters = ImmutableMap.of(new Subfield("c"), positionalFilter);
                    }
                }
                if (!outputType.isPresent() && elementFilters.isEmpty()) {
                    // No need to read the elements when output is not required and the filter is a simple IS [NOT] NULL
                    return null;
                }
                return createStreamReader(streamDescriptor, elementFilters, outputType, ImmutableList.of(), hiveStorageTimeZone, systemMemoryContext.newAggregatedMemoryContext());
            case LIST:
                Optional<ListFilter> childFilter = parentFilter.map(HierarchicalFilter::getChild).map(ListFilter.class::cast);
                return new ListSelectiveStreamReader(streamDescriptor, ImmutableMap.of(), ImmutableList.of(), childFilter.orElse(null), level, outputType, hiveStorageTimeZone, systemMemoryContext.newAggregatedMemoryContext());
            case STRUCT:
                return new StructSelectiveStreamReader(streamDescriptor, ImmutableMap.of(), ImmutableList.of(), outputType, hiveStorageTimeZone, systemMemoryContext.newAggregatedMemoryContext());
            case MAP:
            case UNION:
            default:
                throw new IllegalArgumentException("Unsupported type: " + streamDescriptor.getStreamType());
        }
    }
}
