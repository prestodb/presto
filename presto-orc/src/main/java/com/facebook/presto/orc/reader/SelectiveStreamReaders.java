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

import com.facebook.presto.common.Subfield;
import com.facebook.presto.common.type.ArrayType;
import com.facebook.presto.common.type.BigintType;
import com.facebook.presto.common.type.BooleanType;
import com.facebook.presto.common.type.CharType;
import com.facebook.presto.common.type.DateType;
import com.facebook.presto.common.type.DecimalType;
import com.facebook.presto.common.type.DoubleType;
import com.facebook.presto.common.type.IntegerType;
import com.facebook.presto.common.type.MapType;
import com.facebook.presto.common.type.RealType;
import com.facebook.presto.common.type.RowType;
import com.facebook.presto.common.type.SmallintType;
import com.facebook.presto.common.type.TimestampType;
import com.facebook.presto.common.type.TinyintType;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.VarbinaryType;
import com.facebook.presto.common.type.VarcharType;
import com.facebook.presto.orc.OrcAggregatedMemoryContext;
import com.facebook.presto.orc.OrcRecordReaderOptions;
import com.facebook.presto.orc.StreamDescriptor;
import com.facebook.presto.orc.TupleDomainFilter;
import com.facebook.presto.orc.metadata.OrcType.OrcTypeKind;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import org.joda.time.DateTimeZone;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Predicate;

import static com.facebook.presto.common.array.Arrays.ensureCapacity;
import static com.facebook.presto.common.type.Decimals.MAX_SHORT_PRECISION;
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
            OrcRecordReaderOptions options,
            boolean legacyMapSubscript,
            OrcAggregatedMemoryContext systemMemoryContext)
    {
        OrcTypeKind type = streamDescriptor.getOrcTypeKind();
        switch (type) {
            case BOOLEAN: {
                checkArgument(requiredSubfields.isEmpty(), "Boolean stream reader doesn't support subfields");
                verifyStreamType(streamDescriptor, outputType, BooleanType.class::isInstance);
                return new BooleanSelectiveStreamReader(streamDescriptor, getOptionalOnlyFilter(type, filters), outputType.isPresent(), systemMemoryContext.newOrcLocalMemoryContext(SelectiveStreamReaders.class.getSimpleName()));
            }
            case BYTE: {
                checkArgument(requiredSubfields.isEmpty(), "Byte stream reader doesn't support subfields");
                verifyStreamType(streamDescriptor, outputType, TinyintType.class::isInstance);
                return new ByteSelectiveStreamReader(streamDescriptor, getOptionalOnlyFilter(type, filters), outputType.isPresent(), systemMemoryContext.newOrcLocalMemoryContext(SelectiveStreamReaders.class.getSimpleName()));
            }
            case SHORT:
            case INT:
            case LONG:
            case DATE: {
                checkArgument(requiredSubfields.isEmpty(), "Primitive type stream reader doesn't support subfields");
                verifyStreamType(streamDescriptor, outputType, t -> t instanceof BigintType || t instanceof IntegerType || t instanceof SmallintType || t instanceof DateType);
                return new LongSelectiveStreamReader(streamDescriptor, getOptionalOnlyFilter(type, filters), outputType, systemMemoryContext);
            }
            case FLOAT: {
                checkArgument(requiredSubfields.isEmpty(), "Float type stream reader doesn't support subfields");
                verifyStreamType(streamDescriptor, outputType, RealType.class::isInstance);
                return new FloatSelectiveStreamReader(streamDescriptor, getOptionalOnlyFilter(type, filters), outputType.isPresent(), systemMemoryContext.newOrcLocalMemoryContext(SelectiveStreamReaders.class.getSimpleName()));
            }
            case DOUBLE:
                checkArgument(requiredSubfields.isEmpty(), "Double stream reader doesn't support subfields");
                verifyStreamType(streamDescriptor, outputType, DoubleType.class::isInstance);
                return new DoubleSelectiveStreamReader(streamDescriptor, getOptionalOnlyFilter(type, filters), outputType.isPresent(), systemMemoryContext.newOrcLocalMemoryContext(SelectiveStreamReaders.class.getSimpleName()));
            case BINARY:
            case STRING:
            case VARCHAR:
            case CHAR:
                checkArgument(requiredSubfields.isEmpty(), "Primitive stream reader doesn't support subfields");
                verifyStreamType(streamDescriptor, outputType, t -> t instanceof VarcharType || t instanceof CharType || t instanceof VarbinaryType);
                return new SliceSelectiveStreamReader(streamDescriptor, getOptionalOnlyFilter(type, filters), outputType, systemMemoryContext);
            case TIMESTAMP: {
                checkArgument(requiredSubfields.isEmpty(), "Timestamp stream reader doesn't support subfields");
                verifyStreamType(streamDescriptor, outputType, TimestampType.class::isInstance);
                return new TimestampSelectiveStreamReader(
                        streamDescriptor,
                        getOptionalOnlyFilter(type, filters),
                        hiveStorageTimeZone,
                        outputType.isPresent(),
                        systemMemoryContext.newOrcLocalMemoryContext(SelectiveStreamReaders.class.getSimpleName()),
                        options);
            }
            case LIST:
                verifyStreamType(streamDescriptor, outputType, ArrayType.class::isInstance);
                return new ListSelectiveStreamReader(streamDescriptor, filters, requiredSubfields, null, 0, outputType, hiveStorageTimeZone, options, legacyMapSubscript, systemMemoryContext);
            case STRUCT:
                verifyStreamType(streamDescriptor, outputType, RowType.class::isInstance);
                return new StructSelectiveStreamReader(streamDescriptor, filters, requiredSubfields, outputType, hiveStorageTimeZone, options, legacyMapSubscript, systemMemoryContext);
            case MAP:
                verifyStreamType(streamDescriptor, outputType, MapType.class::isInstance);
                return new MapSelectiveStreamReader(streamDescriptor, filters, requiredSubfields, outputType, hiveStorageTimeZone, options, legacyMapSubscript, systemMemoryContext);
            case DECIMAL: {
                verifyStreamType(streamDescriptor, outputType, DecimalType.class::isInstance);
                if (streamDescriptor.getOrcType().getPrecision().get() <= MAX_SHORT_PRECISION) {
                    return new ShortDecimalSelectiveStreamReader(streamDescriptor, getOptionalOnlyFilter(type, filters), outputType, systemMemoryContext.newOrcLocalMemoryContext(SelectiveStreamReaders.class.getSimpleName()));
                }
                else {
                    return new LongDecimalSelectiveStreamReader(streamDescriptor, getOptionalOnlyFilter(type, filters), outputType, systemMemoryContext.newOrcLocalMemoryContext(SelectiveStreamReaders.class.getSimpleName()));
                }
            }
            case UNION:
            default:
                throw new IllegalArgumentException("Unsupported type: " + type);
        }
    }

    private static void verifyStreamType(StreamDescriptor streamDescriptor, Optional<Type> outputType, Predicate<Type> predicate)
    {
        if (outputType.isPresent()) {
            ReaderUtils.verifyStreamType(streamDescriptor, outputType.get(), predicate);
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
            List<Subfield> requiredSubfields,
            DateTimeZone hiveStorageTimeZone,
            OrcRecordReaderOptions options,
            boolean legacyMapSubscript,
            OrcAggregatedMemoryContext systemMemoryContext)
    {
        switch (streamDescriptor.getOrcTypeKind()) {
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
                return createStreamReader(streamDescriptor, elementFilters, outputType, requiredSubfields, hiveStorageTimeZone, options, legacyMapSubscript, systemMemoryContext.newOrcAggregatedMemoryContext());
            case LIST:
                Optional<ListFilter> childFilter = parentFilter.map(HierarchicalFilter::getChild).map(ListFilter.class::cast);
                return new ListSelectiveStreamReader(streamDescriptor, ImmutableMap.of(), requiredSubfields, childFilter.orElse(null), level, outputType, hiveStorageTimeZone, options, legacyMapSubscript, systemMemoryContext.newOrcAggregatedMemoryContext());
            case STRUCT:
                checkArgument(!parentFilter.isPresent(), "Filters on nested structs are not supported yet");
                return new StructSelectiveStreamReader(streamDescriptor, ImmutableMap.of(), requiredSubfields, outputType, hiveStorageTimeZone, options, legacyMapSubscript, systemMemoryContext.newOrcAggregatedMemoryContext());
            case MAP:
                checkArgument(!parentFilter.isPresent(), "Filters on nested maps are not supported yet");
                return new MapSelectiveStreamReader(streamDescriptor, ImmutableMap.of(), requiredSubfields, outputType, hiveStorageTimeZone, options, legacyMapSubscript, systemMemoryContext.newOrcAggregatedMemoryContext());
            case UNION:
            default:
                throw new IllegalArgumentException("Unsupported type: " + streamDescriptor.getOrcTypeKind());
        }
    }

    public static int[] initializeOutputPositions(int[] outputPositions, int[] positions, int positionCount)
    {
        outputPositions = ensureCapacity(outputPositions, positionCount);
        System.arraycopy(positions, 0, outputPositions, 0, positionCount);
        return outputPositions;
    }
}
