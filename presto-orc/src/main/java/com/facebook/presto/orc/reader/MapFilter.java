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

import com.facebook.presto.orc.StreamDescriptor;
import com.facebook.presto.orc.TupleDomainFilter;
import com.facebook.presto.orc.TupleDomainFilter.NullsFilter;
import com.facebook.presto.orc.TupleDomainFilter.PositionalFilter;
import com.facebook.presto.orc.metadata.OrcType;
import com.facebook.presto.spi.Subfield;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.type.Type;
import com.google.common.collect.ImmutableMap;
import org.openjdk.jol.info.ClassLayout;

import javax.annotation.Nullable;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.Predicate;

import static com.facebook.presto.array.Arrays.ensureCapacity;
import static com.facebook.presto.orc.TupleDomainFilter.ALWAYS_FALSE;
import static com.facebook.presto.orc.TupleDomainFilter.IS_NOT_NULL;
import static com.facebook.presto.orc.TupleDomainFilter.IS_NULL;
import static com.facebook.presto.orc.reader.HierarchicalFilter.createHierarchicalFilter;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.Decimals.MAX_SHORT_PRECISION;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.google.common.base.Preconditions.checkArgument;
import static io.airlift.slice.SizeOf.sizeOf;
import static java.lang.Long.numberOfTrailingZeros;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class MapFilter
        implements HierarchicalFilter
{
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(MapFilter.class).instanceSize();

    // When true, report rows with missing keys as if they didn't pass the filter (unless filters on these keys do allow nulls)
    private final boolean legacyMapSubscript;

    @Nullable
    private final HierarchicalFilter parent;
    @Nullable
    private final HierarchicalFilter child;

    // Array of filters in a fixed order
    private final TupleDomainFilter[] tupleDomainFilters;
    private final int alwaysFalseFilterCode;

    // Set only at the deepest level
    @Nullable
    private final PositionalFilter positionalFilter;

    // filter per position; populated only at the deepest level
    // used to setup positionalFilter
    private TupleDomainFilter[] elementTupleDomainFilters;

    // filters per subscript: subscriptFilters[subscript - 1] = {"> 3", "= 10"}
    // individual subscripts may have multiple filters
    // for example, consider this filter: a[1][2] > 3 and a[1][4] = 10 and a[3][2] < 100
    //  - at level 0, subscript 1 has 2 filters (> 3, = 10) and subscript 3 has one filter (< 100)
    //  - at level 2, subscript 2 has 2 filters (> 3, < 100), subscript 4 has one filter (= 10)
    // each value represents multiple filters - the long value has bits corresponding to filter
    // indices in tupleDomainFilters array set
    private final Map<Object, Long> subscriptFilters;

    private final long requiredFilters;

    private final ElementFilterLookup elementFilterLookup;

    // filters per position; positions with no filters are populated with nulls
    private long[] elementFilters;

    // offsets into elementFilters array identifying ranges corresponding to individual top-level values
    private int[] topLevelOffsets;

    // number of valid entries in topLevelOffsets array, e.g. number of top-level positions
    private int topLevelOffsetCount;

    // IS [NOT] NULL filters that apply to this level
    private final long nullFilters;

    // Filters that do not allow nulls and apply at deeper levels
    private final long notNullDownstreamFilters;

    // IS [NOT] NULL positional filters
    private final NullsFilter nullsFilter;

    // positions with nulls allowed, e.g. positions with no filter or with IS NULL filter; used to setup nullsFilter
    private boolean[] nullsAllowed;

    // positions with non-nulls allowed, e.g. positions with no filter or with IS NOT NULL filter; used to setup nullsFilter
    private boolean[] nonNullsAllowed;

    // top-level positions with "Map key not found" errors
    private boolean[] indexOutOfBounds;

    // used only with legacyMapSubscript == true;
    // top-level positions to fail because map is missing keys with filters that don't allow nulls
    private boolean[] failedOnMissingKey;

    public MapFilter(StreamDescriptor streamDescriptor, Map<Subfield, TupleDomainFilter> subfieldFilters, boolean legacyMapSubscript)
    {
        this(streamDescriptor, subfieldFilters, 0, null, legacyMapSubscript);
    }

    public MapFilter(StreamDescriptor streamDescriptor, Map<Subfield, TupleDomainFilter> subfieldFilters, int level, HierarchicalFilter parent, boolean legacyMapSubscript)
    {
        requireNonNull(streamDescriptor, "streamDescriptor is null");
        requireNonNull(subfieldFilters, "subfieldFilters is null");
        checkArgument(!subfieldFilters.isEmpty(), "subfieldFilters is empty");
        checkArgument(subfieldFilters.size() <= 63, "Number of filters cannot exceed 63");
        checkArgument((level == 0 && parent == null) || (level > 0 && parent != null), "parent must be null for top-level filter and non-null otherwise");

        this.legacyMapSubscript = legacyMapSubscript;
        this.parent = parent;
        tupleDomainFilters = toTupleDomainFilterArray(subfieldFilters);
        alwaysFalseFilterCode = 1 << subfieldFilters.size();

        OrcType keyType = streamDescriptor.getNestedStreams().get(0).getOrcType();
        switch (keyType.getOrcTypeKind()) {
            case SHORT:
            case INT:
            case LONG:
                subscriptFilters = extractFilters(subfieldFilters, level, subfield -> subfield.getPath().size() > level, filter -> true, MapFilter::toLongSubscript);
                break;
            case DECIMAL:
                if (keyType.getPrecision().get() <= MAX_SHORT_PRECISION) {
                    subscriptFilters = extractFilters(subfieldFilters, level, subfield -> subfield.getPath().size() > level, filter -> true, MapFilter::toLongSubscript);
                }
                else {
                    throw new IllegalArgumentException(format("Unsupported map key type: decimal(%s, %s)", keyType.getPrecision(), keyType.getScale()));
                }
                break;
            case STRING:
            case VARCHAR:
            case CHAR:
                subscriptFilters = extractFilters(subfieldFilters, level, subfield -> subfield.getPath().size() > level, filter -> true, MapFilter::toStringSubscript);
                break;
            default:
                throw new IllegalArgumentException("Unsupported map key type: " + keyType.getOrcTypeKind());
        }

        elementFilterLookup = getElementFilterLookup(keyType);

        requiredFilters = extractAndCombineFilters(subfieldFilters, subfield -> subfield.getPath().size() > level, legacyMapSubscript ? filter -> !filter.testNull() : filter -> true);

        nullFilters = extractAndCombineFilters(subfieldFilters, subfield -> subfield.getPath().size() == level + 1, filter -> true);
        notNullDownstreamFilters = extractAndCombineFilters(subfieldFilters, subfield -> subfield.getPath().size() > level + 1, filter -> !filter.testNull());

        if (nullFilters > 0 || notNullDownstreamFilters > 0) {
            nullsFilter = new NullsFilter();
        }
        else {
            nullsFilter = null;
        }

        StreamDescriptor elementStreamDescriptor = streamDescriptor.getNestedStreams().get(1);
        child = createHierarchicalFilter(elementStreamDescriptor, subfieldFilters, level + 1, this, legacyMapSubscript);
        if (child == null) {
            positionalFilter = new PositionalFilter();
        }
        else {
            positionalFilter = null;
        }
    }

    public static Type getKeyOutputType(StreamDescriptor streamDescriptor)
    {
        OrcType keyType = streamDescriptor.getNestedStreams().get(0).getOrcType();
        switch (keyType.getOrcTypeKind()) {
            case SHORT:
            case INT:
            case LONG:
                return BIGINT;
            case DECIMAL:
                if (keyType.getPrecision().get() <= MAX_SHORT_PRECISION) {
                    return BIGINT;
                }

                throw new IllegalArgumentException(format("Unsupported map key type: decimal(%s, %s)", keyType.getPrecision(), keyType.getScale()));
            case STRING:
            case VARCHAR:
            case CHAR:
                return VARCHAR;
            default:
                throw new IllegalArgumentException("Unsupported map key type: " + keyType.getOrcTypeKind());
        }
    }

    private static TupleDomainFilter[] toTupleDomainFilterArray(Map<Subfield, TupleDomainFilter> subfieldFilters)
    {
        TupleDomainFilter[] filters = new TupleDomainFilter[subfieldFilters.size() + 1];
        int index = 0;
        for (TupleDomainFilter filter : subfieldFilters.values()) {
            filters[index++] = filter;
        }
        filters[subfieldFilters.size()] = ALWAYS_FALSE;
        return filters;
    }

    public HierarchicalFilter getParent()
    {
        return parent;
    }

    @Override
    public HierarchicalFilter getChild()
    {
        return child;
    }

    @Override
    public NullsFilter getNullsFilter()
    {
        return nullsFilter;
    }

    @Override
    public int[] getTopLevelOffsets()
    {
        return topLevelOffsets;
    }

    @Override
    public int getTopLevelOffsetCount()
    {
        return topLevelOffsetCount;
    }

    @Override
    public long[] getElementFilters()
    {
        return elementFilters;
    }

    @Override
    public PositionalFilter getPositionalFilter()
    {
        return positionalFilter;
    }

    public void populateElementFilters(int positionCount, boolean[] nulls, int[] lengths, int lengthSum, Block keys)
    {
        elementFilters = ensureCapacity(elementFilters, lengthSum);

        if (parent == null) {
            topLevelOffsetCount = positionCount + 1;
        }
        else {
            topLevelOffsetCount = parent.getTopLevelOffsetCount();
        }
        topLevelOffsets = ensureCapacity(topLevelOffsets, topLevelOffsetCount);
        indexOutOfBounds = ensureCapacity(indexOutOfBounds, topLevelOffsetCount);
        Arrays.fill(indexOutOfBounds, 0, topLevelOffsetCount, false);

        if (legacyMapSubscript) {
            failedOnMissingKey = ensureCapacity(failedOnMissingKey, topLevelOffsetCount);
            Arrays.fill(failedOnMissingKey, 0, topLevelOffsetCount, false);
        }

        int elementPosition = 0;
        if (parent == null) {   // top-level filter
            for (int i = 0; i < positionCount; i++) {
                topLevelOffsets[i] = elementPosition;

                int length = lengths[i];
                long foundFilters = 0;
                for (int j = 0; j < length; j++) {
                    long filter = elementFilterLookup.getFilter(keys, elementPosition);
                    foundFilters |= filter;
                    elementFilters[elementPosition] = filter;
                    elementPosition++;
                }

                if (nulls == null || !nulls[i]) {
                    if ((foundFilters & requiredFilters) != requiredFilters) {
                        // this entry doesn't have enough elements for all filters
                        if (legacyMapSubscript) {
                            failedOnMissingKey[i] = true;
                            if (length > 0) {
                                elementFilters[elementPosition - length] = alwaysFalseFilterCode;
                            }
                        }
                        else {
                            indexOutOfBounds[i] = true;
                        }
                    }
                }
            }
        }
        else {
            int[] parentTopLevelOffsets = updateParentTopLevelOffsets();
            long[] parentElementFilters = parent.getElementFilters();

            int offsetIndex = 0;
            int nextOffset = parentTopLevelOffsets[offsetIndex + 1];
            for (int i = 0; i < positionCount; i++) {
                while (i == nextOffset) {   // there might be duplicate offsets (if some offsets failed)
                    offsetIndex++;
                    topLevelOffsets[offsetIndex] = elementPosition;
                    nextOffset = parentTopLevelOffsets[offsetIndex + 1];
                }

                int length = lengths[i];
                long parentElementFilter = parentElementFilters[i];
                long foundFilters = 0;

                for (int j = 0; j < length; j++) {
                    long filter = elementFilterLookup.getFilter(keys, elementPosition);
                    foundFilters |= filter;
                    elementFilters[elementPosition] = parentElementFilter & filter;
                    elementPosition++;
                }

                if (subscriptFilters != null && (nulls == null || !nulls[i])) {
                    if ((foundFilters & requiredFilters) != requiredFilters) {
                        // this entry doesn't have enough elements for all filters
                        if (legacyMapSubscript) {
                            failedOnMissingKey[offsetIndex] = true;
                            if (length > 0) {
                                elementFilters[elementPosition - length] = alwaysFalseFilterCode;
                            }
                        }
                        else {
                            indexOutOfBounds[offsetIndex] = true;
                        }
                    }
                }
            }
            while (offsetIndex < topLevelOffsetCount - 1) {
                offsetIndex++;
                topLevelOffsets[offsetIndex] = elementPosition;
            }
        }
        topLevelOffsets[topLevelOffsetCount - 1] = elementPosition;

        if (positionalFilter != null) {
            setupPositionalFilter();
        }
        else if (nullsFilter != null) {
            setupNullsFilter();
        }
    }

    // update parentTopLevelOffsets to take into account failed positions
    private int[] updateParentTopLevelOffsets()
    {
        int[] parentTopLevelOffsets = parent.getTopLevelOffsets();

        NullsFilter nullsFilter = parent.getNullsFilter();
        if (nullsFilter != null) {
            boolean[] failed = nullsFilter.getFailed();
            int skipped = 0;
            for (int i = 0; i < topLevelOffsetCount - 1; i++) {
                parentTopLevelOffsets[i] -= skipped;
                if (failed[i]) {
                    skipped += parentTopLevelOffsets[i + 1] - parentTopLevelOffsets[i] - skipped;
                }
            }
            parentTopLevelOffsets[topLevelOffsetCount - 1] -= skipped;
        }

        return parentTopLevelOffsets;
    }

    private void setupPositionalFilter()
    {
        int count = topLevelOffsets[topLevelOffsetCount - 1];
        if (elementTupleDomainFilters == null || elementTupleDomainFilters.length < count) {
            elementTupleDomainFilters = new TupleDomainFilter[count];
        }
        for (int i = 0; i < count; i++) {
            long filter = elementFilters[i];
            if (filter > 0) {
                elementTupleDomainFilters[i] = tupleDomainFilters[numberOfTrailingZeros(filter)];
            }
            else {
                elementTupleDomainFilters[i] = null;
            }
        }
        positionalFilter.setFilters(elementTupleDomainFilters, topLevelOffsets);
    }

    private void setupNullsFilter()
    {
        int count = topLevelOffsets[topLevelOffsetCount - 1];
        nullsAllowed = ensureCapacity(nullsAllowed, count);
        nonNullsAllowed = ensureCapacity(nonNullsAllowed, count);

        for (int i = 0; i < count; i++) {
            long filter = elementFilters[i] & nullFilters;
            if (filter > 0) {
                TupleDomainFilter tupleDomainFilter = tupleDomainFilters[numberOfTrailingZeros(filter)];
                nullsAllowed[i] = tupleDomainFilter == IS_NULL;
                nonNullsAllowed[i] = tupleDomainFilter == IS_NOT_NULL;
            }
            else {
                nullsAllowed[i] = true;
                nonNullsAllowed[i] = true;
            }

            if ((elementFilters[i] & notNullDownstreamFilters) > 0) {
                nullsAllowed[i] = false;
            }
        }
        nullsFilter.setup(nullsAllowed, nonNullsAllowed, topLevelOffsets);
    }

    @Override
    public boolean[] getTopLevelFailed()
    {
        if (child == null) {
            boolean[] failed = positionalFilter.getFailed();
            if (legacyMapSubscript) {
                for (int i = 0; i < failed.length; i++) {
                    failed[i] |= failedOnMissingKey[i];
                }
            }
            return failed;
        }

        boolean[] failed = child.getTopLevelFailed();
        if (nullsFilter != null) {
            for (int i = 0; i < failed.length; i++) {
                failed[i] |= nullsFilter.getFailed()[i];
            }
        }

        if (legacyMapSubscript) {
            boolean[] childIndexOutOfBounds = child.getTopLevelIndexOutOfBounds();
            for (int i = 0; i < failed.length; i++) {
                failed[i] |= failedOnMissingKey[i];
                failed[i] |= childIndexOutOfBounds[i];
            }
        }

        return failed;
    }

    @Override
    public boolean[] getTopLevelIndexOutOfBounds()
    {
        if (child == null) {
            return indexOutOfBounds;
        }

        boolean[] indexOutOfBounds = child.getTopLevelIndexOutOfBounds();
        for (int i = 0; i < indexOutOfBounds.length; i++) {
            indexOutOfBounds[i] |= this.indexOutOfBounds[i];
        }

        return indexOutOfBounds;
    }

    @Override
    public long getRetainedSizeInBytes()
    {
        return INSTANCE_SIZE +
                sizeOf(elementFilters) +
                sizeOf(tupleDomainFilters) +
                // TODO Add longSubscriptFilters
                // TODO Add stringSubscriptFilters
                sizeOf(elementTupleDomainFilters) +
                sizeOf(topLevelOffsets) +
                sizeOf(nonNullsAllowed) +
                sizeOf(nullsAllowed) +
                sizeOf(indexOutOfBounds) +
                (child != null ? child.getRetainedSizeInBytes() : 0);
    }

    private static <T> Map<T, Long> extractFilters(
            Map<Subfield, TupleDomainFilter> filters,
            int level,
            Predicate<Subfield> subfieldPredicate,
            Predicate<TupleDomainFilter> filterPredicate,
            BiFunction<Subfield, Integer, T> toSubscript)
    {
        Map<T, Long> filterCodes = new HashMap<>();

        int index = 0;
        for (Map.Entry<Subfield, TupleDomainFilter> entry : filters.entrySet()) {
            Subfield subfield = entry.getKey();
            TupleDomainFilter filter = entry.getValue();

            if (subfieldPredicate.test(subfield) && filterPredicate.test(filter)) {
                T subscript = toSubscript.apply(subfield, level);
                int filterCode = 1 << index;
                filterCodes.compute(subscript, (k, v) -> v == null ? filterCode : v | filterCode);
            }

            index++;
        }

        return ImmutableMap.copyOf(filterCodes);
    }

    private static long toLongSubscript(Subfield subfield, int level)
    {
        checkArgument(subfield.getPath().size() > level);
        checkArgument(subfield.getPath().get(level) instanceof Subfield.LongSubscript);

        return ((Subfield.LongSubscript) subfield.getPath().get(level)).getIndex();
    }

    private static String toStringSubscript(Subfield subfield, int level)
    {
        checkArgument(subfield.getPath().size() > level);
        checkArgument(subfield.getPath().get(level) instanceof Subfield.StringSubscript);

        return ((Subfield.StringSubscript) subfield.getPath().get(level)).getIndex();
    }

    private static long extractAndCombineFilters(Map<Subfield, TupleDomainFilter> filters, Predicate<Subfield> subfieldPredicate, Predicate<TupleDomainFilter> filterPredicate)
    {
        int combined = 0;
        int index = 0;
        for (Map.Entry<Subfield, TupleDomainFilter> entry : filters.entrySet()) {
            Subfield subfield = entry.getKey();
            TupleDomainFilter filter = entry.getValue();

            if (subfieldPredicate.test(subfield) && filterPredicate.test(filter)) {
                combined |= (1 << index);
            }

            index++;
        }
        return combined;
    }

    private long getFilterForShortSubscript(Block block, int position)
    {
        Long filters = subscriptFilters.get((long) block.getShort(position));
        return filters != null ? filters.longValue() : 0;
    }

    private long getFilterForIntSubscript(Block block, int position)
    {
        Long filters = subscriptFilters.get((long) block.getInt(position));
        return filters != null ? filters.longValue() : 0;
    }

    private long getFilterForLongSubscript(Block block, int position)
    {
        Long filters = subscriptFilters.get(block.getLong(position));
        return filters != null ? filters.longValue() : 0;
    }

    private long getFilterForStringSubscript(Block block, int position)
    {
        Long filters = subscriptFilters.get(block.getSlice(position, 0, block.getSliceLength(position)).toStringUtf8());
        return filters != null ? filters.longValue() : 0;
    }

    private ElementFilterLookup getElementFilterLookup(OrcType keyType)
    {
        switch (keyType.getOrcTypeKind()) {
            case SHORT:
                return this::getFilterForShortSubscript;
            case INT:
                return this::getFilterForIntSubscript;
            case LONG:
                return this::getFilterForLongSubscript;
            case DECIMAL:
                if (keyType.getPrecision().get() <= MAX_SHORT_PRECISION) {
                    return this::getFilterForLongSubscript;
                }

                throw new IllegalArgumentException(format("Unsupported map key type: decimal(%s, %s)", keyType.getPrecision(), keyType.getScale()));
            case STRING:
            case VARCHAR:
            case CHAR:
                return this::getFilterForStringSubscript;
            default:
                throw new IllegalArgumentException("Unsupported map key type: " + keyType.getOrcTypeKind());
        }
    }

    private interface ElementFilterLookup
    {
        long getFilter(Block block, int position);
    }
}
