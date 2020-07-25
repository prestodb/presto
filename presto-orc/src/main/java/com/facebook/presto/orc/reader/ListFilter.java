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
import com.facebook.presto.orc.StreamDescriptor;
import com.facebook.presto.orc.TupleDomainFilter;
import com.facebook.presto.orc.TupleDomainFilter.NullsFilter;
import com.facebook.presto.orc.TupleDomainFilter.PositionalFilter;
import org.openjdk.jol.info.ClassLayout;

import javax.annotation.Nullable;

import java.util.Arrays;
import java.util.Map;
import java.util.function.Predicate;

import static com.facebook.presto.orc.TupleDomainFilter.IS_NOT_NULL;
import static com.facebook.presto.orc.TupleDomainFilter.IS_NULL;
import static com.facebook.presto.orc.reader.HierarchicalFilter.createHierarchicalFilter;
import static com.google.common.base.Preconditions.checkArgument;
import static io.airlift.slice.SizeOf.sizeOf;
import static java.lang.Long.numberOfTrailingZeros;
import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;

public class ListFilter
        implements HierarchicalFilter
{
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(ListFilter.class).instanceSize();

    @Nullable
    private final HierarchicalFilter parent;
    @Nullable
    private final HierarchicalFilter child;

    // Array of filters in a fixed order
    private final TupleDomainFilter[] tupleDomainFilters;

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
    private final long[] subscriptFilters;

    // filters per position; positions with no filters are populated with nulls
    private long[] elementFilters;

    // offsets into elementFilters array identifying ranges corresponding to individual top-level values
    private int[] topLevelOffsets;

    // number of valid entries in topLevelOffsets array, e.g. number of top-level positions
    private int topLevelOffsetCount;

    // IS [NOT] NULL filters that apply to this level
    private final long nullFilters;

    // IS [NOT] NULL positional filters
    private final NullsFilter nullsFilter;

    // positions with nulls allowed, e.g. positions with no filter or with IS NULL filter; used to setup nullsFilter
    private boolean[] nullsAllowed;

    // positions with non-nulls allowed, e.g. positions with no filter or with IS NOT NULL filter; used to setup nullsFilter
    private boolean[] nonNullsAllowed;

    // top-level positions with "Array index out of bounds" errors
    private boolean[] indexOutOfBounds;

    public ListFilter(StreamDescriptor streamDescriptor, Map<Subfield, TupleDomainFilter> subfieldFilters)
    {
        this(streamDescriptor, subfieldFilters, 0, null);
    }

    public ListFilter(StreamDescriptor streamDescriptor, Map<Subfield, TupleDomainFilter> subfieldFilters, int level, HierarchicalFilter parent)
    {
        requireNonNull(streamDescriptor, "streamDescriptor is null");
        requireNonNull(subfieldFilters, "subfieldFilters is null");
        checkArgument(!subfieldFilters.isEmpty(), "subfieldFilters is empty");
        checkArgument(subfieldFilters.size() <= 64, "Number of filters cannot exceed 64");
        checkArgument((level == 0 && parent == null) || (level > 0 && parent != null), "parent must be null for top-level filter and non-null otherwise");

        this.parent = parent;
        tupleDomainFilters = subfieldFilters.values().toArray(new TupleDomainFilter[0]);
        subscriptFilters = extractFilters(subfieldFilters, level, subfield -> subfield.getPath().size() > level);
        nullFilters = combineFilters(extractFilters(subfieldFilters, level, subfield -> subfield.getPath().size() == level + 1));
        if (nullFilters > 0) {
            nullsFilter = new NullsFilter();
        }
        else {
            nullsFilter = null;
        }

        StreamDescriptor elementStreamDescriptor = streamDescriptor.getNestedStreams().get(0);
        child = createHierarchicalFilter(elementStreamDescriptor, subfieldFilters, level + 1, this);
        if (child == null) {
            positionalFilter = new PositionalFilter();
        }
        else {
            positionalFilter = null;
        }
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

    public void populateElementFilters(int positionCount, boolean[] nulls, int[] lengths, int lengthSum)
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

        int elementPosition = 0;
        if (parent == null) {   // top-level filter
            for (int i = 0; i < positionCount; i++) {
                topLevelOffsets[i] = elementPosition;

                for (int j = 0; j < lengths[i]; j++) {
                    if (j < subscriptFilters.length) {
                        elementFilters[elementPosition] = subscriptFilters[j];
                    }
                    else {
                        elementFilters[elementPosition] = 0;
                    }
                    elementPosition++;
                }

                if (nulls == null || !nulls[i]) {
                    for (int j = lengths[i]; j < subscriptFilters.length; j++) {
                        if (subscriptFilters[j] > 0) {
                            // this entry doesn't have enough elements for all filters,
                            // hence, raise "Array index out of bound" error
                            indexOutOfBounds[i] = true;
                            break;
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

                long parentElementFilter = parentElementFilters[i];
                for (int j = 0; j < lengths[i]; j++) {
                    if (subscriptFilters != null && j < subscriptFilters.length) {
                        elementFilters[elementPosition] = parentElementFilter & subscriptFilters[j];
                    }
                    else {
                        elementFilters[elementPosition] = 0;
                    }
                    elementPosition++;
                }

                if (subscriptFilters != null && (nulls == null || !nulls[i])) {
                    for (int j = lengths[i]; j < subscriptFilters.length; j++) {
                        if ((parentElementFilter & subscriptFilters[j]) > 0) {
                            // this entry doesn't have enough elements for all filters,
                            // hence, raise "Array index out of bound" error
                            indexOutOfBounds[offsetIndex] = true;
                            break;
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
        }
        nullsFilter.setup(nullsAllowed, nonNullsAllowed, topLevelOffsets);
    }

    @Override
    public boolean[] getTopLevelFailed()
    {
        if (child == null) {
            return positionalFilter.getFailed();
        }

        boolean[] failed = child.getTopLevelFailed();
        if (nullsFilter != null) {
            for (int i = 0; i < failed.length; i++) {
                failed[i] |= nullsFilter.getFailed()[i];
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
        return INSTANCE_SIZE + sizeOf(elementFilters) + sizeOf(tupleDomainFilters) + sizeOf(subscriptFilters) +
                sizeOf(elementTupleDomainFilters) + sizeOf(topLevelOffsets) +
                sizeOf(nonNullsAllowed) + sizeOf(nullsAllowed) + sizeOf(indexOutOfBounds) +
                (child != null ? child.getRetainedSizeInBytes() : 0);
    }

    private static long[] extractFilters(Map<Subfield, TupleDomainFilter> filters, int level, Predicate<Subfield> predicate)
    {
        int maxSubscript = -1;
        int[] subscripts = new int[filters.size()];
        int index = 0;
        for (Subfield subfield : filters.keySet()) {
            if (predicate.test(subfield)) {
                subscripts[index] = toSubscript(subfield, level);
                maxSubscript = Math.max(maxSubscript, subscripts[index]);
            }
            else {
                subscripts[index] = -1;
            }
            index++;
        }

        if (maxSubscript == -1) {
            return null;
        }

        long[] filterCodes = new long[maxSubscript + 1];
        for (int i = 0; i < subscripts.length; i++) {
            if (subscripts[i] != -1) {
                filterCodes[subscripts[i]] |= 1 << i;
            }
        }

        return filterCodes;
    }

    private static int toSubscript(Subfield subfield, int level)
    {
        checkArgument(subfield.getPath().size() > level);
        checkArgument(subfield.getPath().get(level) instanceof Subfield.LongSubscript);

        return toIntExact(((Subfield.LongSubscript) subfield.getPath().get(level)).getIndex()) - 1;
    }

    private static long combineFilters(long[] filters)
    {
        if (filters == null) {
            return 0;
        }

        int combined = 0;
        for (long filter : filters) {
            combined |= filter;
        }
        return combined;
    }

    private static int[] ensureCapacity(int[] buffer, int capacity)
    {
        if (buffer == null || buffer.length < capacity) {
            return new int[capacity];
        }

        return buffer;
    }

    private static long[] ensureCapacity(long[] buffer, int capacity)
    {
        if (buffer == null || buffer.length < capacity) {
            return new long[capacity];
        }

        return buffer;
    }

    private static boolean[] ensureCapacity(boolean[] buffer, int capacity)
    {
        if (buffer == null || buffer.length < capacity) {
            return new boolean[capacity];
        }

        return buffer;
    }
}
