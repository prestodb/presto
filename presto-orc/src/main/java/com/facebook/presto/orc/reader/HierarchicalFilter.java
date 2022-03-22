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

import java.util.Map;

public interface HierarchicalFilter
{
    HierarchicalFilter getChild();

    // Positional IS [NOT] NULL filters to apply at the next level
    NullsFilter getNullsFilter();

    // Positional range filters to apply at the very bottom level
    PositionalFilter getPositionalFilter();

    // Top-level offsets
    int[] getTopLevelOffsets();

    // Number of valid entries in the top-level offsets array
    int getTopLevelOffsetCount();

    // Filters per-position; positions with no filters are populated with nulls
    long[] getElementFilters();

    // Flags indicating top-level positions with at least one subfield with failed filter
    boolean[] getTopLevelFailed();

    // Flags indicating top-level positions missing elements to apply subfield filters to
    boolean[] getTopLevelIndexOutOfBounds();

    long getRetainedSizeInBytes();

    static HierarchicalFilter createHierarchicalFilter(StreamDescriptor streamDescriptor, Map<Subfield, TupleDomainFilter> subfieldFilters, int level, HierarchicalFilter parent)
    {
        switch (streamDescriptor.getOrcTypeKind()) {
            case LIST:
                return new ListFilter(streamDescriptor, subfieldFilters, level, parent);
            case MAP:
            case STRUCT:
                throw new UnsupportedOperationException();
            default:
                return null;
        }
    }
}
