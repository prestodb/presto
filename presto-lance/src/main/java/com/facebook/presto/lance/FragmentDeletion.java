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
package com.facebook.presto.lance;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;

import java.util.List;

import static java.util.Objects.requireNonNull;

/**
 * The set of row indexes to delete from a single Lance fragment, produced by a
 * row-level DELETE/UPDATE on a worker and applied as a deletion vector on the
 * coordinator during finish.
 */
public class FragmentDeletion
{
    private final int fragmentId;
    private final List<Integer> rowIndexes;

    @JsonCreator
    public FragmentDeletion(
            @JsonProperty("fragmentId") int fragmentId,
            @JsonProperty("rowIndexes") List<Integer> rowIndexes)
    {
        this.fragmentId = fragmentId;
        this.rowIndexes = ImmutableList.copyOf(requireNonNull(rowIndexes, "rowIndexes is null"));
    }

    @JsonProperty
    public int getFragmentId()
    {
        return fragmentId;
    }

    @JsonProperty
    public List<Integer> getRowIndexes()
    {
        return rowIndexes;
    }
}
