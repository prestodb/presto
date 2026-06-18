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
package com.facebook.presto.orc.metadata;

import com.facebook.presto.orc.metadata.statistics.ColumnStatistics;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.collect.Maps.transformValues;
import static java.util.Objects.requireNonNull;

public class Footer
{
    private final long numberOfRows;
    private final int rowsInRowGroup;
    private final OptionalLong rawSize;
    private final List<StripeInformation> stripes;
    private final List<OrcType> types;
    private final List<ColumnStatistics> fileStats;
    private final Map<String, Slice> userMetadata;
    private final Optional<DwrfEncryption> encryption;
    private final Optional<List<Integer>> dwrfStripeCacheOffsets;

    public Footer(
            long numberOfRows,
            int rowsInRowGroup,
            OptionalLong rawSize,
            List<StripeInformation> stripes,
            List<OrcType> types,
            List<ColumnStatistics> fileStats,
            Map<String, Slice> userMetadata,
            Optional<DwrfEncryption> encryption,
            Optional<List<Integer>> dwrfStripeCacheOffsets)
    {
        this.numberOfRows = numberOfRows;
        this.rowsInRowGroup = rowsInRowGroup;
        this.rawSize = requireNonNull(rawSize, "rawSize is null");
        this.stripes = ImmutableList.copyOf(requireNonNull(stripes, "stripes is null"));
        this.types = ImmutableList.copyOf(requireNonNull(types, "types is null"));
        this.fileStats = ImmutableList.copyOf(requireNonNull(fileStats, "columnStatistics is null"));
        requireNonNull(userMetadata, "userMetadata is null");
        this.userMetadata = ImmutableMap.copyOf(transformValues(userMetadata, Slices::copyOf));
        this.encryption = requireNonNull(encryption, "encryption is null");
        this.dwrfStripeCacheOffsets = requireNonNull(dwrfStripeCacheOffsets, "dwrfStripeCacheOffsets is null").map(ImmutableList::copyOf);
    }

    public long getNumberOfRows()
    {
        return numberOfRows;
    }

    public int getRowsInRowGroup()
    {
        return rowsInRowGroup;
    }

    public OptionalLong getRawSize()
    {
        return rawSize;
    }

    public List<StripeInformation> getStripes()
    {
        return stripes;
    }

    public List<OrcType> getTypes()
    {
        return types;
    }

    public List<ColumnStatistics> getFileStats()
    {
        return fileStats;
    }

    public Map<String, Slice> getUserMetadata()
    {
        return ImmutableMap.copyOf(transformValues(userMetadata, Slices::copyOf));
    }

    public Optional<DwrfEncryption> getEncryption()
    {
        return encryption;
    }

    public Optional<List<Integer>> getDwrfStripeCacheOffsets()
    {
        return dwrfStripeCacheOffsets;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("numberOfRows", numberOfRows)
                .add("rowsInRowGroup", rowsInRowGroup)
                .add("stripes", stripes)
                .add("types", types)
                .add("columnStatistics", fileStats)
                .add("userMetadata", userMetadata.keySet())
                .add("dwrfStripeCacheOffsets", dwrfStripeCacheOffsets)
                .toString();
    }
}
