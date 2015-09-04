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

import com.google.common.collect.ImmutableList;

import java.util.List;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class Footer
{
    private final long numberOfRows;
    private final int rowsInRowGroup;
    private final List<StripeInformation> stripes;
    private final List<OrcType> types;
    private final List<ColumnStatistics> fileStats;

    public Footer(long numberOfRows, int rowsInRowGroup, List<StripeInformation> stripes, List<OrcType> types, List<ColumnStatistics> fileStats)
    {
        this.numberOfRows = numberOfRows;
        this.rowsInRowGroup = rowsInRowGroup;
        this.stripes = ImmutableList.copyOf(requireNonNull(stripes, "stripes is null"));
        this.types = ImmutableList.copyOf(requireNonNull(types, "types is null"));
        this.fileStats = ImmutableList.copyOf(requireNonNull(fileStats, "columnStatistics is null"));
    }

    public long getNumberOfRows()
    {
        return numberOfRows;
    }

    public int getRowsInRowGroup()
    {
        return rowsInRowGroup;
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

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("numberOfRows", numberOfRows)
                .add("rowsInRowGroup", rowsInRowGroup)
                .add("stripes", stripes)
                .add("types", types)
                .add("columnStatistics", fileStats)
                .toString();
    }
}
