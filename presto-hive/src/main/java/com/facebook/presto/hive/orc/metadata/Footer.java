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
package com.facebook.presto.hive.orc.metadata;

import com.google.common.base.Objects;
import com.google.common.collect.ImmutableList;

import java.util.List;

import static com.google.common.base.Preconditions.checkNotNull;

public class Footer
{
    private final long numberOfRows;
    private final long rowIndexStride;
    private final List<StripeInformation> stripes;
    private final List<Type> types;
    private final List<ColumnStatistics> fileStats;

    public Footer(long numberOfRows, long rowIndexStride, List<StripeInformation> stripes, List<Type> types, List<ColumnStatistics> fileStats)
    {
        this.numberOfRows = numberOfRows;
        this.rowIndexStride = rowIndexStride;
        this.stripes = ImmutableList.copyOf(checkNotNull(stripes, "stripes is null"));
        this.types = ImmutableList.copyOf(checkNotNull(types, "types is null"));
        this.fileStats = ImmutableList.copyOf(checkNotNull(fileStats, "columnStatistics is null"));
    }

    public long getNumberOfRows()
    {
        return numberOfRows;
    }

    public long getRowIndexStride()
    {
        return rowIndexStride;
    }

    public List<StripeInformation> getStripes()
    {
        return stripes;
    }

    public List<Type> getTypes()
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
        return Objects.toStringHelper(this)
                .add("numberOfRows", numberOfRows)
                .add("rowIndexStride", rowIndexStride)
                .add("stripes", stripes)
                .add("types", types)
                .add("columnStatistics", fileStats)
                .toString();
    }
}
