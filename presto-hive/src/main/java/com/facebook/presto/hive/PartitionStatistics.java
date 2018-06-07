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

package com.facebook.presto.hive;

import com.facebook.presto.hive.metastore.HiveColumnStatistics;
import com.google.common.collect.ImmutableMap;

import java.util.Map;
import java.util.Objects;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class PartitionStatistics
{
    private static final PartitionStatistics EMPTY = new PartitionStatistics(HiveBasicStatistics.createEmptyStatistics(), ImmutableMap.of());

    private final HiveBasicStatistics basicStatistics;
    private final Map<String, HiveColumnStatistics> columnStatistics;

    public static PartitionStatistics empty()
    {
        return EMPTY;
    }

    public PartitionStatistics(
            HiveBasicStatistics basicStatistics,
            Map<String, HiveColumnStatistics> columnStatistics)
    {
        this.basicStatistics = requireNonNull(basicStatistics, "basicStatistics is null");
        this.columnStatistics = ImmutableMap.copyOf(requireNonNull(columnStatistics, "columnStatistics can not be null"));
    }

    public HiveBasicStatistics getBasicStatistics()
    {
        return basicStatistics;
    }

    public Map<String, HiveColumnStatistics> getColumnStatistics()
    {
        return columnStatistics;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        PartitionStatistics that = (PartitionStatistics) o;
        return Objects.equals(basicStatistics, that.basicStatistics) &&
                Objects.equals(columnStatistics, that.columnStatistics);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(basicStatistics, columnStatistics);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("basicStatistics", basicStatistics)
                .add("columnStatistics", columnStatistics)
                .toString();
    }
}
