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

import static java.util.Objects.requireNonNull;

public class PartitionStatistics
{
    private final HiveBasicStatistics basicStatistics;
    private final Map<String, HiveColumnStatistics> columnStatistics;

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
}
