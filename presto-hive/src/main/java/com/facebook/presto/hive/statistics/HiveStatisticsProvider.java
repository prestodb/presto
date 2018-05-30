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

package com.facebook.presto.hive.statistics;

import com.facebook.presto.hive.HivePartition;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.statistics.TableStatistics;

import java.util.List;
import java.util.Map;

/**
 * @param tableColumns must be Hive columns, not hidden (Presto-internal) columns
 */
public interface HiveStatisticsProvider
{
    TableStatistics getTableStatistics(
            ConnectorSession session,
            ConnectorTableHandle tableHandle,
            List<HivePartition> hivePartitions,
            Map<String, ColumnHandle> tableColumns);
}
