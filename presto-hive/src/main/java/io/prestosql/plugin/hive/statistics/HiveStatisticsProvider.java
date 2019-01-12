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

package io.prestosql.plugin.hive.statistics;

import io.prestosql.plugin.hive.HivePartition;
import io.prestosql.spi.connector.ColumnHandle;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.connector.SchemaTableName;
import io.prestosql.spi.statistics.TableStatistics;
import io.prestosql.spi.type.Type;

import java.util.List;
import java.util.Map;

public interface HiveStatisticsProvider
{
    /**
     * @param columns must be Hive columns, not hidden (Presto-internal) columns
     */
    TableStatistics getTableStatistics(
            ConnectorSession session,
            SchemaTableName table,
            Map<String, ColumnHandle> columns,
            Map<String, Type> columnTypes,
            List<HivePartition> partitions);
}
