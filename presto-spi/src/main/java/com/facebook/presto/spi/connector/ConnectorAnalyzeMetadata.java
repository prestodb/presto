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
package com.facebook.presto.spi.connector;

import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.statistics.TableStatisticsMetadata;

import static java.util.Objects.requireNonNull;

public class ConnectorAnalyzeMetadata
{
    private final TableStatisticsMetadata statisticsMetadata;
    private final ConnectorTableHandle tableHandle;

    public ConnectorAnalyzeMetadata(TableStatisticsMetadata statisticsMetadata, ConnectorTableHandle tableHandle)
    {
        this.statisticsMetadata = requireNonNull(statisticsMetadata, "statisticsMetadata is null");
        this.tableHandle = requireNonNull(tableHandle, "tableHandle is null");
    }

    public TableStatisticsMetadata getStatisticsMetadata()
    {
        return statisticsMetadata;
    }

    public ConnectorTableHandle getTableHandle()
    {
        return tableHandle;
    }
}
