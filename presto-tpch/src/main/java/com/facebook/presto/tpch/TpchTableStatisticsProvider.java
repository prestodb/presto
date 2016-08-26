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

package com.facebook.presto.tpch;

import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.facebook.presto.spi.connector.ConnectorTableStatisticsProvider;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.facebook.presto.spi.statistics.Estimate;
import com.facebook.presto.spi.statistics.TableStatistics;
import com.google.common.collect.ImmutableMap;

import static com.facebook.presto.tpch.Types.checkType;

public class TpchTableStatisticsProvider
        implements ConnectorTableStatisticsProvider
{
    @Override
    public TableStatistics getTableStatistics(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorTableLayoutHandle layoutHandle)
    {
        TpchTableLayoutHandle layout = checkType(layoutHandle, TpchTableLayoutHandle.class, "layoutHandle");
        TpchTableHandle table = layout.getTable();

        return new TableStatistics(new Estimate(getRowCount(table)), ImmutableMap.of());
    }

    public long getRowCount(TpchTableHandle tpchTableHandle)
    {
        // todo expose row counts from airlift-tpch instead of hardcoding it here
        // todo add stats for columns
        String tableName = tpchTableHandle.getTableName();
        double scaleFactor = tpchTableHandle.getScaleFactor();
        switch (tableName) {
            case "customer":
                return (long) (150_000 * scaleFactor);
            case "orders":
                return (long) (1_500_000 * scaleFactor);
            case "lineitem":
                return (long) (6_000_000 * scaleFactor);
            case "part":
                return (long) (200_000 * scaleFactor);
            case "partsupp":
                return (long) (800_000 * scaleFactor);
            case "supplier":
                return (long) (10_000 * scaleFactor);
            case "nation":
                return 25;
            case "region":
                return 5;
            default:
                throw new IllegalArgumentException("unknown tpch table name '" + tableName + "'");
        }
    }
}
