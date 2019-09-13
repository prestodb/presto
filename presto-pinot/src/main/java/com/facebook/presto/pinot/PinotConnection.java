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
package com.facebook.presto.pinot;

import com.facebook.presto.spi.PrestoException;
import com.google.inject.Inject;
import io.airlift.log.Logger;
import org.apache.pinot.common.data.Schema;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static com.facebook.presto.pinot.PinotErrorCode.PINOT_FAILURE_GETTING_SCHEMA;
import static com.facebook.presto.pinot.PinotErrorCode.PINOT_FAILURE_GETTING_TABLE;

public class PinotConnection
{
    private static final Logger log = Logger.get(PinotConnection.class);

    private final PinotClusterInfoFetcher pinotClusterInfoFetcher;

    @Inject
    public PinotConnection(PinotClusterInfoFetcher pinotClusterInfoFetcher)
    {
        this.pinotClusterInfoFetcher = pinotClusterInfoFetcher;
    }

    public Map<String, Map<String, List<String>>> getRoutingTable(String table)
            throws Exception
    {
        Map<String, Map<String, List<String>>> routingTableForTable = this.pinotClusterInfoFetcher.getRoutingTableForTable(table);
        log.debug("RoutingTable for table: %s is %s", table, Arrays.toString(routingTableForTable.entrySet().toArray()));
        return routingTableForTable;
    }

    public Map<String, String> getTimeBoundary(String table)
            throws Exception
    {
        Map<String, String> timeBoundaryForTable = this.pinotClusterInfoFetcher.getTimeBoundaryForTable(table);
        log.debug("TimeBoundary for table: %s is %s", table, Arrays.toString(timeBoundaryForTable.entrySet().toArray()));
        return timeBoundaryForTable;
    }

    public List<String> getTableNames()
    {
        try {
            return pinotClusterInfoFetcher.getAllTables();
        }
        catch (Exception e) {
            throw new PrestoException(
                    PINOT_FAILURE_GETTING_TABLE,
                    "Failed getting table names from Pinot",
                    e);
        }
    }

    public PinotTable getTable(String tableName)
    {
        List<PinotColumn> columns = getPinotColumnsForTable(tableName);
        return new PinotTable(tableName, columns);
    }

    private Schema getPinotTableSchema(String tableName)
    {
        try {
            return pinotClusterInfoFetcher.getTableSchema(tableName);
        }
        catch (Exception e) {
            throw new PrestoException(
                    PINOT_FAILURE_GETTING_SCHEMA,
                    String.format("Failed getting schema for %s", tableName),
                    e);
        }
    }

    private List<PinotColumn> getPinotColumnsForTable(String tableName)
    {
        Schema pinotTableSchema = getPinotTableSchema(tableName);
        return PinotColumnUtils.getPinotColumnsForPinotSchema(pinotTableSchema);
    }

    public PinotColumn getPinotTimeColumnForTable(String tableName)
    {
        Schema pinotTableSchema = getPinotTableSchema(tableName);
        String columnName = pinotTableSchema.getTimeColumnName();
        return new PinotColumn(columnName, PinotColumnUtils.getPrestoTypeFromPinotType(pinotTableSchema.getFieldSpecFor(columnName)));
    }
}
