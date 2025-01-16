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
package com.facebook.presto.sql.planner;

import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.ConnectorTableMetadata;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.constraints.PrimaryKeyConstraint;
import com.facebook.presto.spi.constraints.TableConstraint;
import com.facebook.presto.spi.constraints.UniqueConstraint;
import com.facebook.presto.tpch.ColumnNaming;
import com.facebook.presto.tpch.TpchColumnHandle;
import com.facebook.presto.tpch.TpchMetadata;
import com.facebook.presto.tpch.TpchTableHandle;
import com.google.common.collect.ImmutableList;
import io.airlift.tpch.TpchColumn;
import io.airlift.tpch.TpchEntity;
import io.airlift.tpch.TpchTable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static java.util.Collections.emptyMap;

public class TestTableConstraintsMetadata
        extends TpchMetadata
{
    public TestTableConstraintsMetadata(String connectorId, ColumnNaming columnNaming, boolean predicatePushdownEnabled, boolean partitioningEnabled)
    {
        super(connectorId, columnNaming, predicatePushdownEnabled, partitioningEnabled);
    }

    private static ConnectorTableMetadata getTableConstraintsMetadata(String schemaName, TpchTable<?> tpchTable, ColumnNaming columnNaming)
    {
        ImmutableList.Builder<ColumnMetadata> columns = ImmutableList.builder();
        Map<String, ColumnHandle> nameToHandleMap = new HashMap<>(); //SCAFFOLDING
        for (TpchColumn<? extends TpchEntity> column : tpchTable.getColumns()) {
            columns.add(ColumnMetadata.builder()
                    .setName(columnNaming.getName(column))
                    .setType(getPrestoType(column))
                    .setNullable(false)
                    .build());
            nameToHandleMap.put(columnNaming.getName(column), new TpchColumnHandle(columnNaming.getName(column), getPrestoType(column))); //SCAFFOLDING
        }
        columns.add(ColumnMetadata.builder()
                .setName(ROW_NUMBER_COLUMN_NAME)
                .setType(BIGINT)
                .setHidden(true)
                .build());
        SchemaTableName tableName = new SchemaTableName(schemaName, tpchTable.getTableName());
        List<TableConstraint<String>> constraints = getTableConstraints(tpchTable.getTableName());
        return new ConnectorTableMetadata(tableName, columns.build(), emptyMap(), Optional.empty(), constraints, nameToHandleMap);
    }

    private static List<TableConstraint<String>> getTableConstraints(String tableName)
    {
        //Primary key constraints
        List<TableConstraint<String>> constraints = new ArrayList<>();
        TpchTableConstraints tpchProperties = new TpchTableConstraints();
        for (LinkedHashSet<String> pkcols : tpchProperties.lookupPK(tableName)) {
            PrimaryKeyConstraint pk = new PrimaryKeyConstraint(Optional.of(tableName + "pk"), pkcols, false, true, false);
            constraints.add(pk);
        }
        //Unique constraints
        for (LinkedHashSet<String> pkcols : tpchProperties.lookupUKs(tableName)) {
            UniqueConstraint pk = new UniqueConstraint(Optional.of(tableName + "pk"), pkcols, false, true, false);
            constraints.add(pk);
        }
        return constraints;
    }

    @Override
    public ConnectorTableMetadata getTableMetadata(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        TpchTableHandle tpchTableHandle = (TpchTableHandle) tableHandle;
        TpchTable<?> tpchTable = TpchTable.getTable(tpchTableHandle.getTableName());
        String schemaName = TpchMetadata.scaleFactorSchemaName(tpchTableHandle.getScaleFactor());
        return getTableConstraintsMetadata(schemaName, tpchTable, getColumnNaming());
    }

    private static class TpchTableConstraints
    {
        private final String customerTableName = "customer";
        private final String lineitemTableName = "lineitem";
        private final String ordersTableName = "orders";
        private final String partTableName = "part";
        private final String suppliersTableName = "supplier";
        private final String partsuppTableName = "partsupp";
        private final String nationTableName = "nation";
        private final String regionTableName = "region";

        //primary keys
        private final LinkedHashSet<String> customerPK = new LinkedHashSet<>();
        private final LinkedHashSet<String> lineItemPK = new LinkedHashSet<>();
        private final LinkedHashSet<String> ordersPK = new LinkedHashSet<>();
        private final LinkedHashSet<String> partPK = new LinkedHashSet<>();
        private final LinkedHashSet<String> suppliersPK = new LinkedHashSet<>();
        private final LinkedHashSet<String> partsuppPK = new LinkedHashSet<>();
        private final LinkedHashSet<String> nationPK = new LinkedHashSet<>();
        private final LinkedHashSet<String> regionPK = new LinkedHashSet<>();
        private final Map<String, List<LinkedHashSet<String>>> tpchPKLookup = new HashMap<String, List<LinkedHashSet<String>>>();

        //unique keys
        private final LinkedHashSet<String> ordersKey1 = new LinkedHashSet<>();
        private final LinkedHashSet<String> ordersKey2 = new LinkedHashSet<>();
        private final Map<String, List<LinkedHashSet<String>>> tpchUKLookup = new HashMap<String, List<LinkedHashSet<String>>>();

        public TpchTableConstraints()
        {
            //customer
            ArrayList<LinkedHashSet<String>> customerKeys = new ArrayList<>();
            customerPK.add("custkey");
            customerKeys.add(customerPK);
            tpchPKLookup.put(customerTableName, customerKeys);

            //orders
            ArrayList<LinkedHashSet<String>> ordersKeys = new ArrayList<>();
            ordersPK.add("orderkey");
            ordersKeys.add(ordersPK);
            tpchPKLookup.put(ordersTableName, ordersKeys);

            // add supperfulous unique key
            ArrayList<LinkedHashSet<String>> ordersUniqueKeys = new ArrayList<>();
            ordersKey1.add("orderkey");
            ordersKey1.add("custkey");
            ordersUniqueKeys.add(ordersKey1);

            // add supperfulous unique key
            ordersKey2.add("orderkey");
            ordersKey2.add("orderdate");
            ordersUniqueKeys.add(ordersKey2);
            tpchUKLookup.put(ordersTableName, ordersUniqueKeys);

            //lineitem
            ArrayList<LinkedHashSet<String>> lineitemKeys = new ArrayList<>();
            lineItemPK.add("linenumber");
            lineItemPK.add("orderkey");
            lineitemKeys.add(lineItemPK);
            tpchPKLookup.put(lineitemTableName, lineitemKeys);

            //part
            ArrayList<LinkedHashSet<String>> partKeys = new ArrayList<>();
            partPK.add("partkey");
            partKeys.add(partPK);
            tpchPKLookup.put(partTableName, partKeys);

            //suppliers
            ArrayList<LinkedHashSet<String>> suppliersKeys = new ArrayList<>();
            suppliersPK.add("suppkey");
            suppliersKeys.add(suppliersPK);
            tpchPKLookup.put(suppliersTableName, suppliersKeys);

            //partsupp
            ArrayList<LinkedHashSet<String>> partsuppKeys = new ArrayList<>();
            partsuppPK.add("partkey");
            partsuppPK.add("suppkey");
            partsuppKeys.add(partsuppPK);
            tpchPKLookup.put(partsuppTableName, partsuppKeys);

            //nation
            ArrayList<LinkedHashSet<String>> nationKeys = new ArrayList<>();
            nationPK.add("nationkey");
            nationKeys.add(nationPK);
            tpchPKLookup.put(nationTableName, nationKeys);

            //region
            ArrayList<LinkedHashSet<String>> regionKeys = new ArrayList<>();
            regionPK.add("regionkey");
            regionKeys.add(regionPK);
            tpchPKLookup.put(regionTableName, regionKeys);
        }

        public List<LinkedHashSet<String>> lookupPK(String tableName)
        {
            return tpchPKLookup.containsKey(tableName) ? tpchPKLookup.get(tableName) : Collections.emptyList();
        }

        public List<LinkedHashSet<String>> lookupUKs(String tableName)
        {
            return tpchUKLookup.containsKey(tableName) ? tpchUKLookup.get(tableName) : Collections.emptyList();
        }
    }
}
