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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

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
            columns.add(new ColumnMetadata(columnNaming.getName(column), getPrestoType(column), false, null, null, false, emptyMap()));
            nameToHandleMap.put(columnNaming.getName(column), new TpchColumnHandle(columnNaming.getName(column), getPrestoType(column))); //SCAFFOLDING
        }
        columns.add(new ColumnMetadata(ROW_NUMBER_COLUMN_NAME, BIGINT, null, true));
        SchemaTableName tableName = new SchemaTableName(schemaName, tpchTable.getTableName());
        List<TableConstraint<String>> constraints = getTableConstraints(tpchTable.getTableName());
        return new ConnectorTableMetadata(tableName, columns.build(), emptyMap(), Optional.empty(), ConnectorTableMetadata.rebaseTableConstraints(constraints, nameToHandleMap));
    }

    private static List<TableConstraint<String>> getTableConstraints(String tableName)
    {
        //Primary key constraints
        List<TableConstraint<String>> constraints = new ArrayList<>();
        TpchTableConstraints tpchProperties = new TpchTableConstraints();
        for (Set<String> pkcols : tpchProperties.lookupPK(tableName)) {
            PrimaryKeyConstraint pk = new PrimaryKeyConstraint(tableName + "pk", pkcols, false, true);
            constraints.add(pk);
        }
        //Unique constraints
        for (Set<String> pkcols : tpchProperties.lookupUKs(tableName)) {
            UniqueConstraint pk = new UniqueConstraint(tableName + "pk", pkcols, false, true);
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
        private final Set<String> customerPK = new HashSet<>();
        private final Set<String> lineItemPK = new HashSet<>();
        private final Set<String> ordersPK = new HashSet<>();
        private final Set<String> partPK = new HashSet<>();
        private final Set<String> suppliersPK = new HashSet<>();
        private final Set<String> partsuppPK = new HashSet<>();
        private final Set<String> nationPK = new HashSet<>();
        private final Set<String> regionPK = new HashSet<>();
        private final Map<String, List<Set<String>>> tpchPKLookup = new HashMap<String, List<Set<String>>>();

        //unique keys
        private final Set<String> ordersKey1 = new HashSet<>();
        private final Set<String> ordersKey2 = new HashSet<>();
        private final Map<String, List<Set<String>>> tpchUKLookup = new HashMap<String, List<Set<String>>>();

        public TpchTableConstraints()
        {
            //customer
            ArrayList<Set<String>> customerKeys = new ArrayList<>();
            customerPK.add("custkey");
            customerKeys.add(customerPK);
            tpchPKLookup.put(customerTableName, customerKeys);

            //orders
            ArrayList<Set<String>> ordersKeys = new ArrayList<>();
            ordersPK.add("orderkey");
            ordersKeys.add(ordersPK);
            tpchPKLookup.put(ordersTableName, ordersKeys);

            // add supperfulous unique key
            ArrayList<Set<String>> ordersUniqueKeys = new ArrayList<>();
            ordersKey1.add("orderkey");
            ordersKey1.add("custkey");
            ordersUniqueKeys.add(ordersKey1);

            // add supperfulous unique key
            ordersKey2.add("orderkey");
            ordersKey2.add("orderdate");
            ordersUniqueKeys.add(ordersKey2);
            tpchUKLookup.put(ordersTableName, ordersUniqueKeys);

            //lineitem
            ArrayList<Set<String>> lineitemKeys = new ArrayList<>();
            lineItemPK.add("linenumber");
            lineItemPK.add("orderkey");
            lineitemKeys.add(lineItemPK);
            tpchPKLookup.put(lineitemTableName, lineitemKeys);

            //part
            ArrayList<Set<String>> partKeys = new ArrayList<>();
            partPK.add("partkey");
            partKeys.add(partPK);
            tpchPKLookup.put(partTableName, partKeys);

            //suppliers
            ArrayList<Set<String>> suppliersKeys = new ArrayList<>();
            suppliersPK.add("suppkey");
            suppliersKeys.add(suppliersPK);
            tpchPKLookup.put(suppliersTableName, suppliersKeys);

            //partsupp
            ArrayList<Set<String>> partsuppKeys = new ArrayList<>();
            partsuppPK.add("partkey");
            partsuppPK.add("suppkey");
            partsuppKeys.add(partsuppPK);
            tpchPKLookup.put(partsuppTableName, partsuppKeys);

            //nation
            ArrayList<Set<String>> nationKeys = new ArrayList<>();
            nationPK.add("nationkey");
            nationKeys.add(nationPK);
            tpchPKLookup.put(nationTableName, nationKeys);

            //region
            ArrayList<Set<String>> regionKeys = new ArrayList<>();
            regionPK.add("regionkey");
            regionKeys.add(regionPK);
            tpchPKLookup.put(regionTableName, regionKeys);
        }

        public List<Set<String>> lookupPK(String tableName)
        {
            return tpchPKLookup.containsKey(tableName) ? tpchPKLookup.get(tableName) : Collections.emptyList();
        }

        public List<Set<String>> lookupUKs(String tableName)
        {
            return tpchUKLookup.containsKey(tableName) ? tpchUKLookup.get(tableName) : Collections.emptyList();
        }
    }
}
