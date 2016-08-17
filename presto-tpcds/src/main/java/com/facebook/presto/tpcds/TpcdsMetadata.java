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
package com.facebook.presto.tpcds;

import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.ConnectorNodePartitioning;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.ConnectorTableLayout;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.facebook.presto.spi.ConnectorTableLayoutResult;
import com.facebook.presto.spi.ConnectorTableMetadata;
import com.facebook.presto.spi.Constraint;
import com.facebook.presto.spi.LocalProperty;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.SchemaTablePrefix;
import com.facebook.presto.spi.SortingProperty;
import com.facebook.presto.spi.block.SortOrder;
import com.facebook.presto.spi.connector.ConnectorMetadata;
import com.facebook.presto.spi.predicate.TupleDomain;
import com.facebook.presto.spi.type.BigintType;
import com.facebook.presto.spi.type.DateType;
import com.facebook.presto.spi.type.IntegerType;
import com.facebook.presto.spi.type.TimeType;
import com.facebook.presto.spi.type.Type;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.teradata.tpcds.Table;
import com.teradata.tpcds.column.CallCenterColumn;
import com.teradata.tpcds.column.CatalogPageColumn;
import com.teradata.tpcds.column.CatalogReturnsColumn;
import com.teradata.tpcds.column.CatalogSalesColumn;
import com.teradata.tpcds.column.Column;
import com.teradata.tpcds.column.ColumnType;
import com.teradata.tpcds.column.CustomerAddressColumn;
import com.teradata.tpcds.column.CustomerColumn;
import com.teradata.tpcds.column.CustomerDemographicsColumn;
import com.teradata.tpcds.column.DateDimColumn;
import com.teradata.tpcds.column.HouseholdDemographicsColumn;
import com.teradata.tpcds.column.IncomeBandColumn;
import com.teradata.tpcds.column.InventoryColumn;
import com.teradata.tpcds.column.ItemColumn;
import com.teradata.tpcds.column.PromotionColumn;
import com.teradata.tpcds.column.ReasonColumn;
import com.teradata.tpcds.column.ShipModeColumn;
import com.teradata.tpcds.column.StoreColumn;
import com.teradata.tpcds.column.StoreReturnsColumn;
import com.teradata.tpcds.column.StoreSalesColumn;
import com.teradata.tpcds.column.TimeDimColumn;
import com.teradata.tpcds.column.WarehouseColumn;
import com.teradata.tpcds.column.WebPageColumn;
import com.teradata.tpcds.column.WebReturnsColumn;
import com.teradata.tpcds.column.WebSalesColumn;
import com.teradata.tpcds.column.WebSiteColumn;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.facebook.presto.spi.type.DecimalType.createDecimalType;
import static com.facebook.presto.spi.type.VarcharType.createVarcharType;
import static com.facebook.presto.tpcds.Types.checkType;
import static java.util.Objects.requireNonNull;

public class TpcdsMetadata
        implements ConnectorMetadata
{
    public static final String TINY_SCHEMA_NAME = "tiny";
    public static final double TINY_SCALE_FACTOR = 0.01;

    public static final List<String> SCHEMA_NAMES = ImmutableList.of(
            TINY_SCHEMA_NAME, "sf1", "sf10", "sf100", "sf300", "sf1000", "sf3000", "sf10000", "sf30000", "sf100000");

    private final Set<String> tableNames;

    public TpcdsMetadata()
    {
        ImmutableSet.Builder<String> tableNames = ImmutableSet.builder();
        for (Table table : Table.getBaseTables()) {
            tableNames.add(table.name().toLowerCase());
        }
        this.tableNames = tableNames.build();
    }

    @Override
    public List<String> listSchemaNames(ConnectorSession session)
    {
        return SCHEMA_NAMES;
    }

    @Override
    public ConnectorTableHandle getTableHandle(ConnectorSession session, SchemaTableName tableName)
    {
        requireNonNull(tableName, "tableName is null");
        if (!tableNames.contains(tableName.getTableName())) {
            return null;
        }

        // parse the scale factor
        double scaleFactor = schemaNameToScaleFactor(tableName.getSchemaName());
        if (scaleFactor < 0) {
            return null;
        }

        return new TpcdsTableHandle(tableName.getTableName(), scaleFactor);
    }

    @Override
    public List<ConnectorTableLayoutResult> getTableLayouts(
            ConnectorSession session,
            ConnectorTableHandle table,
            Constraint<ColumnHandle> constraint,
            Optional<Set<ColumnHandle>> desiredColumns)
    {
        TpcdsTableHandle tableHandle = checkType(table, TpcdsTableHandle.class, "table");
        String tableName = tableHandle.getTableName();
        Map<String, ColumnHandle> columns = getColumnHandles(session, tableHandle);
        Optional<ColumnHandle> partitioningColumn = Optional.empty();

        if (tableName.equals(Table.STORE_SALES.getName())) {
            partitioningColumn = Optional.of(columns.get(StoreSalesColumn.SS_TICKET_NUMBER.getName()));
        }
        else if (tableName.equals(Table.STORE_RETURNS.getName())) {
            partitioningColumn = Optional.of(columns.get(StoreReturnsColumn.SR_TICKET_NUMBER.getName()));
        }
        else if (tableName.equals(Table.CATALOG_SALES.getName())) {
            partitioningColumn = Optional.of(columns.get(CatalogSalesColumn.CS_ORDER_NUMBER.getName()));
        }
        else if (tableName.equals(Table.CATALOG_RETURNS.getName())) {
            partitioningColumn = Optional.of(columns.get(CatalogReturnsColumn.CR_ORDER_NUMBER.getName()));
        }
        else if (tableName.equals(Table.WEB_SALES.getName())) {
            partitioningColumn = Optional.of(columns.get(WebSalesColumn.WS_ORDER_NUMBER.getName()));
        }
        else if (tableName.equals(Table.WEB_RETURNS.getName())) {
            partitioningColumn = Optional.of(columns.get(WebReturnsColumn.WR_ORDER_NUMBER.getName()));
        }
        else if (tableName.equals(Table.INVENTORY.getName())) {
            partitioningColumn = Optional.of(columns.get(InventoryColumn.INV_ITEM_SK.getName()));
        }
        else if (tableName.equals(Table.STORE.getName())) {
            partitioningColumn = Optional.of(columns.get(StoreColumn.S_STORE_SK.getName()));
        }
        else if (tableName.equals(Table.CALL_CENTER.getName())) {
            partitioningColumn = Optional.of(columns.get(CallCenterColumn.CC_CALL_CENTER_SK.getName()));
        }
        else if (tableName.equals(Table.CATALOG_PAGE.getName())) {
            partitioningColumn = Optional.of(columns.get(CatalogPageColumn.CP_CATALOG_PAGE_SK.getName()));
        }
        else if (tableName.equals(Table.WEB_SITE.getName())) {
            partitioningColumn = Optional.of(columns.get(WebSiteColumn.WEB_SITE_SK.getName()));
        }
        else if (tableName.equals(Table.WEB_PAGE.getName())) {
            partitioningColumn = Optional.of(columns.get(WebPageColumn.WP_WEB_PAGE_SK.getName()));
        }
        else if (tableName.equals(Table.WAREHOUSE.getName())) {
            partitioningColumn = Optional.of(columns.get(WarehouseColumn.W_WAREHOUSE_SK.getName()));
        }
        else if (tableName.equals(Table.CUSTOMER.getName())) {
            partitioningColumn = Optional.of(columns.get(CustomerColumn.C_CUSTOMER_SK.getName()));
        }
        else if (tableName.equals(Table.CUSTOMER_ADDRESS.getName())) {
            partitioningColumn = Optional.of(columns.get(CustomerAddressColumn.CA_ADDRESS_SK.getName()));
        }
        else if (tableName.equals(Table.CUSTOMER_DEMOGRAPHICS.getName())) {
            partitioningColumn = Optional.of(columns.get(CustomerDemographicsColumn.CD_DEMO_SK.getName()));
        }
        else if (tableName.equals(Table.DATE_DIM.getName())) {
            partitioningColumn = Optional.of(columns.get(DateDimColumn.D_DATE_SK.getName()));
        }
        else if (tableName.equals(Table.HOUSEHOLD_DEMOGRAPHICS.getName())) {
            partitioningColumn = Optional.of(columns.get(HouseholdDemographicsColumn.HD_DEMO_SK.getName()));
        }
        else if (tableName.equals(Table.ITEM.getName())) {
            partitioningColumn = Optional.of(columns.get(ItemColumn.I_ITEM_SK.getName()));
        }
        else if (tableName.equals(Table.INCOME_BAND.getName())) {
            partitioningColumn = Optional.of(columns.get(IncomeBandColumn.IB_INCOME_BAND_SK.getName()));
        }
        else if (tableName.equals(Table.PROMOTION.getName())) {
            partitioningColumn = Optional.of(columns.get(PromotionColumn.P_PROMO_SK.getName()));
        }
        else if (tableName.equals(Table.REASON.getName())) {
            partitioningColumn = Optional.of(columns.get(ReasonColumn.R_REASON_SK.getName()));
        }
        else if (tableName.equals(Table.SHIP_MODE.getName())) {
            partitioningColumn = Optional.of(columns.get(ShipModeColumn.SM_SHIP_MODE_SK.getName()));
        }
        else if (tableName.equals(Table.TIME_DIM.getName())) {
            partitioningColumn = Optional.of(columns.get(TimeDimColumn.T_TIME_SK.getName()));
        }

        if (!partitioningColumn.isPresent()) {
            throw new IllegalArgumentException(String.format("Table %s does not exist", tableHandle.getTableName()));
        }

        ConnectorTableLayout layout = createLayoutForPartitioningColumn(tableHandle, partitioningColumn.get());
        return ImmutableList.of(new ConnectorTableLayoutResult(layout, constraint.getSummary()));
    }

    @Override
    public ConnectorTableLayout getTableLayout(ConnectorSession session, ConnectorTableLayoutHandle handle)
    {
        TpcdsTableLayoutHandle layout = checkType(handle, TpcdsTableLayoutHandle.class, "layout");

        return getTableLayouts(session, layout.getTable(), Constraint.alwaysTrue(), Optional.empty())
                .get(0)
                .getTableLayout();
    }

    @Override
    public ConnectorTableMetadata getTableMetadata(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        TpcdsTableHandle tpcdsTableHandle = checkType(tableHandle, TpcdsTableHandle.class, "tableHandle");

        Table table = Table.getTable(tpcdsTableHandle.getTableName());
        String schemaName = scaleFactorSchemaName(tpcdsTableHandle.getScaleFactor());

        return getTableMetadata(schemaName, table);
    }

    private static ConnectorTableMetadata getTableMetadata(String schemaName, Table tpcdsTable)
    {
        ImmutableList.Builder<ColumnMetadata> columns = ImmutableList.builder();
        for (Column column : tpcdsTable.getColumns()) {
            columns.add(new ColumnMetadata(column.getName(), getPrestoType(column.getType())));
    }
        SchemaTableName tableName = new SchemaTableName(schemaName, tpcdsTable.getName());
        return new ConnectorTableMetadata(tableName, columns.build());
    }

    @Override
    public Map<String, ColumnHandle> getColumnHandles(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        ImmutableMap.Builder<String, ColumnHandle> builder = ImmutableMap.builder();
        for (ColumnMetadata columnMetadata : getTableMetadata(session, tableHandle).getColumns()) {
            builder.put(columnMetadata.getName(), new TpcdsColumnHandle(columnMetadata.getName(), columnMetadata.getType()));
        }
        return builder.build();
    }

    @Override
    public ColumnMetadata getColumnMetadata(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle columnHandle)
    {
        ConnectorTableMetadata tableMetadata = getTableMetadata(session, tableHandle);
        String columnName = checkType(columnHandle, TpcdsColumnHandle.class, "columnHandle").getColumnName();

        for (ColumnMetadata column : tableMetadata.getColumns()) {
            if (column.getName().equals(columnName)) {
                return column;
            }
        }
        throw new IllegalArgumentException(String.format("Table %s does not have column %s", tableMetadata.getTable(), columnName));
    }

    @Override
    public Map<SchemaTableName, List<ColumnMetadata>> listTableColumns(ConnectorSession session, SchemaTablePrefix prefix)
    {
        ImmutableMap.Builder<SchemaTableName, List<ColumnMetadata>> tableColumns = ImmutableMap.builder();
        for (String schemaName : getSchemaNames(session, prefix.getSchemaName())) {
            for (Table tpcdsTable : Table.getBaseTables()) {
                if (prefix.getTableName() == null || tpcdsTable.getName().equals(prefix.getTableName())) {
                    ConnectorTableMetadata tableMetadata = getTableMetadata(schemaName, tpcdsTable);
                    tableColumns.put(new SchemaTableName(schemaName, tpcdsTable.getName()), tableMetadata.getColumns());
                }
            }
        }
        return tableColumns.build();
    }

    @Override
    public List<SchemaTableName> listTables(ConnectorSession session, String schemaNameOrNull)
    {
        ImmutableList.Builder<SchemaTableName> builder = ImmutableList.builder();
        for (String schemaName : getSchemaNames(session, schemaNameOrNull)) {
            for (Table tpcdsTable : Table.getBaseTables()) {
                builder.add(new SchemaTableName(schemaName, tpcdsTable.getName()));
            }
        }
            return builder.build();
    }

    private List<String> getSchemaNames(ConnectorSession session, String schemaNameOrNull)
    {
        List<String> schemaNames;
        if (schemaNameOrNull == null) {
            schemaNames = listSchemaNames(session);
        }
        else if (schemaNameToScaleFactor(schemaNameOrNull) > 0) {
            schemaNames = ImmutableList.of(schemaNameOrNull);
        }
        else {
            schemaNames = ImmutableList.of();
        }
        return schemaNames;
    }

    private static String scaleFactorSchemaName(double scaleFactor)
    {
        return "sf" + scaleFactor;
    }

    private static double schemaNameToScaleFactor(String schemaName)
    {
        if (TINY_SCHEMA_NAME.equals(schemaName)) {
            return TINY_SCALE_FACTOR;
        }

        if (!schemaName.startsWith("sf")) {
            return -1;
        }

        try {
            return Double.parseDouble(schemaName.substring(2));
        }
        catch (Exception ignored) {
            return -1;
        }
    }

    private static Type getPrestoType(ColumnType tpcdsType)
    {
        switch (tpcdsType.getBase()) {
            case IDENTIFIER:
                return BigintType.BIGINT;
            case INTEGER:
                return IntegerType.INTEGER;
            case DATE:
                return DateType.DATE;
            case DECIMAL:
                return createDecimalType(tpcdsType.getPrecision().get(), tpcdsType.getScale().get());
            case CHAR: // TODO: change when CHAR(x) gets merged
            case VARCHAR:
                return createVarcharType(tpcdsType.getPrecision().get());
            case TIME:
                return TimeType.TIME;
        }
        throw new IllegalArgumentException("Unsupported TPC-DS type " + tpcdsType);
    }

    public static ConnectorTableLayout createLayoutForPartitioningColumn(TpcdsTableHandle tableHandle, ColumnHandle partitioningColumn)
    {
        Optional<ConnectorNodePartitioning> nodePartitioning = Optional.of(new ConnectorNodePartitioning(
                new TpcdsPartitioningHandle(
                        Table.CATALOG_RETURNS.getName(),
                        Table.CATALOG_RETURNS.getScalingInfo().getRowCountForScale((int) tableHandle.getScaleFactor())),
                ImmutableList.of(partitioningColumn)
        ));
        Optional<Set<ColumnHandle>> partitioningColumns = Optional.of(ImmutableSet.of(partitioningColumn));
        List<LocalProperty<ColumnHandle>> localProperties = ImmutableList.of(new SortingProperty<>(partitioningColumn, SortOrder.ASC_NULLS_FIRST));
        return new ConnectorTableLayout(
                new TpcdsTableLayoutHandle(tableHandle),
                Optional.empty(),
                TupleDomain.all(),
                nodePartitioning,
                partitioningColumns,
                Optional.empty(),
                localProperties
        );
    }
}
