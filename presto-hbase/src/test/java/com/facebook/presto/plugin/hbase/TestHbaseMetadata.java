package com.facebook.presto.plugin.hbase;

import com.facebook.presto.common.predicate.TupleDomain;
import com.facebook.presto.hbase.HbaseClient;
import com.facebook.presto.hbase.HbaseConnectorId;
import com.facebook.presto.hbase.HbaseMetadata;
import com.facebook.presto.hbase.metadata.HbaseTable;
import com.facebook.presto.hbase.model.HbaseColumnHandle;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.ConnectorInsertTableHandle;
import com.facebook.presto.spi.ConnectorOutputTableHandle;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.ConnectorTableLayout;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.facebook.presto.spi.ConnectorTableLayoutResult;
import com.facebook.presto.spi.ConnectorTableMetadata;
import com.facebook.presto.spi.Constraint;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.SchemaTablePrefix;
import com.facebook.presto.spi.TableNotFoundException;
import com.facebook.presto.spi.connector.ConnectorOutputMetadata;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.*;

import static org.mockito.Mockito.*;
import static org.testng.Assert.*;

public class TestHbaseMetadata {

    private HbaseMetadata metadata;
    private HbaseClient client;
    private ConnectorSession session;

    @BeforeMethod
    public void setUp() {
        client = mock(HbaseClient.class);
        session = mock(ConnectorSession.class);
        metadata = new HbaseMetadata(new HbaseConnectorId("test"), client);
    }

    @Test
    public void beginInsert_ShouldSetRollbackAction() {
        ConnectorTableHandle tableHandle = mock(ConnectorTableHandle.class);
        ConnectorInsertTableHandle result = metadata.beginInsert(session, tableHandle);
        assertNotNull(result);
        verify(client, never()).dropTable(any());
    }

    @Test
    public void finishInsert_ShouldClearRollbackAction() {
        ConnectorInsertTableHandle insertHandle = mock(ConnectorInsertTableHandle.class);
        Optional<ConnectorOutputMetadata> result = metadata.finishInsert(session, insertHandle, ImmutableList.of(), ImmutableList.of());
        assertFalse(result.isPresent());
    }

    @Test
    public void listSchemaNames_ShouldReturnSchemaNames() {
        when(client.getSchemaNames()).thenReturn(ImmutableSet.of("schema1", "schema2"));
        List<String> schemaNames = metadata.listSchemaNames(session);
        assertEquals(schemaNames, ImmutableList.of("schema1", "schema2"));
    }

    @Test
    public void listTables_WithSchemaName_ShouldReturnTables() {
        when(client.getTableNames("schema1")).thenReturn(ImmutableSet.of("table1", "table2"));
        List<SchemaTableName> tables = metadata.listTables(session, "schema1");
        assertEquals(tables, ImmutableList.of(new SchemaTableName("schema1", "table1"), new SchemaTableName("schema1", "table2")));
    }

    @Test
    public void dropTable_ShouldDropTable() {
        ConnectorTableHandle tableHandle = mock(ConnectorTableHandle.class);
        metadata.dropTable(session, tableHandle);
        verify(client, times(1)).dropTable(any());
    }

    @Test
    public void getTableHandle_TableExists_ShouldReturnTableHandle() {
        SchemaTableName tableName = new SchemaTableName("schema1", "table1");
        HbaseTable table = mock(HbaseTable.class);
        when(client.getTable(tableName)).thenReturn(table);
        when(table.getSchema()).thenReturn("schema1");
        when(table.getTable()).thenReturn("table1");
        when(table.getRowId()).thenReturn("rowId");
        //OngoingStubbing<Optional<String>> optionalOngoingStubbing = when(table.getScanAuthorizations()).thenReturn(ImmutableSet.of());

        ConnectorTableHandle result = metadata.getTableHandle(session, tableName);
        assertNotNull(result);
    }

    @Test
    public void getTableHandle_TableDoesNotExist_ShouldReturnNull() {
        SchemaTableName tableName = new SchemaTableName("schema1", "table1");
        when(client.getTable(tableName)).thenReturn(null);

        ConnectorTableHandle result = metadata.getTableHandle(session, tableName);
        assertNull(result);
    }

    @Test
    public void beginCreateTable_ShouldSetRollbackAction() {
        ConnectorTableMetadata tableMetadata = mock(ConnectorTableMetadata.class);
        HbaseTable table = mock(HbaseTable.class);
        when(client.createTable(tableMetadata)).thenReturn(table);
        when(table.getSchema()).thenReturn("schema1");
        when(table.getTable()).thenReturn("table1");
        when(table.getRowId()).thenReturn("rowId");
       // when(table.getScanAuthorizations()).thenReturn(ImmutableSet.of());

        ConnectorOutputTableHandle result = metadata.beginCreateTable(session, tableMetadata, Optional.empty());
        assertNotNull(result);
        verify(client, never()).dropTable(any());
    }

    @Test
    public void createTable_ShouldCreateTable() {
        ConnectorTableMetadata tableMetadata = mock(ConnectorTableMetadata.class);
        metadata.createTable(session, tableMetadata, false);
        verify(client, times(1)).createTable(tableMetadata);
    }

    @Test
    public void finishCreateTable_ShouldClearRollbackAction() {
        ConnectorOutputTableHandle tableHandle = mock(ConnectorOutputTableHandle.class);
        Optional<ConnectorOutputMetadata> result = metadata.finishCreateTable(session, tableHandle, ImmutableList.of(), ImmutableList.of());
        assertFalse(result.isPresent());
    }

    @Test
    public void getTableLayouts_ShouldReturnTableLayouts() {
        ConnectorTableHandle table = mock(ConnectorTableHandle.class);
        Constraint<ColumnHandle> constraint = mock(Constraint.class);
        when(constraint.getSummary()).thenReturn(TupleDomain.all());

        List<ConnectorTableLayoutResult> layouts = metadata.getTableLayouts(session, table, constraint, Optional.empty());
        assertEquals(layouts.size(), 1);
    }

    @Test
    public void getTableLayout_ShouldReturnTableLayout() {
        ConnectorTableLayoutHandle handle = mock(ConnectorTableLayoutHandle.class);
        ConnectorTableLayout layout = metadata.getTableLayout(session, handle);
        assertNotNull(layout);
    }

    @Test
    public void getTableMetadata_TableExists_ShouldReturnMetadata() {
        ConnectorTableHandle table = mock(ConnectorTableHandle.class);
        SchemaTableName tableName = new SchemaTableName("schema1", "table1");
        HbaseTable hbaseTable = mock(HbaseTable.class);
        when(client.getTable(tableName)).thenReturn(hbaseTable);
        when(hbaseTable.getColumnsMetadata()).thenReturn(ImmutableList.of());

        ConnectorTableMetadata metadata = this.metadata.getTableMetadata(session, table);
        assertNotNull(metadata);
    }

    @Test(expectedExceptions = TableNotFoundException.class)
    public void getTableMetadata_TableDoesNotExist_ShouldThrowException() {
        ConnectorTableHandle table = mock(ConnectorTableHandle.class);
        when(client.getTable(any())).thenReturn(null);

        this.metadata.getTableMetadata(session, table);
    }

    @Test
    public void getColumnHandles_TableExists_ShouldReturnColumnHandles() {
        ConnectorTableHandle tableHandle = mock(ConnectorTableHandle.class);
        SchemaTableName tableName = new SchemaTableName("schema1", "table1");
        HbaseTable table = mock(HbaseTable.class);
        when(client.getTable(tableName)).thenReturn(table);
        when(table.getColumns()).thenReturn(ImmutableList.of());

        Map<String, ColumnHandle> columnHandles = metadata.getColumnHandles(session, tableHandle);
        assertNotNull(columnHandles);
    }

    @Test(expectedExceptions = TableNotFoundException.class)
    public void getColumnHandles_TableDoesNotExist_ShouldThrowException() {
        ConnectorTableHandle tableHandle = mock(ConnectorTableHandle.class);
        when(client.getTable(any())).thenReturn(null);

        metadata.getColumnHandles(session, tableHandle);
    }

    @Test
    public void getColumnMetadata_ColumnExists_ShouldReturnColumnMetadata() {
        ConnectorTableHandle tableHandle = mock(ConnectorTableHandle.class);
        ColumnHandle columnHandle = mock(ColumnHandle.class);
        HbaseColumnHandle hbaseColumnHandle = mock(HbaseColumnHandle.class);
        when(columnHandle instanceof HbaseColumnHandle).thenReturn(true);
        when(hbaseColumnHandle.getColumnMetadata()).thenReturn(mock(ColumnMetadata.class));

        ColumnMetadata metadata = this.metadata.getColumnMetadata(session, tableHandle, columnHandle);
        assertNotNull(metadata);
    }

    @Test
    public void listTableColumns_ShouldReturnTableColumns() {
        SchemaTablePrefix prefix = mock(SchemaTablePrefix.class);
        SchemaTableName tableName = new SchemaTableName("schema1", "table1");
        ConnectorTableMetadata tableMetadata = mock(ConnectorTableMetadata.class);
        when(client.getTable(tableName)).thenReturn(mock(HbaseTable.class));
        when(tableMetadata.getColumns()).thenReturn(ImmutableList.of());

        Map<SchemaTableName, List<ColumnMetadata>> columns = metadata.listTableColumns(session, prefix);
        assertNotNull(columns);
    }

    @Test
    public void dropSchema_ShouldDropSchema() {
        metadata.dropSchema(session, "schema1");
        verify(client, times(1)).dropSchema("schema1");
    }
}
