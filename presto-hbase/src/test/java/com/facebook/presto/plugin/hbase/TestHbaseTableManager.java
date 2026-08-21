package com.facebook.presto.plugin.hbase;

import static org.junit.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.*;

import java.io.IOException;

import com.facebook.presto.hbase.HbaseTableManager;
import com.facebook.presto.hbase.metadata.HbaseTable;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.junit.Before;
import org.junit.Test;


import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.SchemaTableName;

public class TestHbaseTableManager {

    private Connection connection;
    private Admin admin;
    private Table table;
    private ResultScanner resultScanner;
    private Result result;
    private HbaseTableManager hbaseTableManager;


    @Before
    public void setUp() throws IOException {
        connection = mock(Connection.class);
        admin = mock(Admin.class);
        table = mock(Table.class);
        resultScanner = mock(ResultScanner.class);
        result = mock(Result.class);

        when(connection.getAdmin()).thenReturn(admin);
        when(connection.getTable(any(TableName.class))).thenReturn(table);
        when(table.getScanner(any(Scan.class))).thenReturn(resultScanner); // 使用具体的参数匹配器
        when(resultScanner.next()).thenReturn(result);

        hbaseTableManager = new HbaseTableManager(connection);
    }

    @Test
    public void getTable_TableExistsAndHasData_ReturnsHbaseTable() throws IOException {
        HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf("testSchema", "testTable"));
        HColumnDescriptor columnFamily = new HColumnDescriptor("cf");
        tableDescriptor.addFamily(columnFamily);

        when(admin.getDescriptor(any(TableName.class))).thenReturn(tableDescriptor);
        when(result.getFamilyMap(columnFamily.getName())).thenReturn(mock(java.util.NavigableMap.class));

        HbaseTable hbaseTable = hbaseTableManager.getTable(new SchemaTableName("testSchema", "testTable"));

        assertNotNull(hbaseTable);
        assertEquals("testSchema", hbaseTable.getSchema());
        assertEquals("testTable", hbaseTable.getTable());
        assertTrue(hbaseTable.getColumns().size() > 0);
    }

    @Test(expected = PrestoException.class)
    public void getTable_TableDoesNotExist_ThrowsPrestoException() throws IOException {
        when(admin.getDescriptor(any(TableName.class))).thenThrow(new IOException("Table not found"));

        hbaseTableManager.getTable(new SchemaTableName("testSchema", "nonExistentTable"));
    }

    @Test
    public void getTable_TableExistsButNoData_ReturnsEmptyHbaseTable() throws IOException {
        HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf("testSchema", "testTable"));
        HColumnDescriptor columnFamily = new HColumnDescriptor("cf");
        tableDescriptor.addFamily(columnFamily);

        when(admin.getDescriptor(any(TableName.class))).thenReturn(tableDescriptor);
        when(resultScanner.next()).thenReturn(null);

        HbaseTable hbaseTable = hbaseTableManager.getTable(new SchemaTableName("testSchema", "testTable"));

        assertNotNull(hbaseTable);
        assertEquals("testSchema", hbaseTable.getSchema());
        assertEquals("testTable", hbaseTable.getTable());
        assertTrue(hbaseTable.getColumns().isEmpty());
    }

    @Test
    public void getTable_TableExistsButNoColumnFamilies_ReturnsEmptyHbaseTable() throws IOException {
        HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf("testSchema", "testTable"));

        when(admin.getDescriptor(any(TableName.class))).thenReturn(tableDescriptor);

        HbaseTable hbaseTable = hbaseTableManager.getTable(new SchemaTableName("testSchema", "testTable"));

        assertNotNull(hbaseTable);
        assertEquals("testSchema", hbaseTable.getSchema());
        assertEquals("testTable", hbaseTable.getTable());
        assertTrue(hbaseTable.getColumns().isEmpty());
    }
}

