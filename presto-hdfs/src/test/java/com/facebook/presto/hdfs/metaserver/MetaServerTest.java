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
package com.facebook.presto.hdfs.metaserver;

import com.facebook.presto.hdfs.HDFSColumnHandle;
import com.facebook.presto.hdfs.HDFSConfig;
import com.facebook.presto.hdfs.HDFSDatabase;
import com.facebook.presto.hdfs.HDFSTableHandle;
import com.facebook.presto.hdfs.HDFSTableLayoutHandle;
import com.facebook.presto.hdfs.StorageFormat;
import com.facebook.presto.hdfs.fs.FSFactory;
import com.facebook.presto.hdfs.function.Function;
import com.facebook.presto.hdfs.function.Function0;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorTableMetadata;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.SchemaTablePrefix;
import com.facebook.presto.spi.security.Identity;
import com.facebook.presto.spi.type.CharType;
import com.facebook.presto.spi.type.DoubleType;
import com.facebook.presto.spi.type.IntegerType;
import com.facebook.presto.spi.type.TimeZoneKey;
import com.facebook.presto.spi.type.TimestampType;
import com.facebook.presto.spi.type.VarcharType;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Optional;

import static com.facebook.presto.spi.StandardErrorCode.INVALID_SESSION_PROPERTY;
import static com.facebook.presto.spi.type.TimeZoneKey.UTC_KEY;
import static java.util.Locale.ENGLISH;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertEqualsNoOrder;
import static org.testng.Assert.fail;

/**
 * presto-root
 *
 * @author guodong
 */
public class MetaServerTest
{
    HDFSConfig hdfsConfig;
    FSFactory fsFactory;
    MetaServer metaServer;
    ConnectorSession session;
    String connectorId = "0";

    @BeforeTest
    public void setup()
    {
        hdfsConfig = new HDFSConfig();

        hdfsConfig.setJdbcDriver("org.postgresql.Driver");
        hdfsConfig.setMetaserverPass("jelly");
        hdfsConfig.setMetaserverStore("hdfs://127.0.0.1:9000/warehouse");
        hdfsConfig.setMetaserverUri("jdbc:postgresql://127.0.0.1:5432/metabase");
        hdfsConfig.setMetaserverUser("jelly");

        fsFactory = new FSFactory(hdfsConfig);
        metaServer = new JDBCMetaServer(hdfsConfig, fsFactory);

        session = new TestConnectorSession();
    }

    @Test
    public void testCreateDatabase()
    {
        HDFSDatabase database = new HDFSDatabase("test");
        metaServer.createDatabase(session, database);
    }

    @Test
    public void testCreateTable()
    {
        List<ColumnMetadata> columns = new ArrayList<>();
        ColumnMetadata name = new ColumnMetadata("name", VarcharType.createVarcharType(20));
        ColumnMetadata age = new ColumnMetadata("age", IntegerType.INTEGER);
        ColumnMetadata salary = new ColumnMetadata("salary", DoubleType.DOUBLE);
        ColumnMetadata time = new ColumnMetadata("time", TimestampType.TIMESTAMP);
        ColumnMetadata comment = new ColumnMetadata("comment", CharType.createCharType(10));
        columns.add(name);
        columns.add(age);
        columns.add(salary);
        columns.add(time);
        columns.add(comment);

        ConnectorTableMetadata table = new ConnectorTableMetadata(new SchemaTableName("test", "employee"), columns);

        metaServer.createTable(session, table);
    }

    @Test
    public void testCreateTableWithFiber()
    {
        List<ColumnMetadata> columns = new ArrayList<>();
        ColumnMetadata name = new ColumnMetadata("name", VarcharType.createVarcharType(20));
        ColumnMetadata age = new ColumnMetadata("age", IntegerType.INTEGER);
        ColumnMetadata salary = new ColumnMetadata("salary", DoubleType.DOUBLE);
        ColumnMetadata time = new ColumnMetadata("time", TimestampType.TIMESTAMP);
        ColumnMetadata comment = new ColumnMetadata("comment", CharType.createCharType(10));
        columns.add(name);
        columns.add(age);
        columns.add(salary);
        columns.add(time);
        columns.add(comment);

        ConnectorTableMetadata table = new ConnectorTableMetadata(new SchemaTableName("test", "student"), columns);

        String fiberKey = "name";
        String function = "function0";
        String timeKey = "time";

        metaServer.createTableWithFiber(session, table, fiberKey, function, timeKey);
    }

    @Test
    public void testShowTables()
    {
        // list tables whose database name is "test"
        List<SchemaTableName> namesWithDB = metaServer.listTables(new SchemaTablePrefix("test"));
        List<SchemaTableName> expectedDB = new ArrayList<>();
        SchemaTableName employee = new SchemaTableName("test", "employee");
        SchemaTableName student = new SchemaTableName("test", "student");
        expectedDB.add(employee);
        expectedDB.add(student);

        assertEqualsNoOrder(namesWithDB.toArray(), expectedDB.toArray());
        namesWithDB.stream().forEach(System.out::println);

        // list tables whose database name is "test" and table name is "student"
        List<SchemaTableName> namesWithTable = metaServer.listTables(new SchemaTablePrefix("test", "student"));
        List<SchemaTableName> expectedTbl = new ArrayList<>();
        expectedTbl.add(student);

        assertEqualsNoOrder(namesWithTable.toArray(), expectedTbl.toArray());
        namesWithTable.stream().forEach(System.out::println);
    }

    // DONE
    @Test
    public void testShowAllDatabases()
    {
        List<String> names = metaServer.getAllDatabases();
        List<String> result = new ArrayList<>();
        result.add("default");
        result.add("test");

        assertEqualsNoOrder(names.toArray(), result.toArray());
        names.stream().forEach(System.out::println);
    }

//    @Test
//    public void testGetTableHandle()
//    {
//        HDFSTableHandle tableHandle = metaServer.getTableHandle(connectorId, "test", "student").orElse(null);
//        if (tableHandle == null) {
//            fail("Get no table handle for table test.student");
//        }
//        HDFSTableHandle result = new HDFSTableHandle(connectorId, "test", "student", "hdfs://127.0.0.1:9000/warehouse/test/student");
//
//        assertEquals(tableHandle, result);
//        System.out.println(tableHandle);
//    }

    @Test
    public void testDescribe()
    {
        HDFSTableLayoutHandle tableLayoutHandle = metaServer.getTableLayout(connectorId, "test", "student").orElse(null);
        if (tableLayoutHandle == null) {
            fail("Get no table layout handle for table test.student");
        }
        HDFSTableHandle handle = new HDFSTableHandle(connectorId, "test", "student", "hdfs://127.0.0.1:9000/warehouse/test/student");
        HDFSColumnHandle name = new HDFSColumnHandle("name", VarcharType.createVarcharType(20), "", HDFSColumnHandle.ColumnType.FIBER_COL, connectorId);
        HDFSColumnHandle time = new HDFSColumnHandle("time", TimestampType.TIMESTAMP, "", HDFSColumnHandle.ColumnType.TIME_COL, connectorId);
        Function function = new Function0(80);
        HDFSTableLayoutHandle result = new HDFSTableLayoutHandle(handle, name, time, function, StorageFormat.PARQUET, Optional.empty());

        assertEquals(tableLayoutHandle, result);
        System.out.println(tableLayoutHandle);
    }

//    @Test
//    public void testGetTableColMetadata()
//    {
//        List<ColumnMetadata> metadatas = metaServer.getTableColMetadata(connectorId, "test", "student").orElse(null);
//        if (metadatas == null) {
//            fail("Get no table column metadatas for table test.student");
//        }
//        List<ColumnMetadata> result = new ArrayList<>();
//        ColumnMetadata name = new ColumnMetadata("name", VarcharType.createVarcharType(20));
//        ColumnMetadata age = new ColumnMetadata("age", IntegerType.INTEGER);
//        ColumnMetadata salary = new ColumnMetadata("salary", DoubleType.DOUBLE);
//        ColumnMetadata time = new ColumnMetadata("time", TimestampType.TIMESTAMP);
//        ColumnMetadata comment = new ColumnMetadata("comment", CharType.createCharType(10));
//        result.add(name);
//        result.add(age);
//        result.add(salary);
//        result.add(time);
//        result.add(comment);
//
//        assertEqualsNoOrder(metadatas.toArray(), result.toArray());
//        metadatas.stream().forEach(System.out::println);
//    }

//    @Test
//    public void testGetTableColumnHandle()
//    {
//        List<HDFSColumnHandle> columns = metaServer.getTableColumnHandle(connectorId, "test", "student").orElse(null);
//        if (columns == null) {
//            fail("Get no column handles from table test.student");
//        }
//
//        List<HDFSColumnHandle> result = new ArrayList<>();
//        HDFSColumnHandle name = new HDFSColumnHandle("name", VarcharType.createVarcharType(20), "", HDFSColumnHandle.ColumnType.FIBER_COL, connectorId);
//        HDFSColumnHandle age = new HDFSColumnHandle("age", IntegerType.INTEGER, "", HDFSColumnHandle.ColumnType.REGULAR, connectorId);
//        HDFSColumnHandle salary = new HDFSColumnHandle("salary", DoubleType.DOUBLE, "", HDFSColumnHandle.ColumnType.REGULAR, connectorId);
//        HDFSColumnHandle time = new HDFSColumnHandle("time", TimestampType.TIMESTAMP, "", HDFSColumnHandle.ColumnType.TIME_COL, connectorId);
//        HDFSColumnHandle comment = new HDFSColumnHandle("comment", CharType.createCharType(10), "", HDFSColumnHandle.ColumnType.REGULAR, connectorId);
//        result.add(name);
//        result.add(age);
//        result.add(salary);
//        result.add(time);
//        result.add(comment);
//
//        assertEqualsNoOrder(columns.toArray(), result.toArray());
//        columns.stream().forEach(System.out::println);
//    }

    @Test
    public void testRenameColumn()
    {
        metaServer.renameColumn("test", "employee", "comment", "commentnew");
    }

    @Test
    public void testRenameTable()
    {
        metaServer.renameTable("test", "student", "studentnew");
    }

    @Test
    public void testCreateFiber()
    {
        metaServer.createFiber("test", "studentnew", 10);
    }

    @Test
    public void testUpdateFiber()
    {
        metaServer.updateFiber("test", "studentnew", 10, 20);
    }

    @Test
    public void testGetFibers()
    {
        List<Long> result = metaServer.getFibers("test", "studentnew");
        for (long r : result) {
            System.out.println("Fiber value: " + r);
        }
    }

    @Test
    public void testAddBlock()
    {
        metaServer.addBlock(1, "2016-08-09 20:40:15.443", "2016-08-09 20:48:15.443", "hdfs://127.0.0.1:9000/test");
    }

    @Test
    public void testGetBlocks()
    {
        List<String> result = metaServer.getBlocks(1, "2016-08-09 20:40:15.443", "2016-08-09 20:50:15.443");
        for (String r : result) {
            System.out.println("Block: " + r);
        }
    }

    @Test
    public void testDeleteBlock()
    {
        metaServer.deleteBlock(1, "hdfs://127.0.0.1:9000/test");
    }

    @Test
    public void testDeleteFiber()
    {
        metaServer.deleteFiber("test", "studentnew", 20);
    }

    @Test
    public void testDeleteTable()
    {
        metaServer.deleteTable("test", "employee");
    }

    @Test
    public void testDeleteDatabase()
    {
        metaServer.deleteDatabase("test");
    }

    private class TestConnectorSession implements ConnectorSession
    {
        @Override
        public String getQueryId()
        {
            return "test-meta-server";
        }

        @Override
        public Identity getIdentity()
        {
            return new Identity("user", Optional.empty());
        }

        @Override
        public TimeZoneKey getTimeZoneKey()
        {
            return UTC_KEY;
        }

        @Override
        public Locale getLocale()
        {
            return ENGLISH;
        }

        @Override
        public long getStartTime()
        {
            return 0;
        }

        @Override
        public <T> T getProperty(String name, Class<T> type)
        {
            throw new PrestoException(INVALID_SESSION_PROPERTY, "Unknown session property " + name);
        }
    }
}
