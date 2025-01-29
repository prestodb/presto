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
package com.facebook.plugin.arrow.testingServer;

import com.facebook.airlift.log.Logger;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.VarcharType;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.ConnectorTableMetadata;
import com.facebook.presto.spi.RecordCursor;
import com.facebook.presto.spi.RecordSet;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.tpch.TpchMetadata;
import com.facebook.presto.tpch.TpchTableHandle;
import com.google.common.base.Joiner;
import io.airlift.tpch.TpchTable;
import org.jdbi.v3.core.Handle;
import org.jdbi.v3.core.Jdbi;
import org.jdbi.v3.core.statement.PreparedBatch;
import org.joda.time.DateTimeZone;

import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.BooleanType.BOOLEAN;
import static com.facebook.presto.common.type.DateType.DATE;
import static com.facebook.presto.common.type.DoubleType.DOUBLE;
import static com.facebook.presto.common.type.IntegerType.INTEGER;
import static com.facebook.presto.tpch.TpchMetadata.TINY_SCHEMA_NAME;
import static com.facebook.presto.tpch.TpchRecordSet.createTpchRecordSet;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.airlift.tpch.TpchTable.CUSTOMER;
import static io.airlift.tpch.TpchTable.LINE_ITEM;
import static io.airlift.tpch.TpchTable.NATION;
import static io.airlift.tpch.TpchTable.ORDERS;
import static io.airlift.tpch.TpchTable.PART;
import static io.airlift.tpch.TpchTable.PART_SUPPLIER;
import static io.airlift.tpch.TpchTable.REGION;
import static io.airlift.tpch.TpchTable.SUPPLIER;
import static java.lang.String.format;
import static java.util.Collections.nCopies;

public class TestingH2DatabaseSetup
{
    private static final Logger logger = Logger.get(TestingH2DatabaseSetup.class);
    private TestingH2DatabaseSetup()
    {
        throw new UnsupportedOperationException("This is a utility class and cannot be instantiated");
    }

    public static void setup(String h2JdbcUrl) throws Exception
    {
        Class.forName("org.h2.Driver");

        Connection conn = DriverManager.getConnection(h2JdbcUrl, "sa", "");

        Jdbi jdbi = Jdbi.create(h2JdbcUrl, "sa", "");
        Handle handle = jdbi.open(); // Get a handle for the database connection

        TpchMetadata tpchMetadata = new TpchMetadata("");

        Statement stmt = conn.createStatement();

        // Create schema
        stmt.execute("CREATE SCHEMA IF NOT EXISTS tpch");

        stmt.execute("CREATE TABLE tpch.member (" +
                " id INTEGER PRIMARY KEY," +
                " name VARCHAR(50)," +
                " sex CHAR(1)," +
                " state CHAR(5)" +
                ")");
        stmt.execute("INSERT INTO tpch.member VALUES(1, 'TOM', 'M', 'TMX  '),(2, 'MARY', 'F', 'CD    ')");

        stmt.execute("CREATE TABLE tpch.event (" +
                " id INTEGER PRIMARY KEY," +
                " startDate DATE," +
                " startTime TIME," +
                " startTimestamp TIMESTAMP" +
                ")");
        stmt.execute("INSERT INTO tpch.event VALUES(1, DATE '2004-12-31', TIME '23:59:59'," +
                " TIMESTAMP '2005-12-31 23:59:59')");

        stmt.execute("CREATE TABLE tpch.orders (\n" +
                "  orderkey BIGINT PRIMARY KEY,\n" +
                "  custkey BIGINT NOT NULL,\n" +
                "  orderstatus VARCHAR(1) NOT NULL,\n" +
                "  totalprice DOUBLE NOT NULL,\n" +
                "  orderdate DATE NOT NULL,\n" +
                "  orderpriority VARCHAR(15) NOT NULL,\n" +
                "  clerk VARCHAR(15) NOT NULL,\n" +
                "  shippriority INTEGER NOT NULL,\n" +
                "  comment VARCHAR(79) NOT NULL\n" +
                ")");
        stmt.execute("CREATE INDEX custkey_index ON tpch.orders (custkey)");
        insertRows(tpchMetadata, ORDERS, handle);

        handle.execute("CREATE TABLE tpch.lineitem (\n" +
                "  orderkey BIGINT,\n" +
                "  partkey BIGINT NOT NULL,\n" +
                "  suppkey BIGINT NOT NULL,\n" +
                "  linenumber INTEGER,\n" +
                "  quantity DOUBLE NOT NULL,\n" +
                "  extendedprice DOUBLE NOT NULL,\n" +
                "  discount DOUBLE NOT NULL,\n" +
                "  tax DOUBLE NOT NULL,\n" +
                "  returnflag CHAR(1) NOT NULL,\n" +
                "  linestatus CHAR(1) NOT NULL,\n" +
                "  shipdate DATE NOT NULL,\n" +
                "  commitdate DATE NOT NULL,\n" +
                "  receiptdate DATE NOT NULL,\n" +
                "  shipinstruct VARCHAR(25) NOT NULL,\n" +
                "  shipmode VARCHAR(10) NOT NULL,\n" +
                "  comment VARCHAR(44) NOT NULL,\n" +
                "  PRIMARY KEY (orderkey, linenumber)" +
                ")");
        insertRows(tpchMetadata, LINE_ITEM, handle);

        handle.execute(" CREATE TABLE tpch.partsupp (\n" +
                "  partkey BIGINT NOT NULL,\n" +
                "  suppkey BIGINT NOT NULL,\n" +
                "  availqty INTEGER NOT NULL,\n" +
                "  supplycost DOUBLE NOT NULL,\n" +
                "  comment VARCHAR(199) NOT NULL,\n" +
                "  PRIMARY KEY(partkey, suppkey)" +
                ")");
        insertRows(tpchMetadata, PART_SUPPLIER, handle);

        handle.execute("CREATE TABLE tpch.nation (\n" +
                "  nationkey BIGINT PRIMARY KEY,\n" +
                "  name VARCHAR(25) NOT NULL,\n" +
                "  regionkey BIGINT NOT NULL,\n" +
                "  comment VARCHAR(152) NOT NULL\n" +
                ")");
        insertRows(tpchMetadata, NATION, handle);

        handle.execute("CREATE TABLE tpch.region(\n" +
                "  regionkey BIGINT PRIMARY KEY,\n" +
                "  name VARCHAR(25) NOT NULL,\n" +
                "  comment VARCHAR(115) NOT NULL\n" +
                ")");
        insertRows(tpchMetadata, REGION, handle);
        handle.execute("CREATE TABLE tpch.part(\n" +
                "  partkey BIGINT PRIMARY KEY,\n" +
                "  name VARCHAR(55) NOT NULL,\n" +
                "  mfgr VARCHAR(25) NOT NULL,\n" +
                "  brand VARCHAR(10) NOT NULL,\n" +
                "  type VARCHAR(25) NOT NULL,\n" +
                "  size INTEGER NOT NULL,\n" +
                "  container VARCHAR(10) NOT NULL,\n" +
                "  retailprice DOUBLE NOT NULL,\n" +
                "  comment VARCHAR(23) NOT NULL\n" +
                ")");
        insertRows(tpchMetadata, PART, handle);
        handle.execute(" CREATE TABLE tpch.customer (     \n" +
                "    custkey BIGINT NOT NULL,         \n" +
                "    name VARCHAR(25) NOT NULL,       \n" +
                "    address VARCHAR(40) NOT NULL,    \n" +
                "    nationkey BIGINT NOT NULL,       \n" +
                "    phone VARCHAR(15) NOT NULL,      \n" +
                "    acctbal DOUBLE NOT NULL,         \n" +
                "    mktsegment VARCHAR(10) NOT NULL, \n" +
                "    comment VARCHAR(117) NOT NULL    \n" +
                " ) ");
        insertRows(tpchMetadata, CUSTOMER, handle);
        handle.execute(" CREATE TABLE tpch.supplier ( \n" +
                "    suppkey bigint NOT NULL,         \n" +
                "    name varchar(25) NOT NULL,       \n" +
                "    address varchar(40) NOT NULL,    \n" +
                "    nationkey bigint NOT NULL,       \n" +
                "    phone varchar(15) NOT NULL,      \n" +
                "    acctbal double NOT NULL,         \n" +
                "    comment varchar(101) NOT NULL    \n" +
                " ) ");
        insertRows(tpchMetadata, SUPPLIER, handle);

        ResultSet resultSet1 = stmt.executeQuery("SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = 'TPCH'");
        List<String> tables = new ArrayList<>();
        while (resultSet1.next()) {
            String tableName = resultSet1.getString("TABLE_NAME");
            tables.add(tableName);
        }
        logger.info("Tables in 'tpch' schema: %s", tables.stream().collect(Collectors.joining(", ")));

        ResultSet resultSet = stmt.executeQuery("SELECT SCHEMA_NAME FROM INFORMATION_SCHEMA.SCHEMATA");
        List<String> schemas = new ArrayList<>();
        while (resultSet.next()) {
            String schemaName = resultSet.getString("SCHEMA_NAME");
            schemas.add(schemaName);
        }
        logger.info("Schemas: %s", schemas.stream().collect(Collectors.joining(", ")));
    }

    private static void insertRows(TpchMetadata tpchMetadata, TpchTable tpchTable, Handle handle)
    {
        TpchTableHandle tableHandle = tpchMetadata.getTableHandle(null, new SchemaTableName(TINY_SCHEMA_NAME, tpchTable.getTableName()));
        insertRows(tpchMetadata.getTableMetadata(null, tableHandle), handle, createTpchRecordSet(tpchTable, tableHandle.getScaleFactor()));
    }

    private static void insertRows(ConnectorTableMetadata tableMetadata, Handle handle, RecordSet data)
    {
        List<ColumnMetadata> columns = tableMetadata.getColumns().stream()
                .filter(columnMetadata -> !columnMetadata.isHidden())
                .collect(toImmutableList());

        String schemaName = "tpch";
        String tableNameWithSchema = schemaName + "." + tableMetadata.getTable().getTableName();
        String vars = Joiner.on(',').join(nCopies(columns.size(), "?"));
        String sql = format("INSERT INTO %s VALUES (%s)", tableNameWithSchema, vars);

        RecordCursor cursor = data.cursor();
        while (true) {
            // insert 1000 rows at a time
            PreparedBatch batch = handle.prepareBatch(sql);
            for (int row = 0; row < 1000; row++) {
                if (!cursor.advanceNextPosition()) {
                    if (batch.size() > 0) {
                        batch.execute();
                    }
                    return;
                }
                for (int column = 0; column < columns.size(); column++) {
                    Type type = columns.get(column).getType();
                    if (BOOLEAN.equals(type)) {
                        batch.bind(column, cursor.getBoolean(column));
                    }
                    else if (BIGINT.equals(type)) {
                        batch.bind(column, cursor.getLong(column));
                    }
                    else if (INTEGER.equals(type)) {
                        batch.bind(column, (int) cursor.getLong(column));
                    }
                    else if (DOUBLE.equals(type)) {
                        batch.bind(column, cursor.getDouble(column));
                    }
                    else if (type instanceof VarcharType) {
                        batch.bind(column, cursor.getSlice(column).toStringUtf8());
                    }
                    else if (DATE.equals(type)) {
                        long millisUtc = TimeUnit.DAYS.toMillis(cursor.getLong(column));
                        // H2 expects dates in to be millis at midnight in the JVM timezone
                        long localMillis = DateTimeZone.UTC.getMillisKeepLocal(DateTimeZone.getDefault(), millisUtc);
                        batch.bind(column, new Date(localMillis));
                    }
                    else {
                        throw new IllegalArgumentException("Unsupported type " + type);
                    }
                }
                batch.add();
            }
            batch.execute();
        }
    }
}
