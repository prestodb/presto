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
package com.facebook.presto.tests;

import com.facebook.presto.Session;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.ConnectorTableMetadata;
import com.facebook.presto.spi.RecordCursor;
import com.facebook.presto.spi.RecordSet;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.type.DecimalType;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.VarcharType;
import com.facebook.presto.testing.MaterializedResult;
import com.facebook.presto.testing.MaterializedRow;
import com.facebook.presto.tpch.TpchMetadata;
import com.facebook.presto.tpch.TpchTableHandle;
import com.google.common.base.Joiner;
import org.intellij.lang.annotations.Language;
import org.joda.time.DateTimeZone;
import org.skife.jdbi.v2.DBI;
import org.skife.jdbi.v2.Handle;
import org.skife.jdbi.v2.PreparedBatch;
import org.skife.jdbi.v2.PreparedBatchPart;
import org.skife.jdbi.v2.StatementContext;
import org.skife.jdbi.v2.tweak.ResultSetMapper;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.type.DateType.DATE;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.IntegerType.INTEGER;
import static com.facebook.presto.spi.type.TimeType.TIME;
import static com.facebook.presto.spi.type.TimeWithTimeZoneType.TIME_WITH_TIME_ZONE;
import static com.facebook.presto.spi.type.TimestampType.TIMESTAMP;
import static com.facebook.presto.spi.type.TimestampWithTimeZoneType.TIMESTAMP_WITH_TIME_ZONE;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.facebook.presto.tpch.TpchMetadata.TINY_SCHEMA_NAME;
import static com.facebook.presto.tpch.TpchRecordSet.createTpchRecordSet;
import static com.facebook.presto.type.UnknownType.UNKNOWN;
import static com.facebook.presto.util.DateTimeZoneIndex.getDateTimeZone;
import static com.facebook.presto.util.ImmutableCollectors.toImmutableList;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static io.airlift.tpch.TpchTable.LINE_ITEM;
import static io.airlift.tpch.TpchTable.ORDERS;
import static java.lang.String.format;
import static java.util.Collections.nCopies;

public class H2QueryRunner
{
    private final Handle handle;

    public H2QueryRunner()
    {
        handle = DBI.open("jdbc:h2:mem:test" + System.nanoTime());
        TpchMetadata tpchMetadata = new TpchMetadata("");

        handle.execute("CREATE TABLE orders (\n" +
                "  orderkey BIGINT PRIMARY KEY,\n" +
                "  custkey BIGINT NOT NULL,\n" +
                "  orderstatus CHAR(1) NOT NULL,\n" +
                "  totalprice DOUBLE NOT NULL,\n" +
                "  orderdate DATE NOT NULL,\n" +
                "  orderpriority CHAR(15) NOT NULL,\n" +
                "  clerk CHAR(15) NOT NULL,\n" +
                "  shippriority INTEGER NOT NULL,\n" +
                "  comment VARCHAR(79) NOT NULL\n" +
                ")");
        handle.execute("CREATE INDEX custkey_index ON orders (custkey)");
        TpchTableHandle ordersHandle = tpchMetadata.getTableHandle(null, new SchemaTableName(TINY_SCHEMA_NAME, ORDERS.getTableName()));
        insertRows(tpchMetadata.getTableMetadata(null, ordersHandle), handle, createTpchRecordSet(ORDERS, ordersHandle.getScaleFactor()));

        handle.execute("CREATE TABLE lineitem (\n" +
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
        TpchTableHandle lineItemHandle = tpchMetadata.getTableHandle(null, new SchemaTableName(TINY_SCHEMA_NAME, LINE_ITEM.getTableName()));
        insertRows(tpchMetadata.getTableMetadata(null, lineItemHandle), handle, createTpchRecordSet(LINE_ITEM, lineItemHandle.getScaleFactor()));
    }

    public void close()
    {
        handle.close();
    }

    public MaterializedResult execute(Session session, @Language("SQL") String sql, List<? extends Type> resultTypes)
    {
        MaterializedResult materializedRows = new MaterializedResult(
                handle.createQuery(sql)
                        .map(rowMapper(resultTypes))
                        .list(),
                resultTypes
        );

        // H2 produces dates in the JVM time zone instead of the session timezone
        materializedRows = materializedRows.toTimeZone(DateTimeZone.getDefault(), getDateTimeZone(session.getTimeZoneKey()));

        return materializedRows;
    }

    private static ResultSetMapper<MaterializedRow> rowMapper(final List<? extends Type> types)
    {
        return new ResultSetMapper<MaterializedRow>()
        {
            @Override
            public MaterializedRow map(int index, ResultSet resultSet, StatementContext ctx)
                    throws SQLException
            {
                int count = resultSet.getMetaData().getColumnCount();
                checkArgument(types.size() == count, "type does not match result");
                List<Object> row = new ArrayList<>(count);
                for (int i = 1; i <= count; i++) {
                    Type type = types.get(i - 1);
                    if (BOOLEAN.equals(type)) {
                        boolean booleanValue = resultSet.getBoolean(i);
                        if (resultSet.wasNull()) {
                            row.add(null);
                        }
                        else {
                            row.add(booleanValue);
                        }
                    }
                    else if (BIGINT.equals(type)) {
                        long longValue = resultSet.getLong(i);
                        if (resultSet.wasNull()) {
                            row.add(null);
                        }
                        else {
                            row.add(longValue);
                        }
                    }
                    else if (INTEGER.equals(type)) {
                        int intValue = resultSet.getInt(i);
                        if (resultSet.wasNull()) {
                            row.add(null);
                        }
                        else {
                            row.add(intValue);
                        }
                    }
                    else if (DOUBLE.equals(type)) {
                        double doubleValue = resultSet.getDouble(i);
                        if (resultSet.wasNull()) {
                            row.add(null);
                        }
                        else {
                            row.add(doubleValue);
                        }
                    }
                    else if (type instanceof VarcharType) {
                        String stringValue = resultSet.getString(i);
                        if (resultSet.wasNull()) {
                            row.add(null);
                        }
                        else {
                            row.add(stringValue);
                        }
                    }
                    else if (DATE.equals(type)) {
                        Date dateValue = resultSet.getDate(i);
                        if (resultSet.wasNull()) {
                            row.add(null);
                        }
                        else {
                            row.add(dateValue);
                        }
                    }
                    else if (TIME.equals(type) || TIME_WITH_TIME_ZONE.equals(type)) {
                        Time timeValue = resultSet.getTime(i);
                        if (resultSet.wasNull()) {
                            row.add(null);
                        }
                        else {
                            row.add(timeValue);
                        }
                    }
                    else if (TIMESTAMP.equals(type) || TIMESTAMP_WITH_TIME_ZONE.equals(type)) {
                        Timestamp timestampValue = resultSet.getTimestamp(i);
                        if (resultSet.wasNull()) {
                            row.add(null);
                        }
                        else {
                            row.add(timestampValue);
                        }
                    }
                    else if (UNKNOWN.equals(type)) {
                        Object objectValue = resultSet.getObject(i);
                        checkState(resultSet.wasNull(), "Expected a null value, but got %s", objectValue);
                        row.add(null);
                    }
                    else if (type instanceof DecimalType) {
                        BigDecimal decimalValue = resultSet.getBigDecimal(i);
                        if (resultSet.wasNull()) {
                            row.add(null);
                        }
                        else {
                            row.add(decimalValue);
                        }
                    }
                    else {
                        throw new AssertionError("unhandled type: " + type);
                    }
                }
                return new MaterializedRow(MaterializedResult.DEFAULT_PRECISION, row);
            }
        };
    }

    private static void insertRows(ConnectorTableMetadata tableMetadata, Handle handle, RecordSet data)
    {
        List<ColumnMetadata> columns = tableMetadata.getColumns().stream()
                .filter(columnMetadata -> !columnMetadata.isHidden())
                .collect(toImmutableList());

        String vars = Joiner.on(',').join(nCopies(columns.size(), "?"));
        String sql = format("INSERT INTO %s VALUES (%s)", tableMetadata.getTable().getTableName(), vars);

        RecordCursor cursor = data.cursor();
        while (true) {
            // insert 1000 rows at a time
            PreparedBatch batch = handle.prepareBatch(sql);
            for (int row = 0; row < 1000; row++) {
                if (!cursor.advanceNextPosition()) {
                    batch.execute();
                    return;
                }
                PreparedBatchPart part = batch.add();
                for (int column = 0; column < columns.size(); column++) {
                    Type type = columns.get(column).getType();
                    if (BOOLEAN.equals(type)) {
                        part.bind(column, cursor.getBoolean(column));
                    }
                    else if (BIGINT.equals(type)) {
                        part.bind(column, cursor.getLong(column));
                    }
                    else if (INTEGER.equals(type)) {
                        part.bind(column, (int) cursor.getLong(column));
                    }
                    else if (DOUBLE.equals(type)) {
                        part.bind(column, cursor.getDouble(column));
                    }
                    else if (VARCHAR.equals(type)) {
                        part.bind(column, cursor.getSlice(column).toStringUtf8());
                    }
                    else if (DATE.equals(type)) {
                        long millisUtc = TimeUnit.DAYS.toMillis(cursor.getLong(column));
                        // H2 expects dates in to be millis at midnight in the JVM timezone
                        long localMillis = DateTimeZone.UTC.getMillisKeepLocal(DateTimeZone.getDefault(), millisUtc);
                        part.bind(column, new Date(localMillis));
                    }
                    else {
                        throw new IllegalArgumentException("Unsupported type " + type);
                    }
                }
            }
            batch.execute();
        }
    }
}
