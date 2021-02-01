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
import com.facebook.presto.common.type.ArrayType;
import com.facebook.presto.common.type.CharType;
import com.facebook.presto.common.type.DecimalType;
import com.facebook.presto.common.type.RowType;
import com.facebook.presto.common.type.TimestampType;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.VarcharType;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.ConnectorTableMetadata;
import com.facebook.presto.spi.RecordCursor;
import com.facebook.presto.spi.RecordSet;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.testing.ExpectedQueryRunner;
import com.facebook.presto.testing.MaterializedResult;
import com.facebook.presto.testing.MaterializedRow;
import com.facebook.presto.tpch.TpchMetadata;
import com.facebook.presto.tpch.TpchTableHandle;
import com.google.common.base.Joiner;
import io.airlift.tpch.TpchTable;
import org.intellij.lang.annotations.Language;
import org.jdbi.v3.core.Handle;
import org.jdbi.v3.core.Jdbi;
import org.jdbi.v3.core.mapper.RowMapper;
import org.jdbi.v3.core.statement.ParsedSql;
import org.jdbi.v3.core.statement.PreparedBatch;
import org.jdbi.v3.core.statement.SqlParser;
import org.jdbi.v3.core.statement.StatementContext;
import org.joda.time.DateTimeZone;

import java.math.BigDecimal;
import java.math.MathContext;
import java.sql.Array;
import java.sql.Date;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.BooleanType.BOOLEAN;
import static com.facebook.presto.common.type.Chars.isCharType;
import static com.facebook.presto.common.type.DateType.DATE;
import static com.facebook.presto.common.type.DoubleType.DOUBLE;
import static com.facebook.presto.common.type.IntegerType.INTEGER;
import static com.facebook.presto.common.type.RealType.REAL;
import static com.facebook.presto.common.type.SmallintType.SMALLINT;
import static com.facebook.presto.common.type.TimeType.TIME;
import static com.facebook.presto.common.type.TimeWithTimeZoneType.TIME_WITH_TIME_ZONE;
import static com.facebook.presto.common.type.TimestampType.TIMESTAMP;
import static com.facebook.presto.common.type.TimestampWithTimeZoneType.TIMESTAMP_WITH_TIME_ZONE;
import static com.facebook.presto.common.type.TinyintType.TINYINT;
import static com.facebook.presto.common.type.UnknownType.UNKNOWN;
import static com.facebook.presto.common.type.VarbinaryType.VARBINARY;
import static com.facebook.presto.common.type.Varchars.isVarcharType;
import static com.facebook.presto.tpch.TpchMetadata.TINY_SCHEMA_NAME;
import static com.facebook.presto.tpch.TpchRecordSet.createTpchRecordSet;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Strings.padEnd;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.Lists.newArrayList;
import static io.airlift.tpch.TpchTable.LINE_ITEM;
import static io.airlift.tpch.TpchTable.NATION;
import static io.airlift.tpch.TpchTable.ORDERS;
import static io.airlift.tpch.TpchTable.PART;
import static io.airlift.tpch.TpchTable.PART_SUPPLIER;
import static io.airlift.tpch.TpchTable.REGION;
import static java.lang.String.format;
import static java.util.Collections.nCopies;

public class H2QueryRunner
        implements ExpectedQueryRunner
{
    private final Handle handle;

    public H2QueryRunner()
    {
        handle = Jdbi.open("jdbc:h2:mem:test" + System.nanoTime());
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
        insertRows(tpchMetadata, ORDERS);

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
        insertRows(tpchMetadata, LINE_ITEM);

        handle.execute(" CREATE TABLE partsupp (\n" +
                "  partkey BIGINT NOT NULL,\n" +
                "  suppkey BIGINT NOT NULL,\n" +
                "  availqty INTEGER NOT NULL,\n" +
                "  supplycost DOUBLE NOT NULL,\n" +
                "  comment VARCHAR(199) NOT NULL,\n" +
                "  PRIMARY KEY(partkey, suppkey)" +
                ")");
        insertRows(tpchMetadata, PART_SUPPLIER);

        handle.execute("CREATE TABLE nation (\n" +
                "  nationkey BIGINT PRIMARY KEY,\n" +
                "  name VARCHAR(25) NOT NULL,\n" +
                "  regionkey BIGINT NOT NULL,\n" +
                "  comment VARCHAR(114) NOT NULL\n" +
                ")");
        insertRows(tpchMetadata, NATION);

        handle.execute("CREATE TABLE region(\n" +
                "  regionkey BIGINT PRIMARY KEY,\n" +
                "  name VARCHAR(25) NOT NULL,\n" +
                "  comment VARCHAR(115) NOT NULL\n" +
                ")");
        insertRows(tpchMetadata, REGION);

        handle.execute("CREATE TABLE part(\n" +
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
        insertRows(tpchMetadata, PART);
    }

    private void insertRows(TpchMetadata tpchMetadata, TpchTable tpchTable)
    {
        TpchTableHandle tableHandle = tpchMetadata.getTableHandle(null, new SchemaTableName(TINY_SCHEMA_NAME, tpchTable.getTableName()));
        insertRows(tpchMetadata.getTableMetadata(null, tableHandle), handle, createTpchRecordSet(tpchTable, tableHandle.getScaleFactor()));
    }

    @Override
    public void close()
    {
        handle.close();
    }

    public MaterializedResult execute(Session session, @Language("SQL") String sql, List<? extends Type> resultTypes)
    {
        MaterializedResult materializedRows = new MaterializedResult(
                handle.setSqlParser(new RawSqlParser())
                        .setTemplateEngine((template, context) -> template)
                        .createQuery(sql)
                        .map(rowMapper(resultTypes))
                        .list(),
                resultTypes);

        return materializedRows;
    }

    public Handle getHandle()
    {
        return handle;
    }

    private static RowMapper<MaterializedRow> rowMapper(List<? extends Type> types)
    {
        return new RowMapper<MaterializedRow>()
        {
            @Override
            public MaterializedRow map(ResultSet resultSet, StatementContext context)
                    throws SQLException
            {
                int count = resultSet.getMetaData().getColumnCount();
                checkArgument(types.size() == count, "expected types count (%s) does not match actual column count (%s)", types.size(), count);
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
                    else if (TINYINT.equals(type)) {
                        byte byteValue = resultSet.getByte(i);
                        if (resultSet.wasNull()) {
                            row.add(null);
                        }
                        else {
                            row.add(byteValue);
                        }
                    }
                    else if (SMALLINT.equals(type)) {
                        short shortValue = resultSet.getShort(i);
                        if (resultSet.wasNull()) {
                            row.add(null);
                        }
                        else {
                            row.add(shortValue);
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
                    else if (BIGINT.equals(type)) {
                        long longValue = resultSet.getLong(i);
                        if (resultSet.wasNull()) {
                            row.add(null);
                        }
                        else {
                            row.add(longValue);
                        }
                    }
                    else if (REAL.equals(type)) {
                        float floatValue = resultSet.getFloat(i);
                        if (resultSet.wasNull()) {
                            row.add(null);
                        }
                        else {
                            row.add(floatValue);
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
                    else if (isVarcharType(type)) {
                        String stringValue = resultSet.getString(i);
                        if (resultSet.wasNull()) {
                            row.add(null);
                        }
                        else {
                            row.add(stringValue);
                        }
                    }
                    else if (isCharType(type)) {
                        String stringValue = resultSet.getString(i);
                        if (resultSet.wasNull()) {
                            row.add(null);
                        }
                        else {
                            row.add(padEnd(stringValue, ((CharType) type).getLength(), ' '));
                        }
                    }
                    else if (VARBINARY.equals(type)) {
                        byte[] binary = resultSet.getBytes(i);
                        if (resultSet.wasNull()) {
                            row.add(null);
                        }
                        else {
                            row.add(binary);
                        }
                    }
                    else if (DATE.equals(type)) {
                        // resultSet.getDate(i) doesn't work if JVM's zone skipped day being retrieved (e.g. 2011-12-30 and Pacific/Apia zone)
                        LocalDate dateValue = resultSet.getObject(i, LocalDate.class);
                        if (resultSet.wasNull()) {
                            row.add(null);
                        }
                        else {
                            row.add(dateValue);
                        }
                    }
                    else if (TIME.equals(type)) {
                        // resultSet.getTime(i) doesn't work if JVM's zone had forward offset change during 1970-01-01 (e.g. America/Hermosillo zone)
                        LocalTime timeValue = resultSet.getObject(i, LocalTime.class);
                        if (resultSet.wasNull()) {
                            row.add(null);
                        }
                        else {
                            row.add(timeValue);
                        }
                    }
                    else if (TIME_WITH_TIME_ZONE.equals(type)) {
                        throw new UnsupportedOperationException("H2 does not support TIME WITH TIME ZONE");
                    }
                    else if (TIMESTAMP.equals(type)) {
                        // resultSet.getTimestamp(i) doesn't work if JVM's zone had forward offset at the date/time being retrieved
                        LocalDateTime timestampValue;
                        try {
                            timestampValue = resultSet.getObject(i, LocalDateTime.class);
                        }
                        catch (SQLException first) {
                            // H2 cannot convert DATE to LocalDateTime in their JDBC driver (even though it can convert to java.sql.Timestamp), we need to do this manually
                            try {
                                timestampValue = Optional.ofNullable(resultSet.getObject(i, LocalDate.class)).map(LocalDate::atStartOfDay).orElse(null);
                            }
                            catch (RuntimeException e) {
                                first.addSuppressed(e);
                                throw first;
                            }
                        }
                        if (resultSet.wasNull()) {
                            row.add(null);
                        }
                        else {
                            row.add(timestampValue);
                        }
                    }
                    else if (TIMESTAMP_WITH_TIME_ZONE.equals(type)) {
                        // H2 supports TIMESTAMP WITH TIME ZONE via org.h2.api.TimestampWithTimeZone, but it represent only a fixed-offset TZ (not named)
                        // This means H2 is unsuitable for testing TIMESTAMP WITH TIME ZONE-bearing queries. Those need to be tested manually.
                        throw new UnsupportedOperationException();
                    }
                    else if (UNKNOWN.equals(type)) {
                        Object objectValue = resultSet.getObject(i);
                        checkState(resultSet.wasNull(), "Expected a null value, but got %s", objectValue);
                        row.add(null);
                    }
                    else if (type instanceof DecimalType) {
                        DecimalType decimalType = (DecimalType) type;
                        BigDecimal decimalValue = resultSet.getBigDecimal(i);
                        if (resultSet.wasNull()) {
                            row.add(null);
                        }
                        else {
                            row.add(decimalValue
                                    .setScale(decimalType.getScale(), BigDecimal.ROUND_HALF_UP)
                                    .round(new MathContext(decimalType.getPrecision())));
                        }
                    }
                    else if (type instanceof ArrayType) {
                        Array array = resultSet.getArray(i);
                        if (resultSet.wasNull()) {
                            row.add(null);
                        }
                        else {
                            row.add(newArrayList(mapArrayValues(((ArrayType) type), (Object[]) array.getArray())));
                        }
                    }
                    else if (type instanceof RowType) {
                        Array array = resultSet.getArray(i);
                        if (resultSet.wasNull()) {
                            row.add(null);
                        }
                        else {
                            row.add(newArrayList(mapRowValues((RowType) type, (Object[]) array.getArray())));
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

    private static Object[] mapArrayValues(ArrayType arrayType, Object[] values)
    {
        Type elementType = arrayType.getElementType();
        if (elementType instanceof ArrayType) {
            return Arrays.stream(values)
                    .map(v -> v == null ? null : newArrayList((Object[]) v))
                    .toArray();
        }

        if (elementType instanceof RowType) {
            RowType rowType = (RowType) elementType;
            return Arrays.stream(values)
                    .map(v -> v == null ? null : newArrayList(mapRowValues(rowType, (Object[]) v)))
                    .toArray();
        }

        if (elementType instanceof CharType) {
            int length = ((CharType) elementType).getLength();
            return Arrays.stream(values)
                    .map(String.class::cast)
                    .map(v -> v == null ? null : padEnd(v, length, ' '))
                    .toArray();
        }

        if (elementType instanceof TimestampType) {
            return Arrays.stream(values)
                    .map(v -> v == null ? null : ((Timestamp) v).toLocalDateTime())
                    .toArray();
        }

        return values;
    }

    private static Object[] mapRowValues(RowType rowType, Object[] values)
    {
        int fieldCount = rowType.getFields().size();
        Object[] fields = new Object[fieldCount];
        for (int j = 0; j < fieldCount; j++) {
            Type fieldType = rowType.getTypeParameters().get(j);
            if (fieldType instanceof RowType) {
                fields[j] = newArrayList(mapRowValues((RowType) fieldType, (Object[]) values[j]));
            }
            else {
                fields[j] = values[j];
            }
        }
        return fields;
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

    /**
     * Pass-through SQL parser that does not support named parameters or definitions.
     * This allows queries such as {@code x<y} that do not work with the default parser.
     */
    private static class RawSqlParser
            implements SqlParser
    {
        @Override
        public ParsedSql parse(String sql, StatementContext ctx)
        {
            return ParsedSql.builder().append(sql).build();
        }

        @Override
        public String nameParameter(String rawName, StatementContext ctx)
        {
            throw new UnsupportedOperationException();
        }
    }
}
