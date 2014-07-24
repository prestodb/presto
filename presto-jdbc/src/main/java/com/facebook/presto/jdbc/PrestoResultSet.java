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
package com.facebook.presto.jdbc;

import com.facebook.presto.client.Column;
import com.facebook.presto.client.QueryError;
import com.facebook.presto.client.QueryResults;
import com.facebook.presto.client.StatementClient;
import com.google.common.base.Function;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.joda.time.DateTimeZone;
import org.joda.time.Period;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.DateTimeFormatterBuilder;
import org.joda.time.format.DateTimeParser;
import org.joda.time.format.ISODateTimeFormat;
import org.joda.time.format.PeriodFormatter;
import org.joda.time.format.PeriodFormatterBuilder;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.NClob;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Throwables.propagate;
import static com.google.common.base.Throwables.propagateIfInstanceOf;
import static com.google.common.collect.Iterators.concat;
import static com.google.common.collect.Iterators.transform;
import static java.lang.String.format;

public class PrestoResultSet
        implements ResultSet
{
    private static final DateTimeFormatter DATE_FORMATTER = ISODateTimeFormat.date();
    private static final DateTimeFormatter TIME_FORMATTER = DateTimeFormat.forPattern("HH:mm:ss.SSS");
    private static final DateTimeFormatter TIME_WITH_TIME_ZONE_FORMATTER = new DateTimeFormatterBuilder()
            .append(DateTimeFormat.forPattern("HH:mm:ss.SSS ZZZ").getPrinter(),
                    new DateTimeParser[] {
                            DateTimeFormat.forPattern("HH:mm:ss.SSS Z").getParser(),
                            DateTimeFormat.forPattern("HH:mm:ss.SSS ZZZ").getParser(),
                    }
            )
            .toFormatter()
            .withOffsetParsed();

    private static final DateTimeFormatter TIMESTAMP_FORMATTER = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss.SSS");
    private static final DateTimeFormatter TIMESTAMP_WITH_TIME_ZONE_FORMATTER  = new DateTimeFormatterBuilder()
            .append(DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss.SSS ZZZ").getPrinter(),
                    new DateTimeParser[] {
                            DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss.SSS Z").getParser(),
                            DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss.SSS ZZZ").getParser(),
                    }
            )
            .toFormatter()
            .withOffsetParsed();

    private static final PeriodFormatter INTERVAL_YEAR_TO_MONTH_FORMATTER = new PeriodFormatterBuilder()
            .appendYears()
            .appendLiteral("-")
            .appendMonths()
            .toFormatter();

    private static final PeriodFormatter INTERVAL_DAY_TO_SECOND_FORMATTER = new PeriodFormatterBuilder()
            .appendDays()
            .appendLiteral(" ")
            .appendHours()
            .appendLiteral(":")
            .appendMinutes()
            .appendLiteral(":")
            .appendSecondsWithOptionalMillis()
            .toFormatter();

    private static final int YEAR_FIELD = 0;
    private static final int MONTH_FIELD = 1;
    private static final int DAY_FIELD = 3;
    private static final int HOUR_FIELD = 4;
    private static final int MINUTE_FIELD = 5;
    private static final int SECOND_FIELD = 6;
    private static final int MILLIS_FIELD = 7;

    private static final int VARCHAR_MAX = 1024 * 1024 * 1024;
    private static final int VARBINARY_MAX = 1024 * 1024 * 1024;
    private static final int TIME_ZONE_MAX = 40; // current longest time zone is 32
    private static final int TIME_MAX = "HH:mm:ss.SSS".length();
    private static final int TIME_WITH_TIME_ZONE_MAX = TIME_MAX + TIME_ZONE_MAX;
    private static final int TIMESTAMP_MAX = "yyyy-MM-dd HH:mm:ss.SSS".length();
    private static final int TIMESTAMP_WITH_TIME_ZONE_MAX = TIMESTAMP_MAX + TIME_ZONE_MAX;
    private static final int DATE_MAX = "yyyy-MM-dd".length();

    private final StatementClient client;
    private final DateTimeZone sessionTimeZone;
    private final Iterator<List<Object>> results;
    private final Map<String, Integer> fieldMap;
    private final List<ColumnInfo> columnInfoList;
    private final ResultSetMetaData resultSetMetaData;
    private final AtomicReference<List<Object>> row = new AtomicReference<>();
    private final AtomicBoolean wasNull = new AtomicBoolean();

    PrestoResultSet(StatementClient client)
            throws SQLException
    {
        this.client = checkNotNull(client, "client is null");
        this.sessionTimeZone = DateTimeZone.forID(client.getTimeZoneId());

        List<Column> columns = getColumns(client);
        this.fieldMap = getFieldMap(columns);
        this.columnInfoList = getColumnInfo(columns);
        this.resultSetMetaData = new PrestoResultSetMetaData(columnInfoList);

        this.results = flatten(new ResultsPageIterator(client));
    }

    @Override
    public boolean next()
            throws SQLException
    {
        checkOpen();
        try {
            if (!results.hasNext()) {
                row.set(null);
                return false;
            }
            row.set(results.next());
            return true;
        }
        catch (RuntimeException e) {
            propagateIfInstanceOf(e, SQLException.class);
            throw new SQLException("Error fetching results", e);
        }
    }

    @Override
    public void close()
            throws SQLException
    {
        client.close();
    }

    @Override
    public boolean wasNull()
            throws SQLException
    {
        return wasNull.get();
    }

    @Override
    public String getString(int columnIndex)
            throws SQLException
    {
        Object value = column(columnIndex);
        return (value != null) ? value.toString() : null;
    }

    @Override
    public boolean getBoolean(int columnIndex)
            throws SQLException
    {
        Object value = column(columnIndex);
        return (value != null) ? (Boolean) value : false;
    }

    @Override
    public byte getByte(int columnIndex)
            throws SQLException
    {
        Object value = column(columnIndex);
        return (value != null) ? ((Number) value).byteValue() : 0;
    }

    @Override
    public short getShort(int columnIndex)
            throws SQLException
    {
        Object value = column(columnIndex);
        return (value != null) ? ((Number) value).shortValue() : 0;
    }

    @Override
    public int getInt(int columnIndex)
            throws SQLException
    {
        Object value = column(columnIndex);
        return (value != null) ? ((Number) value).intValue() : 0;
    }

    @Override
    public long getLong(int columnIndex)
            throws SQLException
    {
        Object value = column(columnIndex);
        return (value != null) ? ((Number) value).longValue() : 0;
    }

    @Override
    public float getFloat(int columnIndex)
            throws SQLException
    {
        Object value = column(columnIndex);
        return (value != null) ? ((Number) value).floatValue() : 0;
    }

    @Override
    public double getDouble(int columnIndex)
            throws SQLException
    {
        Object value = column(columnIndex);
        return (value != null) ? ((Number) value).doubleValue() : 0;
    }

    @Override
    public BigDecimal getBigDecimal(int columnIndex, int scale)
            throws SQLException
    {
        throw new UnsupportedOperationException("getBigDecimal");
    }

    @Override
    public byte[] getBytes(int columnIndex)
            throws SQLException
    {
        return (byte[]) column(columnIndex);
    }

    @Override
    public Date getDate(int columnIndex)
            throws SQLException
    {
        return getDate(columnIndex, sessionTimeZone);
    }

    private Date getDate(int columnIndex, DateTimeZone localTimeZone)
            throws SQLException
    {
        Object value = column(columnIndex);
        if (value == null) {
            return null;
        }

        try {
            return new Date(DATE_FORMATTER.withZone(localTimeZone).parseMillis(String.valueOf(value)));
        }
        catch (IllegalArgumentException e) {
            throw new SQLException("Invalid date from server: " + value, e);
        }
    }

    @Override
    public Time getTime(int columnIndex)
            throws SQLException
    {
        return getTime(columnIndex, sessionTimeZone);
    }

    private Time getTime(int columnIndex, DateTimeZone localTimeZone)
            throws SQLException
    {
        Object value = column(columnIndex);
        if (value == null) {
            return null;
        }

        ColumnInfo columnInfo = columnInfoList.get(columnIndex - 1);
        if (columnInfo.getColumnTypeName().equalsIgnoreCase("time")) {
            try {
                return new Time(TIME_FORMATTER.withZone(localTimeZone).parseMillis(String.valueOf(value)));
            }
            catch (IllegalArgumentException e) {
                throw new SQLException("Invalid time from server: " + value, e);
            }
        }

        if (columnInfo.getColumnTypeName().equalsIgnoreCase("time with time zone")) {
            try {
                return new Time(TIME_WITH_TIME_ZONE_FORMATTER.parseMillis(String.valueOf(value)));
            }
            catch (IllegalArgumentException e) {
                throw new SQLException("Invalid time from server: " + value, e);
            }
        }

        throw new IllegalArgumentException("Expected column to be a time type but is " + columnInfo.getColumnTypeName());
    }

    @Override
    public Timestamp getTimestamp(int columnIndex)
            throws SQLException
    {
        return getTimestamp(columnIndex, sessionTimeZone);
    }

    public Timestamp getTimestamp(int columnIndex, DateTimeZone localTimeZone)
            throws SQLException
    {
        Object value = column(columnIndex);
        if (value == null) {
            return null;
        }

        ColumnInfo columnInfo = columnInfoList.get(columnIndex - 1);
        if (columnInfo.getColumnTypeName().equalsIgnoreCase("timestamp")) {
            try {
                return new Timestamp(TIMESTAMP_FORMATTER.withZone(localTimeZone).parseMillis(String.valueOf(value)));
            }
            catch (IllegalArgumentException e) {
                throw new SQLException("Invalid timestamp from server: " + value, e);
            }
        }

        if (columnInfo.getColumnTypeName().equalsIgnoreCase("timestamp with time zone")) {
            try {
                return new Timestamp(TIMESTAMP_WITH_TIME_ZONE_FORMATTER.parseMillis(String.valueOf(value)));
            }
            catch (IllegalArgumentException e) {
                throw new SQLException("Invalid timestamp from server: " + value, e);
            }
        }

        throw new IllegalArgumentException("Expected column to be a timestamp type but is " + columnInfo.getColumnTypeName());
    }

    @Override
    public InputStream getAsciiStream(int columnIndex)
            throws SQLException
    {
        throw new UnsupportedOperationException("getAsciiStream");
    }

    @Override
    public InputStream getUnicodeStream(int columnIndex)
            throws SQLException
    {
        throw new UnsupportedOperationException("getUnicodeStream");
    }

    @Override
    public InputStream getBinaryStream(int columnIndex)
            throws SQLException
    {
        throw new UnsupportedOperationException("getBinaryStream");
    }

    @Override
    public String getString(String columnLabel)
            throws SQLException
    {
        Object value = column(columnLabel);
        return (value != null) ? value.toString() : null;
    }

    @Override
    public boolean getBoolean(String columnLabel)
            throws SQLException
    {
        Object value = column(columnLabel);
        return (value != null) ? (Boolean) value : false;
    }

    @Override
    public byte getByte(String columnLabel)
            throws SQLException
    {
        Object value = column(columnLabel);
        return (value != null) ? ((Number) value).byteValue() : 0;
    }

    @Override
    public short getShort(String columnLabel)
            throws SQLException
    {
        Object value = column(columnLabel);
        return (value != null) ? ((Number) value).shortValue() : 0;
    }

    @Override
    public int getInt(String columnLabel)
            throws SQLException
    {
        Object value = column(columnLabel);
        return (value != null) ? ((Number) value).intValue() : 0;
    }

    @Override
    public long getLong(String columnLabel)
            throws SQLException
    {
        Object value = column(columnLabel);
        return (value != null) ? ((Number) value).longValue() : 0;
    }

    @Override
    public float getFloat(String columnLabel)
            throws SQLException
    {
        Object value = column(columnLabel);
        return (value != null) ? ((Number) value).floatValue() : 0;
    }

    @Override
    public double getDouble(String columnLabel)
            throws SQLException
    {
        Object value = column(columnLabel);
        return (value != null) ? ((Number) value).doubleValue() : 0;
    }

    @Override
    public BigDecimal getBigDecimal(String columnLabel, int scale)
            throws SQLException
    {
        throw new UnsupportedOperationException("getBigDecimal");
    }

    @Override
    public byte[] getBytes(String columnLabel)
            throws SQLException
    {
        return (byte[]) column(columnLabel);
    }

    @Override
    public Date getDate(String columnLabel)
            throws SQLException
    {
        return getDate(columnIndex(columnLabel));
    }

    @Override
    public Time getTime(String columnLabel)
            throws SQLException
    {
        return getTime(columnIndex(columnLabel));
    }

    @Override
    public Timestamp getTimestamp(String columnLabel)
            throws SQLException
    {
        return getTimestamp(columnIndex(columnLabel));
    }

    @Override
    public InputStream getAsciiStream(String columnLabel)
            throws SQLException
    {
        throw new UnsupportedOperationException("getAsciiStream");
    }

    @Override
    public InputStream getUnicodeStream(String columnLabel)
            throws SQLException
    {
        throw new UnsupportedOperationException("getUnicodeStream");
    }

    @Override
    public InputStream getBinaryStream(String columnLabel)
            throws SQLException
    {
        throw new UnsupportedOperationException("getBinaryStream");
    }

    @Override
    public SQLWarning getWarnings()
            throws SQLException
    {
        checkOpen();
        return null;
    }

    @Override
    public void clearWarnings()
            throws SQLException
    {
        checkOpen();
    }

    @Override
    public String getCursorName()
            throws SQLException
    {
        throw new UnsupportedOperationException("getCursorName");
    }

    @Override
    public ResultSetMetaData getMetaData()
            throws SQLException
    {
        return resultSetMetaData;
    }

    @Override
    public Object getObject(int columnIndex)
            throws SQLException
    {
        ColumnInfo columnInfo = columnInfoList.get(columnIndex - 1);
        switch (columnInfo.getColumnType()) {
            case Types.DATE:
                return getDate(columnIndex);
            case Types.TIME:
                return getTime(columnIndex);
            case Types.TIMESTAMP:
                return getTimestamp(columnIndex);
            case Types.JAVA_OBJECT:
                if (columnInfo.getColumnTypeName().equalsIgnoreCase("interval year to month")) {
                    return getIntervalYearMonth(columnIndex);
                }
                else if (columnInfo.getColumnTypeName().equalsIgnoreCase("interval day to second")) {
                    return getIntervalDayTime(columnIndex);
                }
        }
        return column(columnIndex);
    }

    private PrestoIntervalYearMonth getIntervalYearMonth(int columnIndex)
            throws SQLException
    {
        Object value = column(columnIndex);
        if (value == null) {
            return null;
        }

        Period period = INTERVAL_YEAR_TO_MONTH_FORMATTER.parsePeriod(String.valueOf(value));
        return new PrestoIntervalYearMonth(
                period.getValue(YEAR_FIELD),
                period.getValue(MONTH_FIELD));
    }

    private PrestoIntervalDayTime getIntervalDayTime(int columnIndex)
            throws SQLException
    {
        Object value = column(columnIndex);
        if (value == null) {
            return null;
        }

        Period period = INTERVAL_DAY_TO_SECOND_FORMATTER.parsePeriod(String.valueOf(value));
        return new PrestoIntervalDayTime(
                period.getValue(DAY_FIELD),
                period.getValue(HOUR_FIELD),
                period.getValue(MINUTE_FIELD),
                period.getValue(SECOND_FIELD),
                period.getValue(MILLIS_FIELD));
    }

    @Override
    public Object getObject(String columnLabel)
            throws SQLException
    {
        return getObject(columnIndex(columnLabel));
    }

    @Override
    public int findColumn(String columnLabel)
            throws SQLException
    {
        checkOpen();
        return columnIndex(columnLabel);
    }

    @Override
    public Reader getCharacterStream(int columnIndex)
            throws SQLException
    {
        throw new UnsupportedOperationException("getCharacterStream");
    }

    @Override
    public Reader getCharacterStream(String columnLabel)
            throws SQLException
    {
        throw new UnsupportedOperationException("getCharacterStream");
    }

    @Override
    public BigDecimal getBigDecimal(int columnIndex)
            throws SQLException
    {
        throw new UnsupportedOperationException("getBigDecimal");
    }

    @Override
    public BigDecimal getBigDecimal(String columnLabel)
            throws SQLException
    {
        throw new UnsupportedOperationException("getBigDecimal");
    }

    @Override
    public boolean isBeforeFirst()
            throws SQLException
    {
        throw new UnsupportedOperationException("isBeforeFirst");
    }

    @Override
    public boolean isAfterLast()
            throws SQLException
    {
        throw new UnsupportedOperationException("isAfterLast");
    }

    @Override
    public boolean isFirst()
            throws SQLException
    {
        throw new UnsupportedOperationException("isFirst");
    }

    @Override
    public boolean isLast()
            throws SQLException
    {
        throw new UnsupportedOperationException("isLast");
    }

    @Override
    public void beforeFirst()
            throws SQLException
    {
        throw new UnsupportedOperationException("beforeFirst");
    }

    @Override
    public void afterLast()
            throws SQLException
    {
        throw new UnsupportedOperationException("afterLast");
    }

    @Override
    public boolean first()
            throws SQLException
    {
        throw new UnsupportedOperationException("first");
    }

    @Override
    public boolean last()
            throws SQLException
    {
        throw new UnsupportedOperationException("last");
    }

    @Override
    public int getRow()
            throws SQLException
    {
        throw new UnsupportedOperationException("getRow");
    }

    @Override
    public boolean absolute(int row)
            throws SQLException
    {
        throw new UnsupportedOperationException("absolute");
    }

    @Override
    public boolean relative(int rows)
            throws SQLException
    {
        throw new UnsupportedOperationException("relative");
    }

    @Override
    public boolean previous()
            throws SQLException
    {
        throw new UnsupportedOperationException("previous");
    }

    @Override
    public void setFetchDirection(int direction)
            throws SQLException
    {
        throw new UnsupportedOperationException("setFetchDirection");
    }

    @Override
    public int getFetchDirection()
            throws SQLException
    {
        throw new UnsupportedOperationException("getFetchDirection");
    }

    @Override
    public void setFetchSize(int rows)
            throws SQLException
    {
        throw new UnsupportedOperationException("setFetchSize");
    }

    @Override
    public int getFetchSize()
            throws SQLException
    {
        throw new UnsupportedOperationException("getFetchSize");
    }

    @Override
    public int getType()
            throws SQLException
    {
        throw new UnsupportedOperationException("getType");
    }

    @Override
    public int getConcurrency()
            throws SQLException
    {
        throw new UnsupportedOperationException("getConcurrency");
    }

    @Override
    public boolean rowUpdated()
            throws SQLException
    {
        throw new UnsupportedOperationException("rowUpdated");
    }

    @Override
    public boolean rowInserted()
            throws SQLException
    {
        throw new UnsupportedOperationException("rowInserted");
    }

    @Override
    public boolean rowDeleted()
            throws SQLException
    {
        throw new UnsupportedOperationException("rowDeleted");
    }

    @Override
    public void updateNull(int columnIndex)
            throws SQLException
    {
        throw new UnsupportedOperationException("updateNull");
    }

    @Override
    public void updateBoolean(int columnIndex, boolean x)
            throws SQLException
    {
        throw new UnsupportedOperationException("updateBoolean");
    }

    @Override
    public void updateByte(int columnIndex, byte x)
            throws SQLException
    {
        throw new UnsupportedOperationException("updateByte");
    }

    @Override
    public void updateShort(int columnIndex, short x)
            throws SQLException
    {
        throw new UnsupportedOperationException("updateShort");
    }

    @Override
    public void updateInt(int columnIndex, int x)
            throws SQLException
    {
        throw new UnsupportedOperationException("updateInt");
    }

    @Override
    public void updateLong(int columnIndex, long x)
            throws SQLException
    {
        throw new UnsupportedOperationException("updateLong");
    }

    @Override
    public void updateFloat(int columnIndex, float x)
            throws SQLException
    {
        throw new UnsupportedOperationException("updateFloat");
    }

    @Override
    public void updateDouble(int columnIndex, double x)
            throws SQLException
    {
        throw new UnsupportedOperationException("updateDouble");
    }

    @Override
    public void updateBigDecimal(int columnIndex, BigDecimal x)
            throws SQLException
    {
        throw new UnsupportedOperationException("updateBigDecimal");
    }

    @Override
    public void updateString(int columnIndex, String x)
            throws SQLException
    {
        throw new UnsupportedOperationException("updateString");
    }

    @Override
    public void updateBytes(int columnIndex, byte[] x)
            throws SQLException
    {
        throw new UnsupportedOperationException("updateBytes");
    }

    @Override
    public void updateDate(int columnIndex, Date x)
            throws SQLException
    {
        throw new UnsupportedOperationException("updateDate");
    }

    @Override
    public void updateTime(int columnIndex, Time x)
            throws SQLException
    {
        throw new UnsupportedOperationException("updateTime");
    }

    @Override
    public void updateTimestamp(int columnIndex, Timestamp x)
            throws SQLException
    {
        throw new UnsupportedOperationException("updateTimestamp");
    }

    @Override
    public void updateAsciiStream(int columnIndex, InputStream x, int length)
            throws SQLException
    {
        throw new UnsupportedOperationException("updateAsciiStream");
    }

    @Override
    public void updateBinaryStream(int columnIndex, InputStream x, int length)
            throws SQLException
    {
        throw new UnsupportedOperationException("updateBinaryStream");
    }

    @Override
    public void updateCharacterStream(int columnIndex, Reader x, int length)
            throws SQLException
    {
        throw new UnsupportedOperationException("updateCharacterStream");
    }

    @Override
    public void updateObject(int columnIndex, Object x, int scaleOrLength)
            throws SQLException
    {
        throw new UnsupportedOperationException("updateObject");
    }

    @Override
    public void updateObject(int columnIndex, Object x)
            throws SQLException
    {
        throw new UnsupportedOperationException("updateObject");
    }

    @Override
    public void updateNull(String columnLabel)
            throws SQLException
    {
        throw new UnsupportedOperationException("updateNull");
    }

    @Override
    public void updateBoolean(String columnLabel, boolean x)
            throws SQLException
    {
        throw new UnsupportedOperationException("updateBoolean");
    }

    @Override
    public void updateByte(String columnLabel, byte x)
            throws SQLException
    {
        throw new UnsupportedOperationException("updateByte");
    }

    @Override
    public void updateShort(String columnLabel, short x)
            throws SQLException
    {
        throw new UnsupportedOperationException("updateShort");
    }

    @Override
    public void updateInt(String columnLabel, int x)
            throws SQLException
    {
        throw new UnsupportedOperationException("updateInt");
    }

    @Override
    public void updateLong(String columnLabel, long x)
            throws SQLException
    {
        throw new UnsupportedOperationException("updateLong");
    }

    @Override
    public void updateFloat(String columnLabel, float x)
            throws SQLException
    {
        throw new UnsupportedOperationException("updateFloat");
    }

    @Override
    public void updateDouble(String columnLabel, double x)
            throws SQLException
    {
        throw new UnsupportedOperationException("updateDouble");
    }

    @Override
    public void updateBigDecimal(String columnLabel, BigDecimal x)
            throws SQLException
    {
        throw new UnsupportedOperationException("updateBigDecimal");
    }

    @Override
    public void updateString(String columnLabel, String x)
            throws SQLException
    {
        throw new UnsupportedOperationException("updateString");
    }

    @Override
    public void updateBytes(String columnLabel, byte[] x)
            throws SQLException
    {
        throw new UnsupportedOperationException("updateBytes");
    }

    @Override
    public void updateDate(String columnLabel, Date x)
            throws SQLException
    {
        throw new UnsupportedOperationException("updateDate");
    }

    @Override
    public void updateTime(String columnLabel, Time x)
            throws SQLException
    {
        throw new UnsupportedOperationException("updateTime");
    }

    @Override
    public void updateTimestamp(String columnLabel, Timestamp x)
            throws SQLException
    {
        throw new UnsupportedOperationException("updateTimestamp");
    }

    @Override
    public void updateAsciiStream(String columnLabel, InputStream x, int length)
            throws SQLException
    {
        throw new UnsupportedOperationException("updateAsciiStream");
    }

    @Override
    public void updateBinaryStream(String columnLabel, InputStream x, int length)
            throws SQLException
    {
        throw new UnsupportedOperationException("updateBinaryStream");
    }

    @Override
    public void updateCharacterStream(String columnLabel, Reader reader, int length)
            throws SQLException
    {
        throw new UnsupportedOperationException("updateCharacterStream");
    }

    @Override
    public void updateObject(String columnLabel, Object x, int scaleOrLength)
            throws SQLException
    {
        throw new UnsupportedOperationException("updateObject");
    }

    @Override
    public void updateObject(String columnLabel, Object x)
            throws SQLException
    {
        throw new UnsupportedOperationException("updateObject");
    }

    @Override
    public void insertRow()
            throws SQLException
    {
        throw new UnsupportedOperationException("insertRow");
    }

    @Override
    public void updateRow()
            throws SQLException
    {
        throw new UnsupportedOperationException("updateRow");
    }

    @Override
    public void deleteRow()
            throws SQLException
    {
        throw new UnsupportedOperationException("deleteRow");
    }

    @Override
    public void refreshRow()
            throws SQLException
    {
        throw new UnsupportedOperationException("refreshRow");
    }

    @Override
    public void cancelRowUpdates()
            throws SQLException
    {
        throw new UnsupportedOperationException("cancelRowUpdates");
    }

    @Override
    public void moveToInsertRow()
            throws SQLException
    {
        throw new UnsupportedOperationException("moveToInsertRow");
    }

    @Override
    public void moveToCurrentRow()
            throws SQLException
    {
        throw new UnsupportedOperationException("moveToCurrentRow");
    }

    @Override
    public Statement getStatement()
            throws SQLException
    {
        throw new UnsupportedOperationException("getStatement");
    }

    @Override
    public Object getObject(int columnIndex, Map<String, Class<?>> map)
            throws SQLException
    {
        throw new UnsupportedOperationException("getObject");
    }

    @Override
    public Ref getRef(int columnIndex)
            throws SQLException
    {
        throw new UnsupportedOperationException("getRef");
    }

    @Override
    public Blob getBlob(int columnIndex)
            throws SQLException
    {
        throw new UnsupportedOperationException("getBlob");
    }

    @Override
    public Clob getClob(int columnIndex)
            throws SQLException
    {
        throw new UnsupportedOperationException("getClob");
    }

    @Override
    public Array getArray(int columnIndex)
            throws SQLException
    {
        throw new UnsupportedOperationException("getArray");
    }

    @Override
    public Object getObject(String columnLabel, Map<String, Class<?>> map)
            throws SQLException
    {
        throw new UnsupportedOperationException("getObject");
    }

    @Override
    public Ref getRef(String columnLabel)
            throws SQLException
    {
        throw new UnsupportedOperationException("getRef");
    }

    @Override
    public Blob getBlob(String columnLabel)
            throws SQLException
    {
        throw new UnsupportedOperationException("getBlob");
    }

    @Override
    public Clob getClob(String columnLabel)
            throws SQLException
    {
        throw new UnsupportedOperationException("getClob");
    }

    @Override
    public Array getArray(String columnLabel)
            throws SQLException
    {
        throw new UnsupportedOperationException("getArray");
    }

    @Override
    public Date getDate(int columnIndex, Calendar cal)
            throws SQLException
    {
        return getDate(columnIndex, DateTimeZone.forTimeZone(cal.getTimeZone()));
    }

    @Override
    public Date getDate(String columnLabel, Calendar cal)
            throws SQLException
    {
        return getDate(columnIndex(columnLabel), cal);
    }

    @Override
    public Time getTime(int columnIndex, Calendar cal)
            throws SQLException
    {
        return getTime(columnIndex, DateTimeZone.forTimeZone(cal.getTimeZone()));
    }

    @Override
    public Time getTime(String columnLabel, Calendar cal)
            throws SQLException
    {
        return getTime(columnIndex(columnLabel), cal);
    }

    @Override
    public Timestamp getTimestamp(int columnIndex, Calendar cal)
            throws SQLException
    {
        return getTimestamp(columnIndex, DateTimeZone.forTimeZone(cal.getTimeZone()));
    }

    @Override
    public Timestamp getTimestamp(String columnLabel, Calendar cal)
            throws SQLException
    {
        return getTimestamp(columnIndex(columnLabel), cal);
    }

    @Override
    public URL getURL(int columnIndex)
            throws SQLException
    {
        throw new UnsupportedOperationException("getURL");
    }

    @Override
    public URL getURL(String columnLabel)
            throws SQLException
    {
        throw new UnsupportedOperationException("getURL");
    }

    @Override
    public void updateRef(int columnIndex, Ref x)
            throws SQLException
    {
        throw new UnsupportedOperationException("updateRef");
    }

    @Override
    public void updateRef(String columnLabel, Ref x)
            throws SQLException
    {
        throw new UnsupportedOperationException("updateRef");
    }

    @Override
    public void updateBlob(int columnIndex, Blob x)
            throws SQLException
    {
        throw new UnsupportedOperationException("updateBlob");
    }

    @Override
    public void updateBlob(String columnLabel, Blob x)
            throws SQLException
    {
        throw new UnsupportedOperationException("updateBlob");
    }

    @Override
    public void updateClob(int columnIndex, Clob x)
            throws SQLException
    {
        throw new UnsupportedOperationException("updateClob");
    }

    @Override
    public void updateClob(String columnLabel, Clob x)
            throws SQLException
    {
        throw new UnsupportedOperationException("updateClob");
    }

    @Override
    public void updateArray(int columnIndex, Array x)
            throws SQLException
    {
        throw new UnsupportedOperationException("updateArray");
    }

    @Override
    public void updateArray(String columnLabel, Array x)
            throws SQLException
    {
        throw new UnsupportedOperationException("updateArray");
    }

    @Override
    public RowId getRowId(int columnIndex)
            throws SQLException
    {
        throw new UnsupportedOperationException("getRowId");
    }

    @Override
    public RowId getRowId(String columnLabel)
            throws SQLException
    {
        throw new UnsupportedOperationException("getRowId");
    }

    @Override
    public void updateRowId(int columnIndex, RowId x)
            throws SQLException
    {
        throw new UnsupportedOperationException("updateRowId");
    }

    @Override
    public void updateRowId(String columnLabel, RowId x)
            throws SQLException
    {
        throw new UnsupportedOperationException("updateRowId");
    }

    @Override
    public int getHoldability()
            throws SQLException
    {
        return ResultSet.HOLD_CURSORS_OVER_COMMIT;
    }

    @Override
    public boolean isClosed()
            throws SQLException
    {
        return client.isClosed();
    }

    @Override
    public void updateNString(int columnIndex, String nString)
            throws SQLException
    {
        throw new UnsupportedOperationException("updateNString");
    }

    @Override
    public void updateNString(String columnLabel, String nString)
            throws SQLException
    {
        throw new UnsupportedOperationException("updateNString");
    }

    @Override
    public void updateNClob(int columnIndex, NClob nClob)
            throws SQLException
    {
        throw new UnsupportedOperationException("updateNClob");
    }

    @Override
    public void updateNClob(String columnLabel, NClob nClob)
            throws SQLException
    {
        throw new UnsupportedOperationException("updateNClob");
    }

    @Override
    public NClob getNClob(int columnIndex)
            throws SQLException
    {
        throw new UnsupportedOperationException("getNClob");
    }

    @Override
    public NClob getNClob(String columnLabel)
            throws SQLException
    {
        throw new UnsupportedOperationException("getNClob");
    }

    @Override
    public SQLXML getSQLXML(int columnIndex)
            throws SQLException
    {
        throw new UnsupportedOperationException("getSQLXML");
    }

    @Override
    public SQLXML getSQLXML(String columnLabel)
            throws SQLException
    {
        throw new UnsupportedOperationException("getSQLXML");
    }

    @Override
    public void updateSQLXML(int columnIndex, SQLXML xmlObject)
            throws SQLException
    {
        throw new UnsupportedOperationException("updateSQLXML");
    }

    @Override
    public void updateSQLXML(String columnLabel, SQLXML xmlObject)
            throws SQLException
    {
        throw new UnsupportedOperationException("updateSQLXML");
    }

    @Override
    public String getNString(int columnIndex)
            throws SQLException
    {
        throw new UnsupportedOperationException("getNString");
    }

    @Override
    public String getNString(String columnLabel)
            throws SQLException
    {
        throw new UnsupportedOperationException("getNString");
    }

    @Override
    public Reader getNCharacterStream(int columnIndex)
            throws SQLException
    {
        throw new UnsupportedOperationException("getNCharacterStream");
    }

    @Override
    public Reader getNCharacterStream(String columnLabel)
            throws SQLException
    {
        throw new UnsupportedOperationException("getNCharacterStream");
    }

    @Override
    public void updateNCharacterStream(int columnIndex, Reader x, long length)
            throws SQLException
    {
        throw new UnsupportedOperationException("updateNCharacterStream");
    }

    @Override
    public void updateNCharacterStream(String columnLabel, Reader reader, long length)
            throws SQLException
    {
        throw new UnsupportedOperationException("updateNCharacterStream");
    }

    @Override
    public void updateAsciiStream(int columnIndex, InputStream x, long length)
            throws SQLException
    {
        throw new UnsupportedOperationException("updateAsciiStream");
    }

    @Override
    public void updateBinaryStream(int columnIndex, InputStream x, long length)
            throws SQLException
    {
        throw new UnsupportedOperationException("updateBinaryStream");
    }

    @Override
    public void updateCharacterStream(int columnIndex, Reader x, long length)
            throws SQLException
    {
        throw new UnsupportedOperationException("updateCharacterStream");
    }

    @Override
    public void updateAsciiStream(String columnLabel, InputStream x, long length)
            throws SQLException
    {
        throw new UnsupportedOperationException("updateAsciiStream");
    }

    @Override
    public void updateBinaryStream(String columnLabel, InputStream x, long length)
            throws SQLException
    {
        throw new UnsupportedOperationException("updateBinaryStream");
    }

    @Override
    public void updateCharacterStream(String columnLabel, Reader reader, long length)
            throws SQLException
    {
        throw new UnsupportedOperationException("updateCharacterStream");
    }

    @Override
    public void updateBlob(int columnIndex, InputStream inputStream, long length)
            throws SQLException
    {
        throw new UnsupportedOperationException("updateBlob");
    }

    @Override
    public void updateBlob(String columnLabel, InputStream inputStream, long length)
            throws SQLException
    {
        throw new UnsupportedOperationException("updateBlob");
    }

    @Override
    public void updateClob(int columnIndex, Reader reader, long length)
            throws SQLException
    {
        throw new UnsupportedOperationException("updateClob");
    }

    @Override
    public void updateClob(String columnLabel, Reader reader, long length)
            throws SQLException
    {
        throw new UnsupportedOperationException("updateClob");
    }

    @Override
    public void updateNClob(int columnIndex, Reader reader, long length)
            throws SQLException
    {
        throw new UnsupportedOperationException("updateNClob");
    }

    @Override
    public void updateNClob(String columnLabel, Reader reader, long length)
            throws SQLException
    {
        throw new UnsupportedOperationException("updateNClob");
    }

    @Override
    public void updateNCharacterStream(int columnIndex, Reader x)
            throws SQLException
    {
        throw new UnsupportedOperationException("updateNCharacterStream");
    }

    @Override
    public void updateNCharacterStream(String columnLabel, Reader reader)
            throws SQLException
    {
        throw new UnsupportedOperationException("updateNCharacterStream");
    }

    @Override
    public void updateAsciiStream(int columnIndex, InputStream x)
            throws SQLException
    {
        throw new UnsupportedOperationException("updateAsciiStream");
    }

    @Override
    public void updateBinaryStream(int columnIndex, InputStream x)
            throws SQLException
    {
        throw new UnsupportedOperationException("updateBinaryStream");
    }

    @Override
    public void updateCharacterStream(int columnIndex, Reader x)
            throws SQLException
    {
        throw new UnsupportedOperationException("updateCharacterStream");
    }

    @Override
    public void updateAsciiStream(String columnLabel, InputStream x)
            throws SQLException
    {
        throw new UnsupportedOperationException("updateAsciiStream");
    }

    @Override
    public void updateBinaryStream(String columnLabel, InputStream x)
            throws SQLException
    {
        throw new UnsupportedOperationException("updateBinaryStream");
    }

    @Override
    public void updateCharacterStream(String columnLabel, Reader reader)
            throws SQLException
    {
        throw new UnsupportedOperationException("updateCharacterStream");
    }

    @Override
    public void updateBlob(int columnIndex, InputStream inputStream)
            throws SQLException
    {
        throw new UnsupportedOperationException("updateBlob");
    }

    @Override
    public void updateBlob(String columnLabel, InputStream inputStream)
            throws SQLException
    {
        throw new UnsupportedOperationException("updateBlob");
    }

    @Override
    public void updateClob(int columnIndex, Reader reader)
            throws SQLException
    {
        throw new UnsupportedOperationException("updateClob");
    }

    @Override
    public void updateClob(String columnLabel, Reader reader)
            throws SQLException
    {
        throw new UnsupportedOperationException("updateClob");
    }

    @Override
    public void updateNClob(int columnIndex, Reader reader)
            throws SQLException
    {
        throw new UnsupportedOperationException("updateNClob");
    }

    @Override
    public void updateNClob(String columnLabel, Reader reader)
            throws SQLException
    {
        throw new UnsupportedOperationException("updateNClob");
    }

    @Override
    public <T> T getObject(int columnIndex, Class<T> type)
            throws SQLException
    {
        throw new UnsupportedOperationException("getObject");
    }

    @Override
    public <T> T getObject(String columnLabel, Class<T> type)
            throws SQLException
    {
        throw new UnsupportedOperationException("getObject");
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T> T unwrap(Class<T> iface)
            throws SQLException
    {
        if (isWrapperFor(iface)) {
            return (T) this;
        }
        throw new SQLException("No wrapper for " + iface);
    }

    @Override
    public boolean isWrapperFor(Class<?> iface)
            throws SQLException
    {
        return iface.isInstance(this);
    }

    private void checkOpen()
            throws SQLException
    {
        if (isClosed()) {
            throw new SQLException("ResultSet is closed");
        }
    }

    private void checkValidRow()
            throws SQLException
    {
        if (row.get() == null) {
            throw new SQLException("Not on a valid row");
        }
    }

    private Object column(int index)
            throws SQLException
    {
        checkOpen();
        checkValidRow();
        if ((index <= 0) || (index > resultSetMetaData.getColumnCount())) {
            throw new SQLException("Invalid column index: " + index);
        }
        return row.get().get(index - 1);
    }

    private Object column(String label)
            throws SQLException
    {
        checkOpen();
        checkValidRow();
        return row.get().get(columnIndex(label) - 1);
    }

    private int columnIndex(String label)
            throws SQLException
    {
        if (label == null) {
            throw new SQLException("Column label is null");
        }
        Integer index = fieldMap.get(label.toLowerCase());
        if (index == null) {
            throw new SQLException("Invalid column label: " + label);
        }
        return index;
    }

    private static List<Column> getColumns(StatementClient client)
            throws SQLException
    {
        while (client.isValid()) {
            List<Column> columns = client.current().getColumns();
            if (columns != null) {
                return columns;
            }
            client.advance();
        }

        QueryResults results = client.finalResults();
        if (!client.isFailed()) {
            throw new SQLException(format("Query has no columns (#%s)", results.getId()));
        }
        throw resultsException(results);
    }

    private static <T> Iterator<T> flatten(Iterator<Iterable<T>> iterator)
    {
        return concat(transform(iterator, new Function<Iterable<T>, Iterator<T>>()
        {
            @Override
            public Iterator<T> apply(Iterable<T> input)
            {
                return input.iterator();
            }
        }));
    }

    private static class ResultsPageIterator
            extends AbstractIterator<Iterable<List<Object>>>
    {
        private final StatementClient client;

        private ResultsPageIterator(StatementClient client)
        {
            this.client = checkNotNull(client, "client is null");
        }

        @Override
        protected Iterable<List<Object>> computeNext()
        {
            while (client.isValid()) {
                Iterable<List<Object>> data = client.current().getData();
                client.advance();
                if (data != null) {
                    return data;
                }
            }

            if (client.isFailed()) {
                throw propagate(resultsException(client.finalResults()));
            }

            return endOfData();
        }
    }

    private static SQLException resultsException(QueryResults results)
    {
        QueryError error = results.getError();
        String message = format("Query failed (#%s): %s", results.getId(), error.getMessage());
        Throwable cause = (error.getFailureInfo() == null) ? null : error.getFailureInfo().toException();
        return new SQLException(message, error.getSqlState(), error.getErrorCode(), cause);
    }

    private static Map<String, Integer> getFieldMap(List<Column> columns)
    {
        Map<String, Integer> map = new HashMap<>();
        for (int i = 0; i < columns.size(); i++) {
            String name = columns.get(i).getName().toLowerCase();
            if (!map.containsKey(name)) {
                map.put(name, i + 1);
            }
        }
        return ImmutableMap.copyOf(map);
    }

    private static List<ColumnInfo> getColumnInfo(List<Column> columns)
    {
        ImmutableList.Builder<ColumnInfo> list = ImmutableList.builder();
        for (Column column : columns) {
            ColumnInfo.Builder builder = new ColumnInfo.Builder()
                    .setCatalogName("") // TODO
                    .setSchemaName("") // TODO
                    .setTableName("") // TODO
                    .setColumnLabel(column.getName())
                    .setColumnName(column.getName()) // TODO
                    .setColumnTypeName(column.getType().toUpperCase())
                    .setNullable(ResultSetMetaData.columnNullableUnknown)
                    .setCurrency(false);
            setTypeInfo(builder, column.getType());
            list.add(builder.build());
        }
        return list.build();
    }

    private static void setTypeInfo(ColumnInfo.Builder builder, String type)
    {
        switch (type) {
            case "boolean":
                builder.setColumnType(Types.BOOLEAN);
                builder.setColumnDisplaySize(5);
                break;
            case "bigint":
                builder.setColumnType(Types.BIGINT);
                builder.setSigned(true);
                builder.setPrecision(19);
                builder.setScale(0);
                builder.setColumnDisplaySize(20);
                break;
            case "double":
                builder.setColumnType(Types.DOUBLE);
                builder.setSigned(true);
                builder.setPrecision(17);
                builder.setScale(0);
                builder.setColumnDisplaySize(24);
                break;
            case "varchar":
                builder.setColumnType(Types.LONGNVARCHAR);
                builder.setSigned(true);
                builder.setPrecision(VARCHAR_MAX);
                builder.setScale(0);
                builder.setColumnDisplaySize(VARCHAR_MAX);
                break;
            case "varbinary":
                builder.setColumnType(Types.LONGVARBINARY);
                builder.setSigned(true);
                builder.setPrecision(VARBINARY_MAX);
                builder.setScale(0);
                builder.setColumnDisplaySize(VARBINARY_MAX);
                break;
            case "time":
                builder.setColumnType(Types.TIME);
                builder.setSigned(true);
                builder.setPrecision(3);
                builder.setScale(0);
                builder.setColumnDisplaySize(TIME_MAX);
                break;
            case "time with time zone":
                builder.setColumnType(Types.TIME);
                builder.setSigned(true);
                builder.setPrecision(3);
                builder.setScale(0);
                builder.setColumnDisplaySize(TIME_WITH_TIME_ZONE_MAX);
                break;
            case "timestamp":
                builder.setColumnType(Types.TIMESTAMP);
                builder.setSigned(true);
                builder.setPrecision(3);
                builder.setScale(0);
                builder.setColumnDisplaySize(TIMESTAMP_MAX);
                break;
            case "timestamp with time zone":
                builder.setColumnType(Types.TIMESTAMP);
                builder.setSigned(true);
                builder.setPrecision(3);
                builder.setScale(0);
                builder.setColumnDisplaySize(TIMESTAMP_WITH_TIME_ZONE_MAX);
                break;
            case "date":
                builder.setColumnType(Types.DATE);
                builder.setSigned(true);
                builder.setScale(0);
                builder.setColumnDisplaySize(DATE_MAX);
                break;
            case "interval year to month":
                builder.setColumnType(Types.JAVA_OBJECT);
                builder.setColumnDisplaySize(TIMESTAMP_MAX);
                break;
            case "interval day to second":
                builder.setColumnType(Types.JAVA_OBJECT);
                builder.setColumnDisplaySize(TIMESTAMP_MAX);
                break;
            default:
                builder.setColumnType(Types.JAVA_OBJECT);
                break;
        }
    }
}
