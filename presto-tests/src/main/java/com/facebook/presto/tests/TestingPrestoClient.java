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
import com.facebook.presto.client.QueryResults;
import com.facebook.presto.server.testing.TestingPrestoServer;
import com.facebook.presto.spi.type.DecimalType;
import com.facebook.presto.spi.type.SqlIntervalDayTime;
import com.facebook.presto.spi.type.SqlIntervalYearMonth;
import com.facebook.presto.spi.type.TimeZoneKey;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.VarcharType;
import com.facebook.presto.testing.MaterializedResult;
import com.facebook.presto.testing.MaterializedRow;
import com.facebook.presto.type.ArrayType;
import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import io.airlift.log.Logger;
import org.joda.time.Period;
import org.joda.time.format.PeriodFormatter;
import org.joda.time.format.PeriodFormatterBuilder;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.type.DateTimeEncoding.unpackMillisUtc;
import static com.facebook.presto.spi.type.DateType.DATE;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.IntegerType.INTEGER;
import static com.facebook.presto.spi.type.IntervalDayTimeType.INTERVAL_DAY_TIME;
import static com.facebook.presto.spi.type.IntervalYearMonthType.INTERVAL_YEAR_MONTH;
import static com.facebook.presto.spi.type.SmallintType.SMALLINT;
import static com.facebook.presto.spi.type.TimeType.TIME;
import static com.facebook.presto.spi.type.TimeWithTimeZoneType.TIME_WITH_TIME_ZONE;
import static com.facebook.presto.spi.type.TimestampType.TIMESTAMP;
import static com.facebook.presto.spi.type.TimestampWithTimeZoneType.TIMESTAMP_WITH_TIME_ZONE;
import static com.facebook.presto.spi.type.TinyintType.TINYINT;
import static com.facebook.presto.spi.type.VarbinaryType.VARBINARY;
import static com.facebook.presto.testing.MaterializedResult.DEFAULT_PRECISION;
import static com.facebook.presto.util.DateTimeUtils.parseDate;
import static com.facebook.presto.util.DateTimeUtils.parseTime;
import static com.facebook.presto.util.DateTimeUtils.parseTimeWithTimeZone;
import static com.facebook.presto.util.DateTimeUtils.parseTimestampWithTimeZone;
import static com.facebook.presto.util.DateTimeUtils.parseTimestampWithoutTimeZone;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.Iterables.transform;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

public class TestingPrestoClient
        extends AbstractTestingPrestoClient<MaterializedResult>
{
    private static final Logger log = Logger.get("TestQueries");

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

    public TestingPrestoClient(TestingPrestoServer prestoServer, Session defaultSession)
    {
        super(prestoServer, defaultSession);
    }

    @Override
    protected ResultsSession<MaterializedResult> getResultSession(Session session)
    {
        return new MaterializedResultSession(session);
    }

    private class MaterializedResultSession
            implements ResultsSession<MaterializedResult>
    {
        private final ImmutableList.Builder<MaterializedRow> rows = ImmutableList.builder();
        private final AtomicBoolean loggedUri = new AtomicBoolean(false);

        private final AtomicReference<List<Type>> types = new AtomicReference<>();

        private final AtomicReference<Optional<String>> updateType = new AtomicReference<>(Optional.empty());
        private final AtomicReference<OptionalLong> updateCount = new AtomicReference<>(OptionalLong.empty());

        private final TimeZoneKey timeZoneKey;

        private MaterializedResultSession(Session session)
        {
            this.timeZoneKey = session.getTimeZoneKey();
        }

        @Override
        public void setUpdateType(String type)
        {
            updateType.set(Optional.of(requireNonNull("update type is null")));
        }

        @Override
        public void setUpdateCount(long count)
        {
            updateCount.set(OptionalLong.of(count));
        }

        @Override
        public void addResults(QueryResults results)
        {
            if (!loggedUri.getAndSet(true)) {
                log.info("Query %s: %s", results.getId(), results.getInfoUri());
            }

            if (types.get() == null && results.getColumns() != null) {
                types.set(getTypes(results.getColumns()));
            }

            if (results.getData() != null) {
                checkState(types.get() != null, "data received without types");
                rows.addAll(transform(results.getData(), dataToRow(timeZoneKey, types.get())));
            }
        }

        @Override
        public MaterializedResult build(Map<String, String> setSessionProperties, Set<String> resetSessionProperties)
        {
            checkState(types.get() != null, "never received types for the query");
            return new MaterializedResult(
                    rows.build(),
                    types.get(),
                    setSessionProperties,
                    resetSessionProperties,
                    updateType.get(),
                    updateCount.get());
        }
    }

    private static Function<List<Object>, MaterializedRow> dataToRow(final TimeZoneKey timeZoneKey, final List<Type> types)
    {
        return data -> {
            checkArgument(data.size() == types.size(), "columns size does not match types size");
            List<Object> row = new ArrayList<>();
            for (int i = 0; i < data.size(); i++) {
                Object value = data.get(i);
                Type type = types.get(i);
                row.add(convertToRowValue(type, value, timeZoneKey));
            }
            return new MaterializedRow(DEFAULT_PRECISION, row);
        };
    }

    private static Object convertToRowValue(Type type, Object value, TimeZoneKey timeZoneKey)
    {
        if (value == null) {
            return null;
        }

        if (BOOLEAN.equals(type)) {
            return value;
        }
        else if (TINYINT.equals(type)) {
            return ((Number) value).byteValue();
        }
        else if (SMALLINT.equals(type)) {
            return ((Number) value).shortValue();
        }
        else if (INTEGER.equals(type)) {
            return ((Number) value).intValue();
        }
        else if (BIGINT.equals(type)) {
            return ((Number) value).longValue();
        }
        else if (DOUBLE.equals(type)) {
            return ((Number) value).doubleValue();
        }
        else if (type instanceof VarcharType) {
            return value;
        }
        else if (VARBINARY.equals(type)) {
            return value;
        }
        else if (DATE.equals(type)) {
            int days = parseDate((String) value);
            return new Date(TimeUnit.DAYS.toMillis(days));
        }
        else if (TIME.equals(type)) {
            return new Time(parseTime(timeZoneKey, (String) value));
        }
        else if (TIME_WITH_TIME_ZONE.equals(type)) {
            return new Time(unpackMillisUtc(parseTimeWithTimeZone((String) value)));
        }
        else if (TIMESTAMP.equals(type)) {
            return new Timestamp(parseTimestampWithoutTimeZone(timeZoneKey, (String) value));
        }
        else if (TIMESTAMP_WITH_TIME_ZONE.equals(type)) {
            return new Timestamp(unpackMillisUtc(parseTimestampWithTimeZone(timeZoneKey, (String) value)));
        }
        else if (INTERVAL_DAY_TIME.equals(type)) {
            Period period = INTERVAL_DAY_TO_SECOND_FORMATTER.parsePeriod(String.valueOf(value));
            return new SqlIntervalDayTime(
                    period.getValue(DAY_FIELD),
                    period.getValue(HOUR_FIELD),
                    period.getValue(MINUTE_FIELD),
                    period.getValue(SECOND_FIELD),
                    period.getValue(MILLIS_FIELD));
        }
        else if (INTERVAL_YEAR_MONTH.equals(type)) {
            Period period = INTERVAL_YEAR_TO_MONTH_FORMATTER.parsePeriod(String.valueOf(value));
            return new SqlIntervalYearMonth(
                    period.getValue(YEAR_FIELD),
                    period.getValue(MONTH_FIELD));
        }
        else if (type instanceof ArrayType) {
            return ((List<Object>) value).stream()
                    .map(element -> convertToRowValue(((ArrayType) type).getElementType(), element, timeZoneKey))
                    .collect(toList());
        }
        else if (type instanceof DecimalType) {
            return new BigDecimal((String) value);
        }
        else {
            throw new AssertionError("unhandled type: " + type);
        }
    }
}
