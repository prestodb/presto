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
package io.prestosql.plugin.kafka.util;

import com.google.common.collect.ImmutableMap;
import io.prestosql.Session;
import io.prestosql.client.Column;
import io.prestosql.client.QueryData;
import io.prestosql.client.QueryStatusInfo;
import io.prestosql.server.testing.TestingPrestoServer;
import io.prestosql.spi.type.TimeZoneKey;
import io.prestosql.spi.type.Type;
import io.prestosql.spi.type.Varchars;
import io.prestosql.tests.AbstractTestingPrestoClient;
import io.prestosql.tests.ResultsSession;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static com.google.common.base.Preconditions.checkState;
import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.spi.type.BooleanType.BOOLEAN;
import static io.prestosql.spi.type.DateTimeEncoding.unpackMillisUtc;
import static io.prestosql.spi.type.DateType.DATE;
import static io.prestosql.spi.type.DoubleType.DOUBLE;
import static io.prestosql.spi.type.IntegerType.INTEGER;
import static io.prestosql.spi.type.TimeType.TIME;
import static io.prestosql.spi.type.TimeWithTimeZoneType.TIME_WITH_TIME_ZONE;
import static io.prestosql.spi.type.TimestampType.TIMESTAMP;
import static io.prestosql.spi.type.TimestampWithTimeZoneType.TIMESTAMP_WITH_TIME_ZONE;
import static io.prestosql.util.DateTimeUtils.parseTimeLiteral;
import static io.prestosql.util.DateTimeUtils.parseTimestampWithTimeZone;
import static io.prestosql.util.DateTimeUtils.parseTimestampWithoutTimeZone;
import static java.util.Objects.requireNonNull;

public class KafkaLoader
        extends AbstractTestingPrestoClient<Void>
{
    private static final DateTimeFormatter ISO8601_FORMATTER = ISODateTimeFormat.dateTime();

    private final String topicName;
    private final Producer<Long, Object> producer;
    private final AtomicLong count = new AtomicLong();

    public KafkaLoader(Producer<Long, Object> producer,
            String topicName,
            TestingPrestoServer prestoServer,
            Session defaultSession)
    {
        super(prestoServer, defaultSession);

        this.topicName = topicName;
        this.producer = producer;
    }

    @Override
    public ResultsSession<Void> getResultSession(Session session)
    {
        requireNonNull(session, "session is null");
        return new KafkaLoadingSession(session);
    }

    private class KafkaLoadingSession
            implements ResultsSession<Void>
    {
        private final AtomicReference<List<Type>> types = new AtomicReference<>();

        private final TimeZoneKey timeZoneKey;

        private KafkaLoadingSession(Session session)
        {
            this.timeZoneKey = session.getTimeZoneKey();
        }

        @Override
        public void addResults(QueryStatusInfo statusInfo, QueryData data)
        {
            if (types.get() == null && statusInfo.getColumns() != null) {
                types.set(getTypes(statusInfo.getColumns()));
            }

            if (data.getData() != null) {
                checkState(types.get() != null, "Data without types received!");
                List<Column> columns = statusInfo.getColumns();
                for (List<Object> fields : data.getData()) {
                    ImmutableMap.Builder<String, Object> builder = ImmutableMap.builder();
                    for (int i = 0; i < fields.size(); i++) {
                        Type type = types.get().get(i);
                        Object value = convertValue(fields.get(i), type);
                        if (value != null) {
                            builder.put(columns.get(i).getName(), value);
                        }
                    }

                    producer.send(new KeyedMessage<>(topicName, count.getAndIncrement(), builder.build()));
                }
            }
        }

        @Override
        public Void build(Map<String, String> setSessionProperties, Set<String> resetSessionProperties)
        {
            return null;
        }

        private Object convertValue(Object value, Type type)
        {
            if (value == null) {
                return null;
            }

            if (BOOLEAN.equals(type) || Varchars.isVarcharType(type)) {
                return value;
            }
            if (BIGINT.equals(type)) {
                return ((Number) value).longValue();
            }
            if (INTEGER.equals(type)) {
                return ((Number) value).intValue();
            }
            if (DOUBLE.equals(type)) {
                return ((Number) value).doubleValue();
            }
            if (DATE.equals(type)) {
                return value;
            }
            if (TIME.equals(type)) {
                return ISO8601_FORMATTER.print(parseTimeLiteral(timeZoneKey, (String) value));
            }
            if (TIMESTAMP.equals(type)) {
                return ISO8601_FORMATTER.print(parseTimestampWithoutTimeZone(timeZoneKey, (String) value));
            }
            if (TIME_WITH_TIME_ZONE.equals(type) || TIMESTAMP_WITH_TIME_ZONE.equals(type)) {
                return ISO8601_FORMATTER.print(unpackMillisUtc(parseTimestampWithTimeZone(timeZoneKey, (String) value)));
            }
            throw new AssertionError("unhandled type: " + type);
        }
    }
}
