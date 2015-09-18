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
package com.facebook.presto.plugin.postgresql;

import com.facebook.presto.spi.type.BigintType;
import com.facebook.presto.spi.type.BooleanType;
import com.facebook.presto.spi.type.DateType;
import com.facebook.presto.spi.type.DoubleType;
import com.facebook.presto.spi.type.TimestampType;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.VarcharType;
import io.airlift.slice.Slice;

import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

public class QueryBuilder extends com.facebook.presto.plugin.jdbc.QueryBuilder
{
    private static final DateTimeFormatter PATTERN = DateTimeFormatter
            .ofPattern("yyyy-MM-dd HH:mm:ss.SSS")
            .withZone(ZoneId.systemDefault());

    public QueryBuilder(String quote)
    {
        super(quote);
    }

    /**
     * @param type
     * @return True by default for {@link BigintType}, {@link DoubleType}, {@link BooleanType}, {@link VarcharType},
     * {@link DateType}, {@link TimestampType}.
     */
    @Override
    protected boolean supportPredicateOnType(Type type)
    {
        return type.equals(BigintType.BIGINT) || type.equals(DoubleType.DOUBLE) || type.equals(BooleanType.BOOLEAN) ||
                type.equals(VarcharType.VARCHAR) || type.equals(DateType.DATE) || type.equals(TimestampType.TIMESTAMP);
    }

    /**
     * @param type
     * @param value
     * @return Object value if {@link #supportPredicateOnType(Type)} returns true for the given type.
     * @throws UnsupportedOperationException If the type is not supported.
     */
    @Override
    protected String encode(Type type, Object value)
    {
        if (type.equals(TimestampType.TIMESTAMP)) {
            Instant ofEpochMilli = Instant.ofEpochMilli((long) value);
            return singleQuote(PATTERN.format(ofEpochMilli));
        }
        else if (type.equals(DateType.DATE)) {
            LocalDate localDate = LocalDate.from(Instant.ofEpochMilli((long) value).atZone(ZoneId.systemDefault()));
            return singleQuote(localDate.toString());
        }
        else if (type.equals(VarcharType.VARCHAR)) {
            if (value instanceof Slice) {
                return singleQuote(((Slice) value).toStringUtf8());
            }
            if (value instanceof String) {
                return singleQuote(value.toString());
            }
        }
        return super.encode(type, value);
    }
}
