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
package com.facebook.presto.spi.relation;

import com.facebook.presto.common.type.DateType;
import com.facebook.presto.common.type.TimeType;
import com.facebook.presto.common.type.TimeWithTimeZoneType;
import com.facebook.presto.common.type.TimestampType;
import com.facebook.presto.common.type.TimestampWithTimeZoneType;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.spi.SourceLocation;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static java.util.Collections.emptyList;
import static java.util.Objects.requireNonNull;

public class CurrentTimeRowExpression
        extends IntermediateFormRowExpression
{
    private final Function function;
    private final Integer precision;

    public CurrentTimeRowExpression(Optional<SourceLocation> sourceLocation, Function function, Integer precision)
    {
        super(sourceLocation);
        this.function = requireNonNull(function, "function is null");
        this.precision = precision;
    }

    public Function getFunction()
    {
        return function;
    }

    public Integer getPrecision()
    {
        return precision;
    }

    @Override
    public Type getType()
    {
        return function.type;
    }

    @Override
    public List<RowExpression> getChildren()
    {
        return emptyList();
    }

    @Override
    public boolean equals(Object other)
    {
        if (this == other) {
            return true;
        }
        if (other == null || getClass() != other.getClass()) {
            return false;
        }
        CurrentTimeRowExpression obj = (CurrentTimeRowExpression) other;
        return this.function.equals(obj.function);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(function);
    }

    @Override
    public String toString()
    {
        return function.toString();
    }

    @Override
    public <R, C> R accept(RowExpressionVisitor<R, C> visitor, C context)
    {
        return visitor.visitCurrentTimeRowExpression(this, context);
    }

    @Override
    public RowExpression canonicalize()
    {
        return this;
    }

    public enum Function
    {
        TIME("current_time", TimeWithTimeZoneType.TIME_WITH_TIME_ZONE),
        DATE("current_date", DateType.DATE),
        TIMESTAMP("current_timestamp", TimestampWithTimeZoneType.TIMESTAMP_WITH_TIME_ZONE),
        LOCALTIME("localtime", TimeType.TIME),
        LOCALTIMESTAMP("localtimestamp", TimestampType.TIMESTAMP);

        private final String name;
        private final Type type;

        Function(String name, Type type)
        {
            this.name = name;
            this.type = type;
        }

        public String getName()
        {
            return name;
        }

        public Type getType()
        {
            return type;
        }
    }
}
