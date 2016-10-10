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

package com.facebook.presto.hive.metastore;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;

import java.util.Optional;

import static java.util.Arrays.stream;

public class ColumnStatistics
{
    public enum Type
    {
        LONG,
        DOUBLE,
        DECIMAL,
        BOOLEAN,
        DATE,
        STRING,
        BINARY
    }

    private final Type type;
    private final Optional<LongColumnStatistics> longColumnStatistics;
    private final Optional<DoubleColumnStatistics> doubleColumnStatistics;
    private final Optional<DecimalColumnStatistics> decimalColumnStatistics;
    private final Optional<BooleanColumnStatistics> booleanColumnStatistics;
    private final Optional<DateColumnStatistics> dateColumnStatistics;
    private final Optional<StringColumnStatistics> stringColumnStatistics;
    private final Optional<BinaryColumnStatistics> binaryColumnStatistics;

    @JsonCreator
    public ColumnStatistics(
            @JsonProperty("longColumnStatistics") Optional<LongColumnStatistics> longColumnStatistics,
            @JsonProperty("doubleColumnStatistics") Optional<DoubleColumnStatistics> doubleColumnStatistics,
            @JsonProperty("decimalColumnStatistics") Optional<DecimalColumnStatistics> decimalColumnStatistics,
            @JsonProperty("booleanColumnStatistics") Optional<BooleanColumnStatistics> booleanColumnStatistics,
            @JsonProperty("dateColumnStatistics") Optional<DateColumnStatistics> dateColumnStatistics,
            @JsonProperty("stringColumnStatistics") Optional<StringColumnStatistics> stringColumnStatistics,
            @JsonProperty("binaryColumnStatistics") Optional<BinaryColumnStatistics> binaryColumnStatistics)
    {
        Preconditions.checkArgument(
                countNonEmpty(longColumnStatistics, doubleColumnStatistics, decimalColumnStatistics, booleanColumnStatistics,
                        dateColumnStatistics, stringColumnStatistics, binaryColumnStatistics) == 1,
                "exactly one of long/double/decimal/boolean/date/string/binary statistics expected");
        this.longColumnStatistics = longColumnStatistics;
        this.doubleColumnStatistics = doubleColumnStatistics;
        this.decimalColumnStatistics = decimalColumnStatistics;
        this.booleanColumnStatistics = booleanColumnStatistics;
        this.dateColumnStatistics = dateColumnStatistics;
        this.stringColumnStatistics = stringColumnStatistics;
        this.binaryColumnStatistics = binaryColumnStatistics;

        if (longColumnStatistics.isPresent()) {
            type = Type.LONG;
        }
        else if (doubleColumnStatistics.isPresent()) {
            type = Type.DOUBLE;
        }
        else if (decimalColumnStatistics.isPresent()) {
            type = Type.DECIMAL;
        }
        else if (booleanColumnStatistics.isPresent()) {
            type = Type.BOOLEAN;
        }
        else if (dateColumnStatistics.isPresent()) {
            type = Type.DATE;
        }
        else if (stringColumnStatistics.isPresent()) {
            type = Type.STRING;
        }
        else if (binaryColumnStatistics.isPresent()) {
            type = Type.BINARY;
        }
        else {
            throw new IllegalArgumentException("cannot determine statistics type");
        }
    }

    private long countNonEmpty(Optional<?>... optionals)
    {
        return stream(optionals).mapToLong(optional -> optional.isPresent() ? 1 : 0).sum();
    }

    public Type getType()
    {
        return type;
    }

    @JsonProperty
    public LongColumnStatistics getLongColumnStatistics()
    {
        return longColumnStatistics.get();
    }

    @JsonProperty
    public DoubleColumnStatistics getDoubleColumnStatistics()
    {
        return doubleColumnStatistics.get();
    }

    @JsonProperty
    public DecimalColumnStatistics getDecimalColumnStatistics()
    {
        return decimalColumnStatistics.get();
    }

    @JsonProperty
    public BooleanColumnStatistics getBooleanColumnStatistics()
    {
        return booleanColumnStatistics.get();
    }

    @JsonProperty
    public DateColumnStatistics getDateColumnStatistics()
    {
        return dateColumnStatistics.get();
    }

    @JsonProperty
    public StringColumnStatistics getStringColumnStatistics()
    {
        return stringColumnStatistics.get();
    }

    @JsonProperty
    public BinaryColumnStatistics getBinaryColumnStatistics()
    {
        return binaryColumnStatistics.get();
    }

    public static ColumnStatistics columnStatistics(LongColumnStatistics longColumnStatistics)
    {
        return new ColumnStatistics(Optional.of(longColumnStatistics), Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty());
    }

    public static ColumnStatistics columnStatistics(DoubleColumnStatistics doubleColumnStatistics)
    {
        return new ColumnStatistics(Optional.empty(), Optional.of(doubleColumnStatistics), Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty());
    }

    public static ColumnStatistics columnStatistics(DecimalColumnStatistics decimalColumnStatistics)
    {
        return new ColumnStatistics(Optional.empty(), Optional.empty(), Optional.of(decimalColumnStatistics), Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty());
    }

    public static ColumnStatistics columnStatistics(BooleanColumnStatistics booleanColumnStatistics)
    {
        return new ColumnStatistics(Optional.empty(), Optional.empty(), Optional.empty(), Optional.of(booleanColumnStatistics), Optional.empty(), Optional.empty(), Optional.empty());
    }

    public static ColumnStatistics columnStatistics(DateColumnStatistics dateColumnStatistics)
    {
        return new ColumnStatistics(Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty(), Optional.of(dateColumnStatistics), Optional.empty(), Optional.empty());
    }

    public static ColumnStatistics columnStatistics(StringColumnStatistics stringColumnStatistics)
    {
        return new ColumnStatistics(Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty(), Optional.of(stringColumnStatistics), Optional.empty());
    }

    public static ColumnStatistics columnStatistics(BinaryColumnStatistics binaryColumnStatistics)
    {
        return new ColumnStatistics(Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty(), Optional.of(binaryColumnStatistics));
    }
}
