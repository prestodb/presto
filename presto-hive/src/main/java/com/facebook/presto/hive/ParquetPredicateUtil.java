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
package com.facebook.presto.hive;

import io.airlift.slice.Slice;
import parquet.filter.UnboundRecordFilter;
import parquet.schema.Type;

import static parquet.filter.ColumnPredicates.applyFunctionToBoolean;
import static parquet.filter.ColumnPredicates.applyFunctionToInteger;
import static parquet.filter.ColumnPredicates.applyFunctionToLong;
import static parquet.filter.ColumnPredicates.applyFunctionToFloat;
import static parquet.filter.ColumnPredicates.applyFunctionToDouble;
import static parquet.filter.ColumnPredicates.applyFunctionToString;
import static parquet.filter.ColumnPredicates.equalTo;
import static parquet.filter.ColumnPredicates.BooleanPredicateFunction;
import static parquet.filter.ColumnPredicates.IntegerPredicateFunction;
import static parquet.filter.ColumnPredicates.LongPredicateFunction;
import static parquet.filter.ColumnPredicates.FloatPredicateFunction;
import static parquet.filter.ColumnPredicates.DoublePredicateFunction;
import static parquet.filter.ColumnPredicates.PredicateFunction;
import static parquet.filter.ColumnRecordFilter.column;

public final class ParquetPredicateUtil
{
    private ParquetPredicateUtil()
    {
    }

    public static UnboundRecordFilter createEqualToPredicate(Comparable<?> value, Type type, String columnName)
    {
        if (type.isPrimitive()) {
            switch (type.asPrimitiveType().getPrimitiveTypeName()) {
                case BOOLEAN:
                    return column(columnName, equalTo((Boolean) value));
                case INT32:
                    return column(columnName, equalTo(((Long) value).intValue()));
                case INT64:
                    return column(columnName, equalTo((Long) value));
                case FLOAT:
                    return column(columnName, equalTo(((Double) value).floatValue()));
                case DOUBLE:
                    return column(columnName, equalTo((Double) value));
                case BINARY:
                    return column(columnName, equalTo(((Slice) value).toStringUtf8()));
                case INT96:
                case FIXED_LEN_BYTE_ARRAY:
                default:
                    throw new IllegalArgumentException("Unsupported equalTo pushdown for type: " + type.toString());
            }
        }
        else {
            // ToDo: support predicate pushdown for Map, List, and Struct
            throw new IllegalArgumentException("Unsupported pushdown for type: " + type.toString());
        }
    }

    public static UnboundRecordFilter createGreaterThanOrEqualToPredicate(
                                                    Comparable<?> value, Type type, String columnName)
    {
        if (type.isPrimitive()) {
            switch (type.asPrimitiveType().getPrimitiveTypeName()) {
                case BOOLEAN:
                    return column(columnName, applyFunctionToBoolean(
                                        new BooleanGreaterThanOrEqualToPredicate((Boolean) value)));
                case INT32:
                    return column(columnName, applyFunctionToInteger(
                                        new IntegerGreaterThanOrEqualToPredicate(((Long) value).intValue())));
                case INT64:
                    return column(columnName, applyFunctionToLong(
                                        new LongGreaterThanOrEqualToPredicate((Long) value)));
                case FLOAT:
                    return column(columnName, applyFunctionToFloat(
                                        new FloatGreaterThanOrEqualToPredicate(((Long) value).floatValue())));
                case DOUBLE:
                    return column(columnName, applyFunctionToDouble(
                                        new DoubleGreaterThanOrEqualToPredicate((Double) value)));
                case BINARY:
                    return column(columnName, applyFunctionToString(
                                        new BinaryGreaterThanOrEqualToPredicate(((Slice) value).toStringUtf8())));
                case INT96:
                case FIXED_LEN_BYTE_ARRAY:
                default:
                    throw new IllegalArgumentException("Unsupported greaterThanOrEqualto pushdown for type: " + type.toString());
            }
        }
        else {
            // ToDo: support predicate pushdown for Map, List, and Struct
            throw new IllegalArgumentException("Unsupported pushdown for type: " + type.toString());
        }
    }

    public static UnboundRecordFilter createGreaterThanPredicate(
                                                    Comparable<?> value, Type type, String columnName)
    {
        if (type.isPrimitive()) {
            switch (type.asPrimitiveType().getPrimitiveTypeName()) {
                case BOOLEAN:
                    return column(columnName, applyFunctionToBoolean(
                                        new BooleanGreaterThanPredicate((Boolean) value)));
                case INT32:
                    return column(columnName, applyFunctionToInteger(
                                        new IntegerGreaterThanPredicate(((Long) value).intValue())));
                case INT64:
                    return column(columnName, applyFunctionToLong(
                                        new LongGreaterThanPredicate((Long) value)));
                case FLOAT:
                    return column(columnName, applyFunctionToFloat(
                                        new FloatGreaterThanPredicate(((Long) value).floatValue())));
                case DOUBLE:
                    return column(columnName, applyFunctionToDouble(
                                        new DoubleGreaterThanPredicate((Double) value)));
                case BINARY:
                    return column(columnName, applyFunctionToString(
                                        new BinaryGreaterThanPredicate(((Slice) value).toStringUtf8())));
                case INT96:
                case FIXED_LEN_BYTE_ARRAY:
                default:
                    throw new IllegalArgumentException("Unsupported greaterThan pushdown for type: " + type.toString());
            }
        }
        else {
            // ToDo: support predicate pushdown for Map, List, and Struct
            throw new IllegalArgumentException("Unsupported pushdown for type: " + type.toString());
        }
    }

    public static UnboundRecordFilter createLessThanOrEqualToPredicate(
                                                    Comparable<?> value, Type type, String columnName)
    {
        if (type.isPrimitive()) {
            switch (type.asPrimitiveType().getPrimitiveTypeName()) {
                case BOOLEAN:
                    return column(columnName, applyFunctionToBoolean(
                                        new BooleanLessThanOrEqualToPredicate((Boolean) value)));
                case INT32:
                    return column(columnName, applyFunctionToInteger(
                                        new IntegerLessThanOrEqualToPredicate(((Long) value).intValue())));
                case INT64:
                    return column(columnName, applyFunctionToLong(
                                        new LongLessThanOrEqualToPredicate((Long) value)));
                case FLOAT:
                    return column(columnName, applyFunctionToFloat(
                                        new FloatLessThanOrEqualToPredicate(((Long) value).floatValue())));
                case DOUBLE:
                    return column(columnName, applyFunctionToDouble(
                                        new DoubleLessThanOrEqualToPredicate((Double) value)));
                case BINARY:
                    return column(columnName, applyFunctionToString(
                                        new BinaryLessThanOrEqualToPredicate(((Slice) value).toStringUtf8())));
                case INT96:
                case FIXED_LEN_BYTE_ARRAY:
                default:
                    throw new IllegalArgumentException("Unsupported lessThanOrEqualto pushdown for type: " + type.toString());
            }
        }
        else {
            // ToDo: support predicate pushdown for Map, List, and Struct
            throw new IllegalArgumentException("Unsupported pushdown for type: " + type.toString());
        }
    }

    public static UnboundRecordFilter createLessThanPredicate(
                                                    Comparable<?> value, Type type, String columnName)
    {
        if (type.isPrimitive()) {
            switch (type.asPrimitiveType().getPrimitiveTypeName()) {
                case BOOLEAN:
                    return column(columnName, applyFunctionToBoolean(
                                        new BooleanLessThanPredicate((Boolean) value)));
                case INT32:
                    return column(columnName, applyFunctionToInteger(
                                        new IntegerLessThanPredicate(((Long) value).intValue())));
                case INT64:
                    return column(columnName, applyFunctionToLong(
                                        new LongLessThanPredicate((Long) value)));
                case FLOAT:
                    return column(columnName, applyFunctionToFloat(
                                        new FloatLessThanPredicate(((Long) value).floatValue())));
                case DOUBLE:
                    return column(columnName, applyFunctionToDouble(
                                        new DoubleLessThanPredicate((Double) value)));
                case BINARY:
                    return column(columnName, applyFunctionToString(
                                        new BinaryLessThanPredicate(((Slice) value).toStringUtf8())));
                case INT96:
                case FIXED_LEN_BYTE_ARRAY:
                default:
                    throw new IllegalArgumentException("Unsupported lessThan pushdown for type: " + type.toString());
            }
        }
        else {
            // ToDo: support predicate pushdown for Map, List, and Struct
            throw new IllegalArgumentException("Unsupported pushdown for type: " + type.toString());
        }
    }

    public static class BinaryGreaterThanPredicate implements PredicateFunction<String>
    {
        private final String base;

        BinaryGreaterThanPredicate(String base)
        {
            this.base = base;
        }

        @Override
        public boolean functionToApply(String input)
        {
            return input.compareTo(base) > 0;
        }
    };

    public static class BinaryGreaterThanOrEqualToPredicate implements PredicateFunction<String>
    {
        private final String base;

        BinaryGreaterThanOrEqualToPredicate(String base)
        {
            this.base = base;
        }

        @Override
        public boolean functionToApply(String input)
        {
            return input.compareTo(base) >= 0;
        }
    };

    public static class BinaryLessThanPredicate implements PredicateFunction<String>
    {
        private final String base;

        BinaryLessThanPredicate(String base)
        {
            this.base = base;
        }

        @Override
        public boolean functionToApply(String input)
        {
            return input.compareTo(base) < 0;
        }
    };

    public static class BinaryLessThanOrEqualToPredicate implements PredicateFunction<String>
    {
        private final String base;

        BinaryLessThanOrEqualToPredicate(String base)
        {
            this.base = base;
        }

        @Override
        public boolean functionToApply(String input)
        {
            return input.compareTo(base) <= 0;
        }
    };

    public static class LongGreaterThanPredicate implements LongPredicateFunction
    {
        private final long base;

        LongGreaterThanPredicate(long base)
        {
            this.base = base;
        }

        @Override
        public boolean functionToApply(long input)
        {
            return input > base;
        }
    };

    public static class LongGreaterThanOrEqualToPredicate implements LongPredicateFunction
    {
        private final long base;

        LongGreaterThanOrEqualToPredicate(long base)
        {
            this.base = base;
        }

        @Override
        public boolean functionToApply(long input)
        {
            return input >= base;
        }
    };

    public static class LongLessThanPredicate implements LongPredicateFunction
    {
        private final long base;

        LongLessThanPredicate(long base)
        {
            this.base = base;
        }

        @Override
        public boolean functionToApply(long input)
        {
            return input < base;
        }
    };

    public static class LongLessThanOrEqualToPredicate implements LongPredicateFunction
    {
        private final long base;

        LongLessThanOrEqualToPredicate(long base)
        {
            this.base = base;
        }

        @Override
        public boolean functionToApply(long input)
        {
            return input <= base;
        }
    };

    public static class IntegerGreaterThanPredicate implements IntegerPredicateFunction
    {
        private final int base;

        IntegerGreaterThanPredicate(int base)
        {
            this.base = base;
        }

        @Override
        public boolean functionToApply(int input)
        {
            return input > base;
        }
    };

    public static class IntegerGreaterThanOrEqualToPredicate implements IntegerPredicateFunction
    {
        private final int base;

        IntegerGreaterThanOrEqualToPredicate(int base)
        {
            this.base = base;
        }

        @Override
        public boolean functionToApply(int input)
        {
            return input >= base;
        }
    };

    public static class IntegerLessThanPredicate implements IntegerPredicateFunction
    {
        private final int base;

        IntegerLessThanPredicate(int base)
        {
            this.base = base;
        }

        @Override
        public boolean functionToApply(int input)
        {
            return input < base;
        }
    };

    public static class IntegerLessThanOrEqualToPredicate implements IntegerPredicateFunction
    {
        private final int base;

        IntegerLessThanOrEqualToPredicate(int base)
        {
            this.base = base;
        }

        @Override
        public boolean functionToApply(int input)
        {
            return input <= base;
        }
    };

    public static class DoubleGreaterThanPredicate implements DoublePredicateFunction
    {
        private final double base;

        DoubleGreaterThanPredicate(double base)
        {
            this.base = base;
        }

        @Override
        public boolean functionToApply(double input)
        {
            return input > base;
        }
    };

    public static class DoubleGreaterThanOrEqualToPredicate implements DoublePredicateFunction
    {
        private final double base;

        DoubleGreaterThanOrEqualToPredicate(double base)
        {
            this.base = base;
        }

        @Override
        public boolean functionToApply(double input)
        {
            return input >= base;
        }
    };

    public static class DoubleLessThanPredicate implements DoublePredicateFunction
    {
        private final double base;

        DoubleLessThanPredicate(double base)
        {
            this.base = base;
        }

        @Override
        public boolean functionToApply(double input)
        {
            return input < base;
        }
    };

    public static class DoubleLessThanOrEqualToPredicate implements DoublePredicateFunction
    {
        private final double base;

        DoubleLessThanOrEqualToPredicate(double base)
        {
            this.base = base;
        }

        @Override
        public boolean functionToApply(double input)
        {
            return input <= base;
        }
    };

    public static class FloatGreaterThanPredicate implements FloatPredicateFunction
    {
        private final float base;

        FloatGreaterThanPredicate(float base)
        {
            this.base = base;
        }

        @Override
        public boolean functionToApply(float input)
        {
            return input > base;
        }
    };

    public static class FloatGreaterThanOrEqualToPredicate implements FloatPredicateFunction
    {
        private final float base;

        FloatGreaterThanOrEqualToPredicate(float base)
        {
            this.base = base;
        }

        @Override
        public boolean functionToApply(float input)
        {
            return input >= base;
        }
    };

    public static class FloatLessThanPredicate implements FloatPredicateFunction
    {
        private final float base;

        FloatLessThanPredicate(float base)
        {
            this.base = base;
        }

        @Override
        public boolean functionToApply(float input)
        {
            return input < base;
        }
    };

    public static class FloatLessThanOrEqualToPredicate implements FloatPredicateFunction
    {
        private final float base;

        FloatLessThanOrEqualToPredicate(float base)
        {
            this.base = base;
        }

        @Override
        public boolean functionToApply(float input)
        {
            return input <= base;
        }
    };

    public static class BooleanGreaterThanPredicate implements BooleanPredicateFunction
    {
        private final boolean base;

        BooleanGreaterThanPredicate(boolean base)
        {
            this.base = base;
        }

        @Override
        public boolean functionToApply(boolean input)
        {
            return new Boolean(input).compareTo(new Boolean(base)) > 0;
        }
    };

    public static class BooleanGreaterThanOrEqualToPredicate implements BooleanPredicateFunction
    {
        private final boolean base;

        BooleanGreaterThanOrEqualToPredicate(boolean base)
        {
            this.base = base;
        }

        @Override
        public boolean functionToApply(boolean input)
        {
            return new Boolean(input).compareTo(new Boolean(base)) >= 0;
        }
    };

    public static class BooleanLessThanPredicate implements BooleanPredicateFunction
    {
        private final boolean base;

        BooleanLessThanPredicate(boolean base)
        {
            this.base = base;
        }

        @Override
        public boolean functionToApply(boolean input)
        {
            return new Boolean(input).compareTo(new Boolean(base)) < 0;
        }
    };

    public static class BooleanLessThanOrEqualToPredicate implements BooleanPredicateFunction
    {
        private final boolean base;

        BooleanLessThanOrEqualToPredicate(boolean base)
        {
            this.base = base;
        }

        @Override
        public boolean functionToApply(boolean input)
        {
            return new Boolean(input).compareTo(new Boolean(base)) <= 0;
        }
    };
}
