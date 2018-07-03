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

import com.facebook.presto.spi.type.TypeSignature;
import com.facebook.presto.spi.type.TypeSignatureParameter;
import com.google.common.collect.ImmutableList;

import java.sql.Types;
import java.util.List;

public class TypeInfo
{
    private int sqlType;
    private List<Integer> parameterSqlTypes;
    private TypeSignature typeSignature;
    private Nullable nullable;
    private boolean currency;
    private boolean signed;
    private int precision;
    private int scale;
    private int displaySize;

    public enum Nullable
    {
        NO_NULLS, NULLABLE, UNKNOWN
    }

    public TypeInfo(int sqlType, List<Integer> parameterSqlTypes, TypeSignature typeSignature, Nullable nullable,
                    boolean currency, boolean signed, int precision, int scale, int displaySize)
    {
        this.sqlType = sqlType;
        this.parameterSqlTypes = parameterSqlTypes;
        this.typeSignature = typeSignature;
        this.nullable = nullable;
        this.currency = currency;
        this.signed = signed;
        this.precision = precision;
        this.scale = scale;
        this.displaySize = displaySize;
    }

    public int getSqlType()
    {
        return sqlType;
    }

    public List<Integer> getParameterSqlTypes()
    {
        return parameterSqlTypes;
    }

    public TypeSignature getTypeSignature()
    {
        return typeSignature;
    }

    public Nullable getNullable()
    {
        return nullable;
    }

    public boolean isCurrency()
    {
        return currency;
    }

    public boolean isSigned()
    {
        return signed;
    }

    public int getPrecision()
    {
        return precision;
    }

    public int getScale()
    {
        return scale;
    }

    public int getDisplaySize()
    {
        return displaySize;
    }

    static class Builder
    {
        private static final int VARCHAR_MAX = 1024 * 1024 * 1024;
        private static final int VARBINARY_MAX = 1024 * 1024 * 1024;
        private static final int TIME_ZONE_MAX = 40; // current longest time zone is 32
        private static final int TIME_MAX = "HH:mm:ss.SSS".length();
        private static final int TIME_WITH_TIME_ZONE_MAX = TIME_MAX + TIME_ZONE_MAX;
        private static final int TIMESTAMP_MAX = "yyyy-MM-dd HH:mm:ss.SSS".length();
        private static final int TIMESTAMP_WITH_TIME_ZONE_MAX = TIMESTAMP_MAX + TIME_ZONE_MAX;
        private static final int DATE_MAX = "yyyy-MM-dd".length();

        private int sqlType;
        private List<Integer> parameterSqlTypes;
        private TypeSignature typeSignature;
        private Nullable nullable;
        private boolean currency;
        private boolean signed;
        private int precision;
        private int scale;
        private int displaySize;

        public Builder setTypeSignature(TypeSignature signature)
        {
            this.typeSignature = signature;
            this.sqlType = getType(signature);

            ImmutableList.Builder<Integer> parameterTypes = ImmutableList.builder();
            for (TypeSignatureParameter parameter : signature.getParameters()) {
                parameterTypes.add(getType(parameter));
            }
            this.parameterSqlTypes = parameterTypes.build();

            switch (signature.toString()) {
                case "boolean":
                    this.displaySize = 5;
                    break;
                case "bigint":
                    this.signed = true;
                    this.precision = 19;
                    this.scale = 0;
                    this.displaySize = 20;
                    break;
                case "integer":
                    this.signed = true;
                    this.precision = 10;
                    this.scale = 0;
                    this.displaySize = 11;
                    break;
                case "smallint":
                    this.signed = true;
                    this.precision = 5;
                    this.scale = 0;
                    this.displaySize = 6;
                    break;
                case "tinyint":
                    this.signed = true;
                    this.precision = 3;
                    this.scale = 0;
                    this.displaySize = 4;
                    break;
                case "real":
                    this.signed = true;
                    this.precision = 9;
                    this.scale = 0;
                    this.displaySize = 16;
                    break;
                case "double":
                    this.signed = true;
                    this.precision = 17;
                    this.scale = 0;
                    this.displaySize = 24;
                    break;
                case "varchar":
                    this.signed = true;
                    this.precision = VARCHAR_MAX;
                    this.scale = 0;
                    this.displaySize = VARCHAR_MAX;
                    break;
                case "varbinary":
                    this.signed = true;
                    this.precision = VARBINARY_MAX;
                    this.scale = 0;
                    this.displaySize = VARBINARY_MAX;
                    break;
                case "time":
                    this.signed = true;
                    this.precision = 3;
                    this.scale = 0;
                    this.displaySize = TIME_MAX;
                    break;
                case "time with time zone":
                    this.signed = true;
                    this.precision = 3;
                    this.scale = 0;
                    this.displaySize = TIME_WITH_TIME_ZONE_MAX;
                    break;
                case "timestamp":
                    this.signed = true;
                    this.precision = 3;
                    this.scale = 0;
                    this.displaySize = TIMESTAMP_MAX;
                    break;
                case "timestamp with time zone":
                    this.signed = true;
                    this.precision = 3;
                    this.scale = 0;
                    this.displaySize = TIMESTAMP_WITH_TIME_ZONE_MAX;
                    break;
                case "date":
                    this.signed = true;
                    this.scale = 0;
                    this.displaySize = DATE_MAX;
                    break;
                case "interval year to month":
                    this.displaySize = TIMESTAMP_MAX;
                    break;
                case "interval day to second":
                    this.displaySize = TIMESTAMP_MAX;
                    break;
                case "decimal":
                    this.signed = true;
                    this.precision = signature.getParameters().get(0).getLongLiteral().intValue();
                    this.scale = signature.getParameters().get(1).getLongLiteral().intValue();
                    this.displaySize = signature.getParameters().get(0).getLongLiteral().intValue() + 2; // dot and sign
                    break;
            }
            return this;
        }

        public Builder setNullable(Nullable nullable)
        {
            this.nullable = nullable;
            return this;
        }

        public Builder setCurrency(boolean currency)
        {
            this.currency = currency;
            return this;
        }

        public TypeInfo build()
        {
            return new TypeInfo(
                    sqlType,
                    parameterSqlTypes,
                    typeSignature,
                    nullable,
                    currency,
                    signed,
                    precision,
                    scale,
                    displaySize);
        }

        private static int getType(TypeSignatureParameter typeParameter)
        {
            switch (typeParameter.getKind()) {
                case TYPE:
                    return getType(typeParameter.getTypeSignature());
                default:
                    return Types.JAVA_OBJECT;
            }
        }

        private static int getType(TypeSignature type)
        {
            if (type.getBase().equals("array")) {
                return Types.ARRAY;
            }
            switch (type.getBase()) {
                case "boolean":
                    return Types.BOOLEAN;
                case "bigint":
                    return Types.BIGINT;
                case "integer":
                    return Types.INTEGER;
                case "smallint":
                    return Types.SMALLINT;
                case "tinyint":
                    return Types.TINYINT;
                case "real":
                    return Types.REAL;
                case "double":
                    return Types.DOUBLE;
                case "varchar":
                    return Types.VARCHAR;
                case "char":
                    return Types.CHAR;
                case "varbinary":
                    return Types.VARBINARY;
                case "time":
                    return Types.TIME;
                case "time with time zone":
                    return Types.TIME;
                case "timestamp":
                    return Types.TIMESTAMP;
                case "timestamp with time zone":
                    return Types.TIMESTAMP;
                case "date":
                    return Types.DATE;
                case "decimal":
                    return Types.DECIMAL;
                case "unknown":
                    return Types.NULL;
                default:
                    return Types.JAVA_OBJECT;
            }
        }
    }
}
