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

import static com.facebook.presto.jdbc.ColumnInfo.*;
import static java.util.Objects.requireNonNull;

public class ParameterInfo
{
    private final int position;
    private final int parameterType;
    private final TypeSignature parameterTypeSignature;
    private final Nullable nullable;
    private final boolean signed;
    private final int precision;
    private final int scale;

    public ParameterInfo(
            int position,
            int parameterType,
            TypeSignature parameterTypeSignature,
            Nullable nullable,
            boolean signed,
            int precision,
            int scale)
    {
        this.position = position;
        this.parameterType = parameterType;
        this.parameterTypeSignature = requireNonNull(parameterTypeSignature, "parameterTypeName is null");
        this.nullable = requireNonNull(nullable, "nullable is null");
        this.signed = signed;
        this.precision = precision;
        this.scale = scale;
    }

    public static void setTypeInfo(ParameterInfo.Builder builder, TypeSignature type)
    {
        builder.setParameterType(getType(type));
        switch (type.toString()) {
            case "boolean":
                break;
            case "bigint":
                builder.setSigned(true);
                builder.setPrecision(19);
                builder.setScale(0);
                break;
            case "integer":
                builder.setSigned(true);
                builder.setPrecision(10);
                builder.setScale(0);
                break;
            case "smallint":
                builder.setSigned(true);
                builder.setPrecision(5);
                builder.setScale(0);
                break;
            case "tinyint":
                builder.setSigned(true);
                builder.setPrecision(3);
                builder.setScale(0);
                break;
            case "real":
                builder.setSigned(true);
                builder.setPrecision(9);
                builder.setScale(0);
                break;
            case "double":
                builder.setSigned(true);
                builder.setPrecision(17);
                builder.setScale(0);
                break;
            case "varchar":
                builder.setSigned(true);
                builder.setPrecision(VARCHAR_MAX);
                builder.setScale(0);
                break;
            case "varbinary":
                builder.setSigned(true);
                builder.setPrecision(VARBINARY_MAX);
                builder.setScale(0);
                break;
            case "time":
                builder.setSigned(true);
                builder.setPrecision(3);
                builder.setScale(0);
                break;
            case "time with time zone":
                builder.setSigned(true);
                builder.setPrecision(3);
                builder.setScale(0);
                break;
            case "timestamp":
                builder.setSigned(true);
                builder.setPrecision(3);
                builder.setScale(0);
                break;
            case "timestamp with time zone":
                builder.setSigned(true);
                builder.setPrecision(3);
                builder.setScale(0);
                break;
            case "date":
                builder.setSigned(true);
                builder.setScale(0);
                break;
            case "interval year to month":
                break;
            case "interval day to second":
                break;
            case "decimal":
                builder.setSigned(true);
                builder.setPrecision(type.getParameters().get(0).getLongLiteral().intValue());
                builder.setScale(type.getParameters().get(1).getLongLiteral().intValue());
                break;
        }
    }

    public String getParameterTypeName()
    {
        return parameterTypeSignature.toString();
    }

    public int getPosition()
    {
        return position;
    }

    public int getParameterType()
    {
        return parameterType;
    }

    public TypeSignature getParameterTypeSignature()
    {
        return parameterTypeSignature;
    }

    public Nullable getNullable()
    {
        return nullable;
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

    static class Builder
    {
        private int position;
        private int parameterType;
        private TypeSignature parameterTypeSignature;
        private Nullable nullable;
        private boolean signed;
        private int precision;
        private int scale;

        public ParameterInfo.Builder setPosition(int position)
        {
            this.position = position;
            return this;
        }

        public ParameterInfo.Builder setParameterType(int parameterType)
        {
            this.parameterType = parameterType;
            return this;
        }

        public ParameterInfo.Builder setParameterTypeSignature(TypeSignature parameterTypeSignature)
        {
            this.parameterTypeSignature = parameterTypeSignature;
            return this;
        }

        public ParameterInfo.Builder setNullable(Nullable nullable)
        {
            this.nullable = nullable;
            return this;
        }

        public ParameterInfo.Builder setSigned(boolean signed)
        {
            this.signed = signed;
            return this;
        }

        public ParameterInfo.Builder setPrecision(int precision)
        {
            this.precision = precision;
            return this;
        }

        public ParameterInfo.Builder setScale(int scale)
        {
            this.scale = scale;
            return this;
        }

        public ParameterInfo build()
        {
            return new ParameterInfo(
                    position,
                    parameterType,
                    parameterTypeSignature,
                    nullable,
                    signed,
                    precision,
                    scale);
        }
    }
}
