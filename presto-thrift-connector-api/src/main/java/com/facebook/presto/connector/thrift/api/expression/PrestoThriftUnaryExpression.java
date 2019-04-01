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
package com.facebook.presto.connector.thrift.api.expression;

import com.facebook.presto.connector.thrift.api.PrestoThriftDomain;
import com.facebook.presto.spi.predicate.SpiUnaryExpression;
import com.facebook.presto.spi.type.BigintType;
import com.facebook.presto.spi.type.BooleanType;
import com.facebook.presto.spi.type.DateType;
import com.facebook.presto.spi.type.DoubleType;
import com.facebook.presto.spi.type.IntegerType;
import com.facebook.presto.spi.type.TimestampType;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeSignature;
import com.facebook.presto.spi.type.VarcharType;
import io.airlift.drift.annotations.ThriftConstructor;
import io.airlift.drift.annotations.ThriftField;
import io.airlift.drift.annotations.ThriftStruct;

import java.util.Objects;

import static com.facebook.presto.spi.type.StandardTypes.BIGINT;
import static com.facebook.presto.spi.type.StandardTypes.BOOLEAN;
import static com.facebook.presto.spi.type.StandardTypes.DATE;
import static com.facebook.presto.spi.type.StandardTypes.DOUBLE;
import static com.facebook.presto.spi.type.StandardTypes.INTEGER;
import static com.facebook.presto.spi.type.StandardTypes.TIMESTAMP;
import static com.facebook.presto.spi.type.StandardTypes.VARCHAR;
import static com.google.common.base.MoreObjects.toStringHelper;
import static io.airlift.drift.annotations.ThriftField.Requiredness.REQUIRED;

@ThriftStruct
public final class PrestoThriftUnaryExpression
{
    private final String column;
    private final String type;
    private final PrestoThriftDomain domain;

    @ThriftConstructor
    public PrestoThriftUnaryExpression(String column, String type, PrestoThriftDomain domain)
    {
        this.column = column;
        this.type = type;
        this.domain = domain;
    }

    @ThriftField(value = 1, requiredness = REQUIRED)
    public String getColumn()
    {
        return column;
    }

    @ThriftField(value = 2, requiredness = REQUIRED)
    public String getType()
    {
        return type;
    }

    @ThriftField(value = 3, requiredness = REQUIRED)
    public PrestoThriftDomain getDomain()
    {
        return domain;
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        PrestoThriftUnaryExpression other = (PrestoThriftUnaryExpression) obj;
        return Objects.equals(this.column, other.column) &&
                Objects.equals(this.domain, other.domain);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(column, domain);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("column", column)
                .add("valueSet", domain)
                .toString();
    }

    public static SpiUnaryExpression toSpiUnaryExpression(PrestoThriftUnaryExpression expression)
    {
        Type type = getType(expression.getType());
        return new SpiUnaryExpression(expression.getColumn(), PrestoThriftDomain.toDomain(expression.getDomain(), type));
    }

    public static Type getType(String stringType)
    {
        TypeSignature signature = TypeSignature.parseTypeSignature(stringType);
        switch (signature.getBase()) {
            case INTEGER:
                return IntegerType.INTEGER;
            case BIGINT:
                return BigintType.BIGINT;
            case DOUBLE:
                return DoubleType.DOUBLE;
            case VARCHAR:
                return VarcharType.VARCHAR;
            case BOOLEAN:
                return BooleanType.BOOLEAN;
            case DATE:
                return DateType.DATE;
            case TIMESTAMP:
                return TimestampType.TIMESTAMP;
            default:
                throw new IllegalArgumentException("Unsupported type: " + signature.getBase() + " found in PrestoThriftUnaryExpression");
        }
    }
}
