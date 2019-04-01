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

import com.facebook.presto.spi.predicate.SpiExpression;
import io.airlift.drift.annotations.ThriftConstructor;
import io.airlift.drift.annotations.ThriftField;
import io.airlift.drift.annotations.ThriftStruct;

import javax.annotation.Nullable;

import java.util.Objects;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static io.airlift.drift.annotations.ThriftField.Requiredness.OPTIONAL;

@ThriftStruct
public final class PrestoThriftExpression
{
    private final PrestoThriftUnaryExpression unaryExpression;

    @ThriftConstructor
    public PrestoThriftExpression(@Nullable PrestoThriftUnaryExpression unaryExpression)
    {
        checkArgument(isExactlyOneNonNull(unaryExpression, null), "exactly one value set must be present");
        this.unaryExpression = unaryExpression;
    }

    @Nullable
    @ThriftField(value = 1, requiredness = OPTIONAL)
    public PrestoThriftUnaryExpression getUnaryExpression()
    {
        return unaryExpression;
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
        PrestoThriftExpression other = (PrestoThriftExpression) obj;
        return Objects.equals(this.unaryExpression, other.unaryExpression);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(unaryExpression);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("expression", firstNonNull(unaryExpression, null))
                .toString();
    }

    public static SpiExpression toSpiExpression(PrestoThriftExpression expression)
    {
        if (expression.getUnaryExpression() != null) {
            return PrestoThriftUnaryExpression.toSpiUnaryExpression(expression.getUnaryExpression());
        }
        else {
            throw new IllegalArgumentException("Unknown implementation of a Expression: " + expression.getClass());
        }
    }

    private static boolean isExactlyOneNonNull(Object a, Object b)
    {
        return a != null && b == null || a == null && b != null;
    }

    private static Object firstNonNull(Object a, Object b)
    {
        if (a != null) {
            return a;
        }
        if (b != null) {
            return b;
        }
        throw new IllegalArgumentException("All arguments are null");
    }
}
