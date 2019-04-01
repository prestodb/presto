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
package com.facebook.presto.connector.thrift.api;

import com.facebook.presto.connector.thrift.api.expression.PrestoThriftExpression;
import com.facebook.presto.spi.security.RowLevelSecurityResponse;
import io.airlift.drift.annotations.ThriftConstructor;
import io.airlift.drift.annotations.ThriftField;
import io.airlift.drift.annotations.ThriftStruct;

import java.util.Objects;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static io.airlift.drift.annotations.ThriftField.Requiredness.REQUIRED;

@ThriftStruct
public final class PrestoThriftRowLevelSecurityResponse
{
    private final PrestoThriftExpression expression;
    private final String warning;

    @ThriftConstructor
    public PrestoThriftRowLevelSecurityResponse(PrestoThriftExpression expression, String warning)
    {
        checkArgument(expression != null, "expression is null");
        checkArgument(warning != null, "warning is null");
        this.expression = expression;
        this.warning = warning;
    }

    @ThriftField(value = 1, requiredness = REQUIRED)
    public PrestoThriftExpression getExpression()
    {
        return expression;
    }

    @ThriftField(value = 2, requiredness = REQUIRED)
    public String getWarning()
    {
        return warning;
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
        PrestoThriftRowLevelSecurityResponse other = (PrestoThriftRowLevelSecurityResponse) obj;
        return Objects.equals(this.expression, other.expression) && Objects.equals(this.warning, other.warning);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(expression, warning);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("expression", expression)
                .add("warning", warning)
                .toString();
    }

    public static RowLevelSecurityResponse toRowLevelSecurityResponse(PrestoThriftRowLevelSecurityResponse rowLevelSecurityResponse)
    {
        return new RowLevelSecurityResponse(PrestoThriftExpression.toSpiExpression(rowLevelSecurityResponse.getExpression()), rowLevelSecurityResponse.getWarning());
    }
}
