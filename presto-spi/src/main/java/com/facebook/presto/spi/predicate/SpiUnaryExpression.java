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
package com.facebook.presto.spi.predicate;

import com.facebook.presto.spi.ConnectorSession;

import java.util.Objects;

import static java.util.Objects.requireNonNull;

/**
 * Defines a unary expression.
 * It is of the form column IN domain.
 * Unary expressions currently do not support un-orderable types like JSON, Array and HyperLogLog
 * It also do not support EquatablValueSet and AllOrNoneValueSet.
 */
public class SpiUnaryExpression
        implements SpiExpression
{
    private final String column;
    private final Domain domain;

    public SpiUnaryExpression(String column, Domain domain)
    {
        this.column = requireNonNull(column, "column is null");
        this.domain = requireNonNull(domain, "domain is null");
    }

    public String getColumn()
    {
        return column;
    }
    public Domain getDomain()
    {
        return domain;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(column, domain);
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
        SpiUnaryExpression other = (SpiUnaryExpression) obj;
        return Objects.equals(this.domain, other.domain) &&
                Objects.equals(this.column, other.column);
    }

    @Override
    public String toString(ConnectorSession session)
    {
        return "[ " + column + domain.toString(session) + " ]";
    }
}
