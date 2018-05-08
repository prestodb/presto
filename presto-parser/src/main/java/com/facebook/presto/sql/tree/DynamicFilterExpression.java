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
package com.facebook.presto.sql.tree;

import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class DynamicFilterExpression
        extends Expression
{
    private final String sourceId;
    private final Expression probeExpression;
    private final String dynamicFilterSymbol;

    public DynamicFilterExpression(String sourceId, Expression probeExpression, String dynamicFilterSymbol)
    {
        super(Optional.empty());
        this.sourceId = requireNonNull(sourceId, "sourceId is null");
        this.probeExpression = requireNonNull(probeExpression, "probeExpression is null");
        this.dynamicFilterSymbol = requireNonNull(dynamicFilterSymbol, "dynamicFilterSymbol is null");
    }

    public String getSourceId()
    {
        return sourceId;
    }

    public String getDynamicFilterSymbol()
    {
        return dynamicFilterSymbol;
    }

    public Expression getProbeExpression()
    {
        return probeExpression;
    }

    @Override
    protected <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitDynamicFilterExpression(this, context);
    }

    @Override
    public List<? extends Node> getChildren()
    {
        return ImmutableList.of(probeExpression);
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        DynamicFilterExpression dfExpression = (DynamicFilterExpression) o;
        return Objects.equals(sourceId, dfExpression.sourceId) &&
                Objects.equals(probeExpression, dfExpression.probeExpression) &&
                Objects.equals(dynamicFilterSymbol, dfExpression.dynamicFilterSymbol);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(sourceId, probeExpression, dynamicFilterSymbol);
    }
}
