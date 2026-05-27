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

package com.facebook.presto.iceberg.optimizer.derivedColumns;

import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;

import java.util.List;
import java.util.Objects;

// TODO (prashant) Convert this to record class when checkstyle supports java 17.
public final class RewrittenExpressionMetadata
{
    private final RowExpression rewrittenExpression;
    private final List<VariableReferenceExpression> derivedColumnsAdded;

    public RewrittenExpressionMetadata(RowExpression rewrittenExpression, List<VariableReferenceExpression> derivedColumnsAdded)
    {
        this.rewrittenExpression = rewrittenExpression;
        this.derivedColumnsAdded = derivedColumnsAdded;
    }

    public RowExpression rewrittenExpression()
    {
        return rewrittenExpression;
    }

    public List<VariableReferenceExpression> derivedColumnsAdded()
    {
        return derivedColumnsAdded;
    }

    @Override
    public boolean equals(Object obj)
    {
        if (obj == this) {
            return true;
        }
        if (obj == null || obj.getClass() != this.getClass()) {
            return false;
        }
        var that = (RewrittenExpressionMetadata) obj;
        return Objects.equals(this.rewrittenExpression, that.rewrittenExpression) &&
                Objects.equals(this.derivedColumnsAdded, that.derivedColumnsAdded);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(rewrittenExpression, derivedColumnsAdded);
    }

    @Override
    public String toString()
    {
        return "RewrittenExpressionMetadata[" +
                "rewrittenExpression=" + rewrittenExpression + ", " +
                "derivedColumnsAdded=" + derivedColumnsAdded + ']';
    }
}
