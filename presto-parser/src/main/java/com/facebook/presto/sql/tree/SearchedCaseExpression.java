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

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import java.util.List;

public class SearchedCaseExpression
        extends Expression
{
    private final List<WhenClause> whenClauses;
    private final Expression defaultValue;

    public SearchedCaseExpression(List<WhenClause> whenClauses, Expression defaultValue)
    {
        Preconditions.checkNotNull(whenClauses, "whenClauses is null");
        this.whenClauses = ImmutableList.copyOf(whenClauses);
        this.defaultValue = defaultValue;
    }

    public List<WhenClause> getWhenClauses()
    {
        return whenClauses;
    }

    public Expression getDefaultValue()
    {
        return defaultValue;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitSearchedCaseExpression(this, context);
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

        SearchedCaseExpression that = (SearchedCaseExpression) o;

        if (defaultValue != null ? !defaultValue.equals(that.defaultValue) : that.defaultValue != null) {
            return false;
        }
        if (!whenClauses.equals(that.whenClauses)) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode()
    {
        int result = whenClauses.hashCode();
        result = 31 * result + (defaultValue != null ? defaultValue.hashCode() : 0);
        return result;
    }
}
