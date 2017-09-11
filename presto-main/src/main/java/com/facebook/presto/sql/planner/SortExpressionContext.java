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
package com.facebook.presto.sql.planner;

import com.facebook.presto.sql.tree.Expression;

import java.util.Objects;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class SortExpressionContext
{
    private final Expression sortExpression;
    private final Expression searchExpression;

    public SortExpressionContext(Expression sortExpression, Expression searchExpression)
    {
        this.sortExpression = requireNonNull(sortExpression, "sortExpression can not be null");
        this.searchExpression = requireNonNull(searchExpression, "searchExpression can not be null");
    }

    public Expression getSortExpression()
    {
        return sortExpression;
    }

    public Expression getSearchExpression()
    {
        return searchExpression;
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
        SortExpressionContext that = (SortExpressionContext) o;
        return Objects.equals(sortExpression, that.sortExpression) &&
                Objects.equals(searchExpression, that.searchExpression);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(sortExpression, searchExpression);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("sortExpression", sortExpression)
                .add("searchExpression", searchExpression)
                .toString();
    }
}
