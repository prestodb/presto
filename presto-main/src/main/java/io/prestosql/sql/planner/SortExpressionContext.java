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
package io.prestosql.sql.planner;

import com.google.common.collect.ImmutableList;
import io.prestosql.sql.tree.Expression;

import java.util.List;
import java.util.Objects;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class SortExpressionContext
{
    private final Expression sortExpression;
    private final List<Expression> searchExpressions;

    public SortExpressionContext(Expression sortExpression, List<Expression> searchExpressions)
    {
        this.sortExpression = requireNonNull(sortExpression, "sortExpression can not be null");
        this.searchExpressions = ImmutableList.copyOf(searchExpressions);
    }

    public Expression getSortExpression()
    {
        return sortExpression;
    }

    public List<Expression> getSearchExpressions()
    {
        return searchExpressions;
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
                Objects.equals(searchExpressions, that.searchExpressions);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(sortExpression, searchExpressions);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("sortExpression", sortExpression)
                .add("searchExpressions", searchExpressions)
                .toString();
    }
}
