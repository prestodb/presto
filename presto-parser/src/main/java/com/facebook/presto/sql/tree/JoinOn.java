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

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class JoinOn
        extends JoinCriteria
{
    private final Expression expression;

    public JoinOn(Expression expression)
    {
        this.expression = requireNonNull(expression, "expression is null");
    }

    public Expression getExpression()
    {
        return expression;
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        if ((obj == null) || (getClass() != obj.getClass())) {
            return false;
        }
        JoinOn o = (JoinOn) obj;
        return Objects.equals(expression, o.expression);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(expression);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .addValue(expression)
                .toString();
    }

    @Override
    public List<Node> getNodes()
    {
        return ImmutableList.of(expression);
    }
}
