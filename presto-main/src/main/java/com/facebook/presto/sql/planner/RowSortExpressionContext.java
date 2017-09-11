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
import com.facebook.presto.sql.tree.FieldReference;

import java.util.Objects;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkState;

public class RowSortExpressionContext
{
    private final int channel;

    public RowSortExpressionContext(int channel)
    {
        this.channel = channel;
    }

    public int getChannel()
    {
        return channel;
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
        RowSortExpressionContext other = (RowSortExpressionContext) obj;
        return Objects.equals(this.channel, other.channel);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(channel);
    }

    public String toString()
    {
        return toStringHelper(this)
                .add("channel", channel)
                .toString();
    }

    public static RowSortExpressionContext fromExpression(Expression expression)
    {
        checkState(expression instanceof FieldReference, "Unsupported expression type [%s]", expression);
        return new RowSortExpressionContext(((FieldReference) expression).getFieldIndex());
    }
}
