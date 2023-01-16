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
package com.facebook.presto.sql.jsonpath.tree;

import com.facebook.presto.sql.tree.StringLiteral;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class StartsWithPredicate
        extends Predicate
{
    private final PathNode whole;
    private final PathNode initial;

    public StartsWithPredicate(PathNode whole, PathNode initial)
    {
        requireNonNull(whole, "whole is null");
        requireNonNull(initial, "initial is null");
        checkArgument(initial instanceof NamedVariable || (initial instanceof SqlValueLiteral && ((SqlValueLiteral) initial).getValue() instanceof StringLiteral), "initial must be a named variable or a string literal");

        this.whole = whole;
        this.initial = initial;
    }

    @Override
    public <R, C> R accept(JsonPathTreeVisitor<R, C> visitor, C context)
    {
        return visitor.visitStartsWithPredicate(this, context);
    }

    public PathNode getWhole()
    {
        return whole;
    }

    public PathNode getInitial()
    {
        return initial;
    }
}
