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

import static java.util.Objects.requireNonNull;

public class DisjunctionPredicate
        extends Predicate
{
    private final Predicate left;
    private final Predicate right;

    public DisjunctionPredicate(Predicate left, Predicate right)
    {
        this.left = requireNonNull(left, "left is null");
        this.right = requireNonNull(right, "right is null");
    }

    @Override
    public <R, C> R accept(JsonPathTreeVisitor<R, C> visitor, C context)
    {
        return visitor.visitDisjunctionPredicate(this, context);
    }

    public Predicate getLeft()
    {
        return left;
    }

    public Predicate getRight()
    {
        return right;
    }
}
