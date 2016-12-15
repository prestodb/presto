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

import com.facebook.presto.sql.tree.Node;

import java.util.Optional;
import java.util.function.Predicate;

import static com.facebook.presto.util.Predicates.alwaysTrue;
import static java.util.Objects.requireNonNull;

/**
 * See its twin {@link com.facebook.presto.sql.planner.optimizations.PlanNodeSearcher}
 */
public final class AstNodeSearcher
{
    public static AstNodeSearcher searchFrom(Node node)
    {
        return new AstNodeSearcher(node);
    }

    private final Node node;
    private Predicate<Node> where = alwaysTrue();

    public AstNodeSearcher(Node node)
    {
        this.node = node;
    }

    public AstNodeSearcher where(Predicate<Node> where)
    {
        this.where = requireNonNull(where, "where is null");
        return this;
    }

    public boolean matches()
    {
        return findFirst().isPresent();
    }

    public <T extends Node> Optional<T> findFirst()
    {
        return findFirstRecursive(node);
    }

    private <T extends Node> Optional<T> findFirstRecursive(Node node)
    {
        if (where.test(node)) {
            return Optional.of((T) node);
        }
        for (Node child : node.getNodes()) {
            Optional<T> found = findFirstRecursive(child);
            if (found.isPresent()) {
                return found;
            }
        }
        return Optional.empty();
    }
}
