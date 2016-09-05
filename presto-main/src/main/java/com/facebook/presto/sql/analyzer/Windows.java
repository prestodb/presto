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
package com.facebook.presto.sql.analyzer;

import com.facebook.presto.sql.tree.AstVisitor;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.Node;
import com.facebook.presto.sql.tree.SortItem;
import com.facebook.presto.sql.tree.Window;
import com.facebook.presto.sql.tree.WindowDefinition;
import com.facebook.presto.sql.tree.WindowFrame;
import com.facebook.presto.sql.tree.WindowInline;
import com.facebook.presto.sql.tree.WindowName;
import com.facebook.presto.sql.tree.WindowSpecification;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;

import static com.facebook.presto.sql.analyzer.SemanticErrorCode.INVALID_WINDOW_SPECIFICATION;
import static com.google.common.base.Preconditions.checkArgument;

final class Windows
{
    private Windows()
    {
    }

    static Map<String, WindowSpecification> createWindowSpecificationMap(Node node, List<WindowDefinition> windowDefinitions, Function<String, Optional<WindowSpecification>> windowSpecificationLookup)
    {
        Map<String, WindowSpecification> windowSpecifications = new HashMap<>();
        for (WindowDefinition def : windowDefinitions) {
            if (windowSpecifications.containsKey(def.getName())) {
                throw new SemanticException(INVALID_WINDOW_SPECIFICATION, node, "Duplicate window definition '%s'", def.getName());
            }
            WindowSpecification spec = def.getSpecification();
            if (spec.getExistingName().isPresent()) {
                Optional<WindowSpecification> existing = Optional.ofNullable(windowSpecifications.get(spec.getExistingName().get()));
                if (!existing.isPresent()) {
                    existing = windowSpecificationLookup.apply(spec.getExistingName().get());
                    if (!existing.isPresent()) {
                        throw new SemanticException(INVALID_WINDOW_SPECIFICATION, node, "Existing window definition '%s' not found for '%s'", spec.getExistingName().get(), def.getName());
                    }
                }
                spec = resolveWindowReference(node, spec, existing.get());
            }
            windowSpecifications.put(def.getName(), spec);
        }
        return ImmutableMap.copyOf(windowSpecifications);
    }

    private static WindowSpecification resolveWindowReference(Node node, WindowSpecification referrer, WindowSpecification referent)
    {
        checkArgument(referrer.getExistingName().isPresent(), "Window specification does not contain reference");

        // These rules are arcane and inconsistent, see section 7.11 of the SQL 2008 spec for details.
        // Alternatively, look at transformWindowDefinitions in backend/parser/parse_clause.c in pg.

        if (referent.getExistingName().isPresent()) {
            throw new SemanticException(INVALID_WINDOW_SPECIFICATION, node, "Cannot chain window specification references");
        }

        if (!referrer.getPartitionBy().isEmpty()) {
            throw new SemanticException(INVALID_WINDOW_SPECIFICATION, node, "Referrer window specification must not contain partition clauses");
        }
        List<Expression> partitionBy = ImmutableList.copyOf(referent.getPartitionBy());

        List<SortItem> orderBy;
        if (referrer.getOrderBy().isEmpty()) {
            orderBy = ImmutableList.copyOf(referent.getOrderBy());
        }
        else {
            if (!referent.getOrderBy().isEmpty()) {
                throw new SemanticException(INVALID_WINDOW_SPECIFICATION, node, "Referrer window specification must not specify orderBy when referent also specifies orderBy");
            }
            orderBy = ImmutableList.copyOf(referrer.getOrderBy());
        }

        if (referent.getFrame().isPresent()) {
            throw new SemanticException(INVALID_WINDOW_SPECIFICATION, node, "Referent window specification must not specify frame");
        }
        Optional<WindowFrame> frame = referrer.getFrame();

        return new WindowSpecification(
                Optional.empty(),
                partitionBy,
                orderBy,
                frame);
    }

    static WindowSpecification resolveWindowSpecification(Window node, Function<String, Optional<WindowSpecification>> windowSpecificationLookup)
    {
        return node.accept(new AstVisitor<WindowSpecification, Void>()
        {
            @Override
            protected WindowSpecification visitWindowName(WindowName node, Void context)
            {
                Optional<WindowSpecification> windowSpecification = windowSpecificationLookup.apply(node.getName());
                if (!windowSpecification.isPresent()) {
                    throw new SemanticException(INVALID_WINDOW_SPECIFICATION, node, "Named window specification not found: %s", node.getName());
                }
                return windowSpecification.get();
            }

            @Override
            protected WindowSpecification visitWindowInline(WindowInline node, Void context)
            {
                if (node.getSpecification().getExistingName().isPresent()) {
                    Optional<WindowSpecification> existing = windowSpecificationLookup.apply(node.getSpecification().getExistingName().get());
                    if (!existing.isPresent()) {
                        throw new SemanticException(INVALID_WINDOW_SPECIFICATION, node, "Named window specification not found: %s", node.getSpecification().getExistingName().get());
                    }
                    return resolveWindowReference(node, node.getSpecification(), existing.get());
                }
                else {
                    return node.getSpecification();
                }
            }
        }, null);
    }
}
