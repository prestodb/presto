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
package com.facebook.presto.verifier.resolver;

import com.facebook.presto.common.CatalogSchemaName;
import com.facebook.presto.sql.tree.AstVisitor;
import com.facebook.presto.sql.tree.FunctionCall;
import com.facebook.presto.sql.tree.Node;
import com.facebook.presto.sql.tree.QualifiedName;
import com.facebook.presto.verifier.framework.DataMatchResult;
import com.facebook.presto.verifier.framework.QueryBundle;
import com.google.common.base.Splitter;

import javax.inject.Inject;

import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import static com.facebook.presto.metadata.BuiltInTypeAndFunctionNamespaceManager.DEFAULT_NAMESPACE;
import static com.facebook.presto.verifier.framework.DataMatchResult.MatchType.COLUMN_MISMATCH;
import static com.facebook.presto.verifier.framework.DataMatchResult.MatchType.ROW_COUNT_MISMATCH;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;

public class IgnoredFunctionsMismatchResolver
        implements FailureResolver
{
    public static final String NAME = "ignored-functions";

    private final List<String> ignoredFunctions;

    @Inject
    public IgnoredFunctionsMismatchResolver(IgnoredFunctionsMismatchResolverConfig config)
    {
        this.ignoredFunctions = config.getFunctions().stream()
                .map(IgnoredFunctionsMismatchResolver::normalizeFunctionName)
                .collect(toImmutableList());
    }

    @Override
    public Optional<String> resolveResultMismatch(DataMatchResult matchResult, QueryBundle control)
    {
        checkArgument(!matchResult.isMatched(), "Expect not matched");
        if (matchResult.getMatchType() != COLUMN_MISMATCH && matchResult.getMatchType() != ROW_COUNT_MISMATCH) {
            return Optional.empty();
        }

        Set<String> ignoredFunctionsInQuery = new HashSet<>();
        new Visitor().process(control.getQuery(), ignoredFunctionsInQuery);
        return ignoredFunctionsInQuery.isEmpty() ? Optional.empty() : Optional.of("Query references ignored function: " + ignoredFunctionsInQuery);
    }

    private class Visitor
            extends AstVisitor<Void, Set<String>>
    {
        @Override
        protected Void visitNode(Node node, Set<String> context)
        {
            node.getChildren().forEach(child -> process(child, context));
            return null;
        }

        @Override
        protected Void visitFunctionCall(FunctionCall node, Set<String> ignoredFunctionsInQuery)
        {
            super.visitFunctionCall(node, ignoredFunctionsInQuery);
            String functionName = normalizeFunctionName(node.getName());
            if (ignoredFunctions.contains(functionName)) {
                ignoredFunctionsInQuery.add(functionName);
            }
            return null;
        }
    }

    private static String normalizeFunctionName(String name)
    {
        return normalizeFunctionName(QualifiedName.of(Splitter.on(".").splitToList(name)));
    }

    private static String normalizeFunctionName(QualifiedName name)
    {
        if (name.getParts().size() == 3 &&
                new CatalogSchemaName(name.getParts().get(0), name.getParts().get(1)).equals(DEFAULT_NAMESPACE)) {
            return name.getSuffix();
        }
        return name.toString();
    }
}
