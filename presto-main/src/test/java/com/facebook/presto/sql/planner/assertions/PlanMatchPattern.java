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
package com.facebook.presto.sql.planner.assertions;

import com.facebook.presto.Session;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.spi.predicate.Domain;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.planner.plan.JoinNode;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.sql.planner.plan.ProjectNode;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.FunctionCall;
import com.facebook.presto.sql.tree.QualifiedName;
import com.facebook.presto.sql.tree.SymbolReference;
import com.facebook.presto.sql.tree.Window;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import static java.util.Collections.nCopies;
import static java.util.Objects.requireNonNull;

public final class PlanMatchPattern
{
    private final List<Matcher> matchers = new ArrayList<>();

    private final List<PlanMatchPattern> sourcePatterns;
    private boolean anyTree;

    public static PlanMatchPattern node(Class<? extends PlanNode> nodeClass, PlanMatchPattern... sources)
    {
        return any(sources).with(new PlanNodeMatcher(nodeClass));
    }

    public static PlanMatchPattern any(PlanMatchPattern... sources)
    {
        return new PlanMatchPattern(ImmutableList.copyOf(sources));
    }

    /**
     * Matches to any tree of nodes with children matching to given source matchers.
     * anyNodeTree(tableScanNode("nation")) - will match to any plan which all leafs contain
     * any node containing table scan from nation table.
     */
    public static PlanMatchPattern anyTree(PlanMatchPattern... sources)
    {
        return any(sources).matchToAnyNodeTree();
    }

    public static PlanMatchPattern anyNot(Class<? extends PlanNode> excludeNodeClass, PlanMatchPattern... sources)
    {
        return any(sources).with(new NotPlanNodeMatcher(excludeNodeClass));
    }

    public static PlanMatchPattern tableScan(String expectedTableName)
    {
        return any().with(new TableScanMatcher(expectedTableName));
    }

    public static PlanMatchPattern tableScan(String expectedTableName, Map<String, Domain> constraint)
    {
        return any().with(new TableScanMatcher(expectedTableName, constraint));
    }

    public static PlanMatchPattern window(List<FunctionCall> functionCalls, PlanMatchPattern... sources)
    {
        return any(sources).with(new WindowMatcher(functionCalls));
    }

    public static PlanMatchPattern project(PlanMatchPattern... sources)
    {
        return node(ProjectNode.class, sources);
    }

    public static PlanMatchPattern semiJoin(String sourceSymbolAlias, String filteringSymbolAlias, String outputAlias, PlanMatchPattern... sources)
    {
        return any(sources).with(new SemiJoinMatcher(sourceSymbolAlias, filteringSymbolAlias, outputAlias));
    }

    public static PlanMatchPattern join(JoinNode.Type joinType, List<AliasPair> expectedEquiCriteria, PlanMatchPattern... sources)
    {
        return any(sources).with(new JoinMatcher(joinType,  expectedEquiCriteria));
    }

    public static AliasPair aliasPair(String left, String right)
    {
        return new AliasPair(left, right);
    }

    public static PlanMatchPattern filter(String predicate, PlanMatchPattern... sources)
    {
        Expression expectedPredicate = new SqlParser().createExpression(predicate);
        return any(sources).with(new FilterMatcher(expectedPredicate));
    }

    public PlanMatchPattern(List<PlanMatchPattern> sourcePatterns)
    {
        requireNonNull(sourcePatterns, "sourcePatterns are null");

        this.sourcePatterns = ImmutableList.copyOf(sourcePatterns);
    }

    List<PlanMatchingState> matches(PlanNode node, Session session, Metadata metadata, SymbolAliases symbolAliases)
    {
        ImmutableList.Builder<PlanMatchingState> states = ImmutableList.builder();
        if (anyTree) {
            int sourcesCount = node.getSources().size();
            if (sourcesCount > 1) {
                states.add(new PlanMatchingState(nCopies(sourcesCount, this), symbolAliases));
            }
            else {
                states.add(new PlanMatchingState(ImmutableList.of(this), symbolAliases));
            }
        }
        if (node.getSources().size() == sourcePatterns.size() && matchers.stream().allMatch(it -> it.matches(node, session, metadata, symbolAliases))) {
            states.add(new PlanMatchingState(sourcePatterns, symbolAliases));
        }
        return states.build();
    }

    public PlanMatchPattern withSymbol(String pattern, String alias)
    {
        return with(new SymbolMatcher(pattern, alias));
    }

    public PlanMatchPattern with(Matcher matcher)
    {
        matchers.add(matcher);
        return this;
    }

    public PlanMatchPattern matchToAnyNodeTree()
    {
        anyTree = true;
        return this;
    }

    public boolean isTerminated()
    {
        return sourcePatterns.isEmpty();
    }

    private static List<Expression> toExpressionList(String ... args)
    {
        ImmutableList.Builder<Expression> builder = ImmutableList.builder();
        for (String arg : args) {
            if (arg.equals("*")) {
                builder.add(new PlanMatchPattern.AnySymbolReference());
            }
            else {
                builder.add(new SymbolReference(arg));
            }
        }

        return builder.build();
    }

    public static FunctionCall functionCall(String name, Window window, boolean distinct, String... args)
    {
        return new FunctionCall(QualifiedName.of(name), Optional.of(window), distinct, toExpressionList(args));
    }

    public static FunctionCall functionCall(String name, String ... args)
    {
        return new RelaxedEqualityFunctionCall(QualifiedName.of(name), toExpressionList(args));
    }

    @Override
    public String toString()
    {
        StringBuilder builder = new StringBuilder();
        toString(builder, 0);
        return builder.toString();
    }

    private void toString(StringBuilder builder, int indent)
    {
        builder.append(indentString(indent));
        if (anyTree) {
            builder.append("anyTree ");
        }
        builder.append("PlanMatchPattern {\n");

        for (Matcher matcher : matchers) {
            builder.append(indentString(indent + 1)).append(matcher.toString()).append("\n");
        }

        for (PlanMatchPattern pattern : sourcePatterns) {
            pattern.toString(builder, indent + 1);
        }

        builder.append(indentString(indent)).append("}\n");
    }

    private String indentString(int indent)
    {
        return Strings.repeat("    ", indent);
    }

    private static class AnySymbolReference extends SymbolReference
    {
        AnySymbolReference()
        {
            super("*");
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) {
                return true;
            }

            if (o == null || !SymbolReference.class.isInstance(o)) {
                return false;
            }

            return true;
        }

        @Override
        public int hashCode()
        {
            throw new UnsupportedOperationException();
        }
    }

    private static class RelaxedEqualityFunctionCall extends FunctionCall
    {
        RelaxedEqualityFunctionCall(QualifiedName name, List<Expression> arguments)
        {
            super(name, arguments);
        }

        @Override
        public boolean equals(Object obj)
        {
            if (this == obj) {
                return true;
            }
            if (obj == null || obj.getClass() != FunctionCall.class) {
                return false;
            }
            FunctionCall o = (FunctionCall) obj;
            return Objects.equals(getName(), o.getName()) &&
                    Objects.equals(getArguments(), o.getArguments());
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(getName(), getArguments());
        }
    }
}
