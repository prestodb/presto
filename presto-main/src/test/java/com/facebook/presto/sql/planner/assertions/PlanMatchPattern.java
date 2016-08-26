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
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.planner.plan.ApplyNode;
import com.facebook.presto.sql.planner.plan.FilterNode;
import com.facebook.presto.sql.planner.plan.JoinNode;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.sql.planner.plan.ProjectNode;
import com.facebook.presto.sql.planner.plan.SemiJoinNode;
import com.facebook.presto.sql.planner.plan.TableScanNode;
import com.facebook.presto.sql.planner.plan.WindowNode;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.FunctionCall;
import com.facebook.presto.sql.tree.QualifiedName;
import com.facebook.presto.sql.tree.SymbolReference;
import com.facebook.presto.sql.tree.WindowFrame;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;

import static com.facebook.presto.util.ImmutableCollectors.toImmutableList;
import static com.google.common.base.Preconditions.checkState;
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
        return node(TableScanNode.class).with(new TableScanMatcher(expectedTableName));
    }

    public static PlanMatchPattern tableScan(String expectedTableName, Map<String, Domain> constraint)
    {
        return node(TableScanNode.class).with(new TableScanMatcher(expectedTableName, constraint));
    }

    public static PlanMatchPattern window(WindowNode.Specification specification, List<FunctionCall> functionCalls, PlanMatchPattern source)
    {
        return any(source).with(new WindowMatcher(specification, functionCalls));
    }

    public static PlanMatchPattern project(PlanMatchPattern source)
    {
        return node(ProjectNode.class, source);
    }

    public static PlanMatchPattern semiJoin(String sourceSymbolAlias, String filteringSymbolAlias, String outputAlias, PlanMatchPattern source, PlanMatchPattern filtering)
    {
        return node(SemiJoinNode.class, source, filtering).with(new SemiJoinMatcher(sourceSymbolAlias, filteringSymbolAlias, outputAlias));
    }

    public static PlanMatchPattern join(JoinNode.Type joinType, List<AliasPair> expectedEquiCriteria, PlanMatchPattern left, PlanMatchPattern right)
    {
        return node(JoinNode.class, left, right).with(new JoinMatcher(joinType, expectedEquiCriteria));
    }

    public static AliasPair aliasPair(String left, String right)
    {
        return new AliasPair(left, right);
    }

    public static PlanMatchPattern filter(String predicate, PlanMatchPattern source)
    {
        Expression expectedPredicate = new SqlParser().createExpression(predicate);
        return node(FilterNode.class, source).with(new FilterMatcher(expectedPredicate));
    }

    public static PlanMatchPattern apply(List<String> correlationSymbolAliases, PlanMatchPattern inputPattern, PlanMatchPattern subqueryPattern)
    {
        return node(ApplyNode.class, inputPattern, subqueryPattern).with(new CorrelationMatcher(correlationSymbolAliases));
    }

    public PlanMatchPattern(List<PlanMatchPattern> sourcePatterns)
    {
        requireNonNull(sourcePatterns, "sourcePatterns are null");

        this.sourcePatterns = ImmutableList.copyOf(sourcePatterns);
    }

    List<PlanMatchingState> matches(PlanNode node, Session session, Metadata metadata, ExpressionAliases expressionAliases)
    {
        ImmutableList.Builder<PlanMatchingState> states = ImmutableList.builder();
        if (anyTree) {
            int sourcesCount = node.getSources().size();
            if (sourcesCount > 1) {
                states.add(new PlanMatchingState(nCopies(sourcesCount, this), expressionAliases));
            }
            else {
                states.add(new PlanMatchingState(ImmutableList.of(this), expressionAliases));
            }
        }
        if (node.getSources().size() == sourcePatterns.size() && matchers.stream().allMatch(it -> it.matches(node, session, metadata, expressionAliases))) {
            states.add(new PlanMatchingState(sourcePatterns, expressionAliases));
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

    /*
     * Caveat Emptor: FunctionCall's come from the parser, and represent what
     * the user has typed. As such, they don't contain a WindowFrame if one
     * isn't specified in the SQL. In this case, the correct value to pass is
     * Optional.empty()
     */
    public static FunctionCall functionCall(String name, Optional<WindowFrame> frame, SymbolReference... args)
    {
        return new RelaxedEqualityFunctionCall(QualifiedName.of(name), Arrays.asList(args), frame);
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
        checkState(matchers.stream().filter(PlanNodeMatcher.class::isInstance).count() <= 1);

        builder.append(indentString(indent));
        if (anyTree) {
            builder.append("anyTree");
        }
        else {
            builder.append("node");
        }

        Optional<PlanNodeMatcher> planNodeMatcher = matchers.stream()
                .filter(PlanNodeMatcher.class::isInstance)
                .map(PlanNodeMatcher.class::cast)
                .findFirst();

        if (planNodeMatcher.isPresent()) {
            builder.append("(").append(planNodeMatcher.get().getNodeClass().getSimpleName()).append(")");
        }

        List<Matcher> matchersToPrint = matchers.stream()
                .filter(matcher -> !(matcher instanceof PlanNodeMatcher))
                .collect(toImmutableList());

        builder.append("\n");

        if (matchersToPrint.size() + sourcePatterns.size() == 0) {
            return;
        }

        for (Matcher matcher : matchersToPrint) {
            builder.append(indentString(indent + 1)).append(matcher.toString()).append("\n");
        }

        for (PlanMatchPattern pattern : sourcePatterns) {
            pattern.toString(builder, indent + 1);
        }
    }

    private String indentString(int indent)
    {
        return Strings.repeat("    ", indent);
    }

    public static SymbolStem symbolStem(String stem)
    {
        return new SymbolStem(stem);
    }

    /*
     * All comments below about Symbols apply to SymbolReferences as well.
     *
     * This is the result of a compromise to satisfy as many of the following
     * constraints as possible:
     *
     * 1) Many objects we want to compare in plans already define equals
     * methods. We should use these to the greatest degree possible in the
     * various Matchers, because there's no test-specific code that can get
     * out of date.
     * 2) Many of said objects contain either Symbols.
     * 3) Symbols in the final Plan may be decorated by the SymbolAllocator to
     * make them unique as needed.
     * 4) There's no way to know a-priori what the decorated name of a Symbol
     * will be in the plan. Even if we could, future changes to the planner
     * might change the name of the Symbols in the plan, rendering the expected
     * values in tests that depended on them out of date.
     *
     * The solution to writing expected plans that match actual plans and are
     * as self-maintaining as possible is to extend Symbol so that the
     * following returns true:
     *   expectedSymbol.equals(actualDecoratedSymbol)
     *
     * This ensures that we can construct expected objects containing Symbols
     * and use their built-in equals() methods to compare them to the actual
     * objects in the plan.
     *
     * To actually implement this, we need to handle comparing the equality
     * comparison between e.g. column_name and column_name_829. This is done by
     * treating column_name as a stem and making sure that everything prior to
     * the last underscore in column_name_829 is the same as the stem.
     *
     * This basically reverses the transformation SymbolAllocator does to
     * create the unique column_name_829 Symbol name in the first place.
     *
     * A different approach to this that is problematic is to have a
     * TestSymbolAllocator that keeps track of the reverse mappings in a Map
     * having entries such as "column_name_927" -> "column_name". The issue
     * with this is that constructing the expected plan for a query would have
     * to depend on the actual plan, which is what actually has the
     * SymbolAllocator. Having the expected value for a test depend on the
     * actual value seems fishy.
     */
    private interface Stem<T>
    {
        String getStem();
        String getOtherName(T other);

        default boolean stemEquals(Object o, Class<T> clazz, Function<T, String> getName)
        {
            if (this == o) {
                return true;
            }

            if (o == null) {
                return false;
            }

            if (!clazz.equals(o.getClass())) {
                return false;
            }

            T other = clazz.cast(o);
            String name = getOtherName(other);
            int endIndex = name.lastIndexOf('_');
            String otherStem = endIndex == -1 ? name : name.substring(0, endIndex);
            return getStem().equals(otherStem);
        }
    }

    private static class SymbolStem extends Symbol
        implements Stem<Symbol>
    {
        private final String stem;

        private SymbolStem(String stem)
        {
            super(stem + "_*");
            this.stem = requireNonNull(stem);
        }

        @Override
        public boolean equals(Object o)
        {
            // Inconsistent with hashCode. See below.
            return stemEquals(o, Symbol.class, Symbol::getName);
        }

        @Override
        public int hashCode()
        {
            /*
             * hashCode() and equals() are inconsistent with each other. In theory, we'd like to
             * throw UnsupportedOperationException here. We can't, however because creating a
             * WindowNode.Specification with more than one Ordering requires being able to put
             * Symbols in a hash table.
             *
             * It turns out that this being inconsistent with equals ends up not mattering. We
             * can't meaningfully do an equals comparison on the hash tables representing the
             * expected Orderings and the actual Orderings because that would require knowing
             * the mangled symbols in the expected value a priori. Instead, we do an equals
             * comparison that relies solely on equals() returning true for mangled and
             * non-mangled symbols
             */
            return stem.hashCode();
        }

        @Override
        public String getStem()
        {
            return stem;
        }

        @Override
        public String getOtherName(Symbol other)
        {
            return other.getName();
        }
    }

    public static SymbolReference anySymbolReference()
    {
        return new AnySymbolReference();
    }

    public static SymbolReference symbolReferenceStem(String stem)
    {
        return new SymbolReferenceStem(stem);
    }

    private static class AnySymbolReference extends SymbolReference
    {
        private AnySymbolReference()
        {
            super("*");
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) {
                return true;
            }

            if (o == null || !SymbolReference.class.equals(o.getClass())) {
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

    private static class SymbolReferenceStem extends SymbolReference
            implements Stem<SymbolReference>
    {
        private final String stem;

        private SymbolReferenceStem(String stem)
        {
            super(stem + "_*");
            this.stem = requireNonNull(stem);
        }

        @Override
        public boolean equals(Object o)
        {
            return stemEquals(o, SymbolReference.class, SymbolReference::getName);
        }

        @Override
        public int hashCode()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public String getStem()
        {
            return stem;
        }

        @Override
        public String getOtherName(SymbolReference other)
        {
            return other.getName();
        }
    }

    private static class RelaxedEqualityFunctionCall extends FunctionCall
    {
        /*
         * FunctionCalls for window functions must have a Window. By the time
         * the we've gone through the optimizers, everything but the Frame
         * has a corresponding entry in the WindowNode. Since the WindowNode
         * has had default entries filled in, we only need to compare the
         * window frame with the one in the FunctionCall in equals.
         */
        Optional<WindowFrame> frame;

        private RelaxedEqualityFunctionCall(QualifiedName name, List<Expression> arguments, Optional<WindowFrame> frame)
        {
            super(name, arguments);
            this.frame = requireNonNull(frame);
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

            /*
             * A FunctionCall representing a window function must have a
             * window. If not, there's a failure somewhere upstream.
             */
            checkState(o.getWindow().isPresent());

            return Objects.equals(getName(), o.getName()) &&
                    Objects.equals(getArguments(), o.getArguments()) &&
                    Objects.equals(frame, o.getWindow().get().getFrame());
        }

        @Override
        public int hashCode()
        {
            // This is a test object. We shouldn't put it in a hash table.
            throw new UnsupportedOperationException();
        }
    }
}
