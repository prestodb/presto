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
package com.facebook.presto.verifier.rewrite;

import com.facebook.presto.common.type.TypeManager;
import com.facebook.presto.metadata.FunctionAndTypeManager;
import com.facebook.presto.spi.function.FunctionKind;
import com.facebook.presto.spi.function.Signature;
import com.facebook.presto.spi.function.SqlFunction;
import com.facebook.presto.sql.ExpressionFormatter;
import com.facebook.presto.sql.parser.ParsingException;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.tree.ArrayConstructor;
import com.facebook.presto.sql.tree.CurrentTime;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.ExpressionRewriter;
import com.facebook.presto.sql.tree.ExpressionTreeRewriter;
import com.facebook.presto.sql.tree.FunctionCall;
import com.facebook.presto.sql.tree.Identifier;
import com.facebook.presto.sql.tree.IfExpression;
import com.facebook.presto.sql.tree.Literal;
import com.facebook.presto.sql.tree.Node;
import com.facebook.presto.sql.tree.OrderBy;
import com.facebook.presto.sql.tree.Query;
import com.facebook.presto.sql.tree.SearchedCaseExpression;
import com.facebook.presto.sql.tree.SimpleCaseExpression;
import com.facebook.presto.sql.tree.SortItem;
import com.facebook.presto.sql.tree.Statement;
import com.facebook.presto.sql.tree.SubqueryExpression;
import com.facebook.presto.sql.tree.Window;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.Multimap;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.facebook.presto.verifier.framework.VerifierUtil.PARSING_OPTIONS;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

public class FunctionCallRewriter
{
    private static final List<Class<?>> SUPPORTED_ORIGINAL_FUNCTIONS = ImmutableList.of(FunctionCall.class, CurrentTime.class);
    private static final List<Class<?>> SUPPORTED_SUBSTITUTE_EXPRESSIONS = ImmutableList.of(FunctionCall.class, IfExpression.class, SimpleCaseExpression.class,
            SearchedCaseExpression.class, Identifier.class, Literal.class, ArrayConstructor.class);
    private static final String OMIT_IDENTIFIER = "_";

    private final Multimap<String, FunctionCallSubstitute> functionCallSubstituteMap;
    private final FunctionAndTypeManager functionAndTypeManager;

    private FunctionCallRewriter(Multimap<String, FunctionCallSubstitute> functionCallSubstituteMap, FunctionAndTypeManager functionAndTypeManager)
    {
        this.functionCallSubstituteMap = requireNonNull(functionCallSubstituteMap, "functionCallSubstituteMap is null.");
        this.functionAndTypeManager = requireNonNull(functionAndTypeManager, "functionAndTypeManager is null");
    }

    public static Optional<FunctionCallRewriter> getInstance(Multimap<String, FunctionCallSubstitute> functionCallSubstitutes, TypeManager typeManager)
    {
        if (functionCallSubstitutes.isEmpty()) {
            return Optional.empty();
        }
        checkState(typeManager instanceof FunctionAndTypeManager, "FunctionAndTypeManager is required for FunctionCallRewriter.");
        return Optional.of(new FunctionCallRewriter(functionCallSubstitutes, (FunctionAndTypeManager) typeManager));
    }

    public static Multimap<String, FunctionCallSubstitute> validateAndConstructFunctionCallSubstituteMap(String functionCallSubstitutes)
    {
        ImmutableMultimap.Builder<String, FunctionCallSubstitute> map = ImmutableMultimap.builder();
        if (functionCallSubstitutes == null) {
            return map.build();
        }

        Splitter commaSplitter = Splitter.on("/,/").omitEmptyStrings().trimResults();
        Splitter slashSplitter = Splitter.on('/').omitEmptyStrings().trimResults();
        for (String substitute : commaSplitter.split(functionCallSubstitutes)) {
            List<String> specs = slashSplitter.splitToList(substitute);
            if (specs.size() != 2) {
                throw new IllegalArgumentException(String.format("Original function call and substitute must both be specified, %s.", substitute));
            }
            Expression originalExpression = parseOriginalFunctionCall(specs.get(0));
            Expression substituteExpression = parseSubstituteExpression(specs.get(1));

            if (originalExpression instanceof FunctionCall) {
                FunctionCall originalFunction = (FunctionCall) originalExpression;
                map.put(originalFunction.getName().getSuffix(), new FunctionCallSubstitute(originalExpression, substituteExpression));
            }
            else if (originalExpression instanceof CurrentTime) {
                CurrentTime originalFunction = (CurrentTime) originalExpression;
                map.put(originalFunction.getFunction().getName(), new FunctionCallSubstitute(originalExpression, substituteExpression));
            }
        }

        return map.build();
    }

    public RewriterResult rewrite(Statement root)
    {
        RewriterContext context = new RewriterContext();

        Statement rewrittenRoot = (Statement) new Rewriter(functionCallSubstituteMap, functionAndTypeManager).process(root, context);
        String functionCallSubstitutes = context.rewrittenFunctionCalls.stream().map(functionCallSubstitute -> {
            String formattedOriginal = ExpressionFormatter.formatExpression(functionCallSubstitute.originalExpression, Optional.empty());
            String formattedSubstitute = ExpressionFormatter.formatExpression(functionCallSubstitute.substituteExpression, Optional.empty());
            return String.format("%s is substituted with %s", formattedOriginal, formattedSubstitute);
        }).collect(Collectors.joining(", "));

        return new RewriterResult(rewrittenRoot, functionCallSubstitutes.isEmpty() ? Optional.empty() : Optional.of(functionCallSubstitutes));
    }

    public static class FunctionCallSubstitute
    {
        private final Expression originalExpression;
        private final Expression substituteExpression;

        public FunctionCallSubstitute(Expression originalExpression, Expression substituteExpression)
        {
            this.originalExpression = requireNonNull(originalExpression, "originalExpression is null");
            this.substituteExpression = requireNonNull(substituteExpression, "substituteExpression is null");
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
            FunctionCallSubstitute that = (FunctionCallSubstitute) o;
            return originalExpression.equals(that.originalExpression) && substituteExpression.equals(that.substituteExpression);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(originalExpression, substituteExpression);
        }
    }

    public static class RewriterResult
    {
        private final Statement rewrittenNode;
        private final Optional<String> substitutions;

        public RewriterResult(Statement rewrittenNode, Optional<String> substitutions)
        {
            this.rewrittenNode = requireNonNull(rewrittenNode, "rewrittenNode is null");
            this.substitutions = substitutions;
        }

        public Statement getRewrittenNode()
        {
            return rewrittenNode;
        }

        public Optional<String> getSubstitutions()
        {
            return substitutions;
        }
    }

    private static class Rewriter
            extends DefaultTreeRewriter<RewriterContext>
    {
        private final Multimap<String, FunctionCallSubstitute> functionCallSubstituteMap;
        private final SubstituteIdentifierResolver substituteIdentifierResolver;

        public Rewriter(Multimap<String, FunctionCallSubstitute> functionCallSubstituteMap, FunctionAndTypeManager functionAndTypeManager)
        {
            this.functionCallSubstituteMap = requireNonNull(functionCallSubstituteMap, "functionCallSubstituteMap is null.");
            this.substituteIdentifierResolver = new SubstituteIdentifierResolver(functionAndTypeManager);
        }

        @Override
        protected Node visitExpression(Expression node, RewriterContext context)
        {
            Rewriter queryRewriter = this;

            return ExpressionTreeRewriter.rewriteWith(new ExpressionRewriter<Void>()
            {
                @Override
                public Expression rewriteFunctionCall(FunctionCall original, Void voidContext, ExpressionTreeRewriter<Void> treeRewriter)
                {
                    FunctionCall defaultRewrite = treeRewriter.defaultRewrite(original, voidContext); // unit test

                    Optional<FunctionCallSubstitute> substituteInfo = getSubstitution(original);
                    if (!substituteInfo.isPresent()) {
                        return defaultRewrite;
                    }

                    Map<Identifier, Expression> identifierToArgumentMap = getIdentifierToOriginalArgumentMap((FunctionCall) substituteInfo.get().originalExpression, defaultRewrite);
                    Expression rewritten = buildSubstitute(substituteInfo.get().substituteExpression, identifierToArgumentMap, defaultRewrite);

                    context.rewrittenFunctionCalls.add(new FunctionCallSubstitute(original, rewritten));
                    return rewritten;
                }

                @Override
                public Expression rewriteCurrentTime(CurrentTime original, Void voidContext, ExpressionTreeRewriter<Void> treeRewriter)
                {
                    CurrentTime defaultRewrite = treeRewriter.defaultRewrite(original, voidContext);

                    Optional<FunctionCallSubstitute> substituteInfo = getSubstitution(original);
                    if (!substituteInfo.isPresent()) {
                        return defaultRewrite;
                    }

                    Expression rewritten = substituteInfo.get().substituteExpression;

                    context.rewrittenFunctionCalls.add(new FunctionCallSubstitute(original, rewritten));
                    return rewritten;
                }

                @Override
                public Expression rewriteSubqueryExpression(SubqueryExpression expression, Void voidContext, ExpressionTreeRewriter<Void> treeRewriter)
                {
                    Node query = queryRewriter.process(expression.getQuery(), context);
                    if (expression.getQuery() == query) {
                        return expression;
                    }

                    return new SubqueryExpression((Query) query);
                }
            }, node);
        }

        private Optional<FunctionCallSubstitute> getSubstitution(FunctionCall instance)
        {
            for (FunctionCallSubstitute substitution : functionCallSubstituteMap.get(instance.getName().getSuffix())) {
                if (!(substitution.originalExpression instanceof FunctionCall)) {
                    continue;
                }

                List<Expression> originalArguments = ((FunctionCall) substitution.originalExpression).getArguments();
                List<Expression> instanceArguments = instance.getArguments();
                if (originalArguments.size() > instanceArguments.size()) {
                    continue;
                }

                int i = 0;
                for (; i < originalArguments.size(); i++) {
                    if (originalArguments.get(i) instanceof Literal && !(instanceArguments.get(i) instanceof Literal)) {
                        break;
                    }
                    if (originalArguments.get(i) instanceof ArrayConstructor) {
                        if (!(instanceArguments.get(i) instanceof ArrayConstructor)) {
                            break;
                        }

                        List<Expression> originalValues = ((ArrayConstructor) originalArguments.get(i)).getValues();
                        List<Expression> actualValues = ((ArrayConstructor) instanceArguments.get(i)).getValues();
                        if (!originalValues.isEmpty() && originalValues.get(0) instanceof Literal && !actualValues.stream().allMatch(Literal.class::isInstance)) { // unit test
                            break;
                        }
                    }
                }
                if (i < originalArguments.size()) {
                    continue;
                }
                return Optional.of(substitution);
            }

            return Optional.empty();
        }

        private Optional<FunctionCallSubstitute> getSubstitution(CurrentTime instance)
        {
            for (FunctionCallSubstitute substitution : functionCallSubstituteMap.get(instance.getFunction().getName())) {
                if (substitution.originalExpression instanceof CurrentTime) {
                    return Optional.of(substitution);
                }
            }

            return Optional.empty();
        }

        private static Map<Identifier, Expression> getIdentifierToOriginalArgumentMap(FunctionCall originalPattern, FunctionCall originalInstance)
        {
            ImmutableMap.Builder<Identifier, Expression> identifierToArgumentMap = ImmutableMap.builder();

            List<Expression> patternArguments = originalPattern.getArguments();
            List<Expression> instanceArguments = originalInstance.getArguments();
            for (int i = 0; i < patternArguments.size(); i++) {
                if (patternArguments.get(i) instanceof Identifier) {
                    Identifier identifier = (Identifier) patternArguments.get(i);
                    if (OMIT_IDENTIFIER.equals(identifier.getValue())) {
                        continue;
                    }
                }
                identifierToArgumentMap.put(toIdentifier(patternArguments.get(i)), instanceArguments.get(i));
            }

            List<SortItem> patternOrderBys = originalPattern.getOrderBy().map(OrderBy::getSortItems).orElse(ImmutableList.of());
            List<SortItem> instanceOrderBys = originalInstance.getOrderBy().map(OrderBy::getSortItems).orElse(ImmutableList.of());
            for (int i = 0; i < patternOrderBys.size(); i++) {
                Identifier identifier = (Identifier) patternOrderBys.get(i).getSortKey();
                if (OMIT_IDENTIFIER.equals(identifier.getValue())) {
                    continue;
                }
                identifierToArgumentMap.put(identifier, instanceOrderBys.get(i).getSortKey());
            }

            List<Expression> patternWindowPartitionBys = originalPattern.getWindow().map(Window::getPartitionBy).orElse(ImmutableList.of());
            List<Expression> instanceWindowPartitionBys = originalInstance.getWindow().map(Window::getPartitionBy).orElse(ImmutableList.of());
            for (int i = 0; i < patternWindowPartitionBys.size(); i++) {
                Identifier identifier = (Identifier) patternWindowPartitionBys.get(i);
                if (OMIT_IDENTIFIER.equals(identifier.getValue())) {
                    continue;
                }
                identifierToArgumentMap.put(identifier, instanceWindowPartitionBys.get(i));
            }

            List<SortItem> patternWindowOrderBys = originalPattern.getWindow().flatMap(Window::getOrderBy).map(OrderBy::getSortItems).orElse(ImmutableList.of());
            List<SortItem> instanceWindowOrderBys = originalInstance.getWindow().flatMap(Window::getOrderBy).map(OrderBy::getSortItems).orElse(ImmutableList.of());
            for (int i = 0; i < patternWindowOrderBys.size(); i++) {
                Identifier identifier = (Identifier) patternWindowOrderBys.get(i).getSortKey();
                if (OMIT_IDENTIFIER.equals(identifier.getValue())) {
                    continue;
                }
                identifierToArgumentMap.put(identifier, instanceWindowOrderBys.get(i).getSortKey());
            }

            return identifierToArgumentMap.build();
        }

        private Expression buildSubstitute(Expression substitutePattern, Map<Identifier, Expression> identifierToArgumentMap, FunctionCall originalInstance)
        {
            return substituteIdentifierResolver.resolve(substitutePattern, identifierToArgumentMap, originalInstance);
        }
    }

    private static class RewriterContext
    {
        public List<FunctionCallSubstitute> rewrittenFunctionCalls = new ArrayList<>();
    }

    private static class SubstituteIdentifierResolver
    {
        private final FunctionAndTypeManager functionAndTypeManager;

        public SubstituteIdentifierResolver(FunctionAndTypeManager functionAndTypeManager)
        {
            this.functionAndTypeManager = functionAndTypeManager;
        }

        public Expression resolve(Expression substitutePattern, Map<Identifier, Expression> identifierToArgumentMap, FunctionCall originalInstance)
        {
            return ExpressionTreeRewriter.rewriteWith(new ExpressionRewriter<Void>()
            {
                @Override
                public Expression rewriteIdentifier(Identifier identifier, Void context, ExpressionTreeRewriter<Void> treeRewriter)
                {
                    if (!identifierToArgumentMap.containsKey(identifier)) {
                        return identifier;
                    }
                    return identifierToArgumentMap.get(identifier);
                }

                @Override
                public Expression rewriteFunctionCall(FunctionCall functionPattern, Void context, ExpressionTreeRewriter<Void> treeRewriter)
                {
                    FunctionCall defaultRewrite = treeRewriter.defaultRewrite(functionPattern, context);
                    if (!isAggregateOrWindowFunction(functionPattern)) {
                        return defaultRewrite;
                    }

                    boolean rewrittenDistinct = originalInstance.isDistinct();
                    boolean rewrittenIgnoreNulls = false;
                    Optional<Expression> rewrittenFilter = defaultRewrite.getFilter().isPresent() ? defaultRewrite.getFilter() : originalInstance.getFilter();
                    Optional<OrderBy> rewrittenOrderBy = defaultRewrite.getOrderBy().isPresent() ? defaultRewrite.getOrderBy() : originalInstance.getOrderBy();

                    Optional<Window> rewrittenWindow;
                    if (defaultRewrite.getWindow().isPresent() && originalInstance.getWindow().isPresent()) {
                        Window defaultWindow = defaultRewrite.getWindow().get();
                        Window originalWindow = originalInstance.getWindow().get();
                        rewrittenWindow = Optional.of(new Window(!defaultWindow.getPartitionBy().isEmpty() ? defaultWindow.getPartitionBy() : originalWindow.getPartitionBy(),
                                defaultWindow.getOrderBy().isPresent() ? defaultWindow.getOrderBy() : originalWindow.getOrderBy(), defaultWindow.getFrame().isPresent() ?
                                defaultWindow.getFrame() : originalWindow.getFrame()));
                    }
                    else {
                        rewrittenWindow = defaultRewrite.getWindow().isPresent() ? defaultRewrite.getWindow() : originalInstance.getWindow();
                    }

                    return new FunctionCall(defaultRewrite.getName(), rewrittenWindow, rewrittenFilter, rewrittenOrderBy, rewrittenDistinct, rewrittenIgnoreNulls,
                            defaultRewrite.getArguments());
                }

                @Override
                public Expression rewriteLiteral(Literal literal, Void context, ExpressionTreeRewriter<Void> treeRewriter)
                {
                    Expression argument = identifierToArgumentMap.get(toIdentifier(literal));
                    if (argument == null) {
                        return literal;
                    }
                    return argument;
                }

                @Override
                public Expression rewriteArrayConstructor(ArrayConstructor array, Void context, ExpressionTreeRewriter<Void> treeRewriter)
                {
                    Expression argument = identifierToArgumentMap.get(toIdentifier(array));
                    if (argument == null) {
                        return array;
                    }
                    return argument;
                }
            }, substitutePattern);
        }

        private boolean isAggregateOrWindowFunction(FunctionCall functionCall)
        {
            Collection<SqlFunction> allFunctions = functionAndTypeManager.listBuiltInFunctions();
            for (SqlFunction function : allFunctions) {
                Signature signature = function.getSignature();
                if (signature.getNameSuffix().equals(functionCall.getName().getSuffix())) {
                    if (signature.getKind().equals(FunctionKind.AGGREGATE) || signature.getKind().equals(FunctionKind.WINDOW)) {
                        return true;
                    }
                }
            }
            return false;
        }
    }

    private static Expression parseOriginalFunctionCall(String functionCallSpec)
    {
        SqlParser sqlParser = new SqlParser();
        Expression expression;
        try {
            expression = sqlParser.createExpression(functionCallSpec, PARSING_OPTIONS);
        }
        catch (ParsingException e) {
            throw new IllegalArgumentException(String.format("Function call spec %s is not in a valid format.", functionCallSpec), e);
        }

        if (SUPPORTED_ORIGINAL_FUNCTIONS.stream().noneMatch(clazz -> clazz.equals(expression.getClass()))) {
            throw new IllegalArgumentException(String.format("Substituting %s in %s is not supported.", expression.getClass().getSimpleName(), functionCallSpec));
        }

        if (expression instanceof FunctionCall) {
            FunctionCall functionCall = (FunctionCall) expression;

            Stream<Expression> arguments = functionCall.getArguments().stream();
            arguments = Stream.concat(arguments, functionCall.getOrderBy().map(OrderBy::getSortItems).orElse(ImmutableList.of()).stream().map(SortItem::getSortKey));
            arguments = Stream.concat(arguments, functionCall.getWindow().map(Window::getPartitionBy).orElse(ImmutableList.of()).stream());
            arguments = Stream.concat(arguments, functionCall.getWindow().flatMap(Window::getOrderBy).map(OrderBy::getSortItems).orElse(ImmutableList.of()).stream().map(SortItem::getSortKey));

            arguments.forEach(argument -> {
                if (argument instanceof Identifier || argument instanceof Literal) {
                    return;
                }
                if (argument instanceof ArrayConstructor) {
                    if (((ArrayConstructor) argument).getValues().stream().allMatch(Literal.class::isInstance)) {
                        return;
                    }
                }
                throw new IllegalArgumentException(String.format("Argument of type %s from %s is not supported.", argument.getClass().getSimpleName(), functionCallSpec));
            });
        }
        return expression;
    }

    private static Expression parseSubstituteExpression(String expressionSpec)
    {
        SqlParser sqlParser = new SqlParser();
        Expression expression;
        try {
            expression = sqlParser.createExpression(expressionSpec, PARSING_OPTIONS);
        }
        catch (ParsingException e) {
            throw new IllegalArgumentException(String.format("Expression spec %s is not in a valid format.", expressionSpec), e);
        }

        if (SUPPORTED_SUBSTITUTE_EXPRESSIONS.stream().noneMatch(clazz -> clazz.isAssignableFrom(expression.getClass()))) {
            throw new IllegalArgumentException(String.format("Substitution of with from %s is not supported.", expression.getClass().getSimpleName()));
        }

        return expression;
    }

    private static Identifier toIdentifier(Expression expression)
    {
        if (expression instanceof Identifier) {
            return (Identifier) expression;
        }
        return new Identifier(String.valueOf(expression.toString().hashCode()));
    }
}
