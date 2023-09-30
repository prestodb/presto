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

import com.facebook.presto.sql.ExpressionFormatter;
import com.facebook.presto.sql.parser.ParsingException;
import com.facebook.presto.sql.parser.SqlParser;
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
import com.facebook.presto.sql.tree.SortItem;
import com.facebook.presto.sql.tree.SubqueryExpression;
import com.facebook.presto.sql.tree.Window;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.commons.math3.analysis.function.Exp;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

import static com.facebook.presto.verifier.framework.VerifierUtil.PARSING_OPTIONS;
import static java.util.Objects.requireNonNull;

public class FunctionCallRewriter
        extends DefaultTreeRewriter<FunctionCallRewriter.Context>
{
    public static class FunctionCallSubstitute
    {
        private final Expression substituteExpression;
        private final List<Identifier> originalArgumentIdentifierList;
        private final List<Identifier> originalWindowPartitionIdentifierList;
        private final List<Identifier> originalWindowOrderIdentifierList;

        public FunctionCallSubstitute(Expression substituteExpression, List<Identifier> originalArgumentIdentifierList)
        {
            this(substituteExpression, originalArgumentIdentifierList, ImmutableList.of(), ImmutableList.of());
        }

        public FunctionCallSubstitute(Expression substituteExpression, List<Identifier> originalArgumentIdentifierList, List<Identifier> originalWindowPartitionIdentifierList, List<Identifier> originalWindowOrderIdentifierList)
        {
            this.substituteExpression = requireNonNull(substituteExpression, "substituteExpression is null");
            this.originalArgumentIdentifierList = ImmutableList.copyOf(requireNonNull(originalArgumentIdentifierList, "originalArgumentIdentifierList is null"));
            this.originalWindowPartitionIdentifierList = ImmutableList.copyOf(requireNonNull(originalWindowPartitionIdentifierList, "originalWindowPartitionIdentifierList is null"));
            this.originalWindowOrderIdentifierList = ImmutableList.copyOf(requireNonNull(originalWindowOrderIdentifierList, "originalWindowOrderIdentifierList is null"));
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
            return substituteExpression.equals(that.substituteExpression) && Objects.equals(originalArgumentIdentifierList, that.originalArgumentIdentifierList) && Objects.equals(originalWindowPartitionIdentifierList, that.originalWindowPartitionIdentifierList) && Objects.equals(originalWindowOrderIdentifierList, that.originalWindowOrderIdentifierList);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(substituteExpression, originalArgumentIdentifierList, originalWindowPartitionIdentifierList, originalWindowOrderIdentifierList);
        }
    }

    private static final List<Class<?>> SUPPORTED_ORIGINAL_FUNCTIONS = ImmutableList.of(FunctionCall.class, CurrentTime.class);
    private static final List<Class<?>> SUPPORTED_SUBSTITUTE_EXPRESSIONS = ImmutableList.of(FunctionCall.class, IfExpression.class, Literal.class);

    private final Map<String, FunctionCallSubstitute> functionCallSubstituteMap;

    private List<List<Expression>> processedFunctionCallSubstitutes = ImmutableList.of();

    private FunctionCallRewriter(Map<String, FunctionCallSubstitute> functionCallSubstituteMap)
    {
        this.functionCallSubstituteMap = requireNonNull(functionCallSubstituteMap, "functionCallSubstituteMap is null.");
    }

    public static FunctionCallRewriter getInstance(String functionCallSubstitutes)
    {
        Map<String, FunctionCallSubstitute> functionCallSubstituteMap = validateAndConstructFunctionCallSubstituteMap(functionCallSubstitutes);
        return functionCallSubstituteMap.isEmpty() ? null : new FunctionCallRewriter(functionCallSubstituteMap);
    }

    public static Map<String, FunctionCallSubstitute> validateAndConstructFunctionCallSubstituteMap(String functionCallSubstitutes)
    {
        ImmutableMap.Builder<String, FunctionCallSubstitute> map = ImmutableMap.builder();
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
            Expression originalExpression = parseFunctionCall(specs.get(0));
            Expression substituteExpression = parseSubstituteExpression(specs.get(1));

            if (originalExpression instanceof FunctionCall) {
                FunctionCall originalFunction = (FunctionCall) originalExpression;

                String errorMessage = String.format("All arguments must be Identifier for a function call to be substituted, %s.", substitute);
                List<Identifier> originalArguments = toIdentifierList(originalFunction.getArguments(), errorMessage);
                List<Identifier> originalWindowPartition = toIdentifierList(originalFunction.getWindow().map(Window::getPartitionBy).orElse(ImmutableList.of()), errorMessage);
                List<Identifier> originalWindowOrder = toIdentifierList(originalFunction.getWindow().flatMap(Window::getOrderBy).map(OrderBy::getSortItems).orElse(ImmutableList.of()).stream().map(SortItem::getSortKey).collect(ImmutableList.toImmutableList()), errorMessage);

                map.put(originalFunction.getName().getSuffix(), new FunctionCallSubstitute(substituteExpression, originalArguments, originalWindowPartition,
                        originalWindowOrder));
            }
            else if (originalExpression instanceof CurrentTime) {
                CurrentTime originalFunction = (CurrentTime) originalExpression;
                map.put(originalFunction.getFunction().getName(), new FunctionCallSubstitute(substituteExpression, ImmutableList.of(), ImmutableList.of(), ImmutableList.of()));
            }
        }

        Map<String, FunctionCallSubstitute> result = map.build();
        if (result.isEmpty()) {
            throw new IllegalArgumentException(String.format("% has no valid function call substitution spec.", functionCallSubstitutes));
        }

        return result;
    }

    public Node rewrite(Node root)
    {
        Context context = new Context();
        Node rewritten = process(root, context);
        processedFunctionCallSubstitutes = context.getFunctionCallSubstitutes();
        return rewritten;
    }

    public String getProcessedFunctionCallSubstitutes()
    {
        return processedFunctionCallSubstitutes.stream().map(functionCallSubstitute -> {
                    String formattedOriginal = ExpressionFormatter.formatExpression(functionCallSubstitute.get(0), Optional.empty());
                    String formattedSubstitute = ExpressionFormatter.formatExpression(functionCallSubstitute.get(1), Optional.empty());
                    return String.format("%s is substituted with %s", formattedOriginal, formattedSubstitute);
                }
        ).collect(Collectors.joining(", "));
    }

    @Override
    protected Node visitExpression(Expression node, FunctionCallRewriter.Context context)
    {
        FunctionCallRewriter nodeRewritter = this;

        return ExpressionTreeRewriter.rewriteWith(new ExpressionRewriter<Void>()
        {
            @Override
            public Expression rewriteFunctionCall(FunctionCall original, Void voidContext, ExpressionTreeRewriter<Void> treeRewriter)
            {
                FunctionCall defaultRewrite = treeRewriter.defaultRewrite(original, voidContext);

                FunctionCallSubstitute substituteInfo = functionCallSubstituteMap.get(original.getName().getSuffix());
                if (substituteInfo == null) {
                    return defaultRewrite;
                }

                Map<Identifier, Expression> identifierToArgumentMap = getIdentifierToArgumentMap(defaultRewrite, substituteInfo);
                Expression rewritten = resolveFunctionsAndIdentifierArguments(substituteInfo.substituteExpression, identifierToArgumentMap, defaultRewrite);

                context.addFunctionCallSubstitute(original, rewritten);
                return rewritten;
            }

            @Override
            public Expression rewriteCurrentTime(CurrentTime original, Void voidContext, ExpressionTreeRewriter<Void> treeRewriter)
            {
                CurrentTime defaultRewrite = treeRewriter.defaultRewrite(original, voidContext);
                FunctionCallSubstitute substituteInfo = functionCallSubstituteMap.get(original.getFunction().getName());
                if (substituteInfo == null) {
                    return defaultRewrite;
                }

                Expression rewritten = substituteInfo.substituteExpression;

                context.addFunctionCallSubstitute(original, rewritten);
                return rewritten;
            }

            @Override
            public Expression rewriteSubqueryExpression(SubqueryExpression expression, Void voidContext, ExpressionTreeRewriter<Void> treeRewriter)
            {
                Node query = nodeRewritter.process(expression.getQuery(), context);
                if (expression.getQuery() == query) {
                    return expression;
                }

                return new SubqueryExpression((Query) query);
            }
        }, node);
    }

    private static Expression resolveFunctionsAndIdentifierArguments(Expression expression, Map<Identifier, Expression> identifierToArgumentMap,
            FunctionCall sourceFunctionCall)
    {
        return ExpressionTreeRewriter.rewriteWith(new ExpressionRewriter<Boolean>()
        {
            @Override
            public Expression rewriteIdentifier(Identifier targetNode, Boolean context, ExpressionTreeRewriter<Boolean> treeRewriter)
            {
                if (!identifierToArgumentMap.containsKey(targetNode)) {
                    return targetNode;
                }
                return identifierToArgumentMap.get(targetNode);
            }

            @Override
            public Expression rewriteFunctionCall(FunctionCall targetNode, Boolean isRoot, ExpressionTreeRewriter<Boolean> treeRewriter)
            {
                FunctionCall defaultRewrite = treeRewriter.defaultRewrite(targetNode, false);

                boolean rewrittenDistinct = defaultRewrite.isDistinct();
                boolean rewrittenIgnoreNulls = defaultRewrite.isIgnoreNulls();
                if (targetNode.getName().getSuffix().equals(sourceFunctionCall.getName().getSuffix())) {
                    rewrittenDistinct = sourceFunctionCall.isDistinct();
                    rewrittenIgnoreNulls = sourceFunctionCall.isIgnoreNulls();
                }

                Optional<Window> rewrittenWindow = defaultRewrite.getWindow();
                if (isRoot && sourceFunctionCall.getWindow().isPresent()) {
                    if (defaultRewrite.getWindow().isPresent()) {
                        Window sourceWindow = sourceFunctionCall.getWindow().get();
                        Window targetWindow = defaultRewrite.getWindow().get();
                        rewrittenWindow = Optional.of(new Window(!targetWindow.getPartitionBy().isEmpty() ? targetWindow.getPartitionBy() : sourceWindow.getPartitionBy(),
                                targetWindow.getOrderBy().isPresent() ? targetWindow.getOrderBy() : sourceWindow.getOrderBy(), targetWindow.getFrame().isPresent() ?
                                targetWindow.getFrame() : sourceWindow.getFrame()));
                    }
                    else {
                        rewrittenWindow = sourceFunctionCall.getWindow();
                    }
                }

                Optional<Expression> rewrittenFilter = defaultRewrite.getFilter().isPresent() ? defaultRewrite.getFilter() : sourceFunctionCall.getFilter();
                Optional<OrderBy> rewrittenOrderBy = defaultRewrite.getOrderBy().isPresent() ? defaultRewrite.getOrderBy() : sourceFunctionCall.getOrderBy();

                return new FunctionCall(defaultRewrite.getName(), rewrittenWindow, rewrittenFilter, rewrittenOrderBy, rewrittenDistinct, rewrittenIgnoreNulls,
                        defaultRewrite.getArguments());
            }
        }, expression, true);
    }

    private static Expression parseFunctionCall(String functionCallSpec)
    {
        String errorMessage = String.format("Function call spec %s is not in a valid format.", functionCallSpec);

        SqlParser sqlParser = new SqlParser();
        Expression expression;
        try {
            expression = sqlParser.createExpression(functionCallSpec, PARSING_OPTIONS);
        }
        catch (ParsingException e) {
            throw new IllegalArgumentException(errorMessage, e);
        }

        if (!SUPPORTED_ORIGINAL_FUNCTIONS.stream().anyMatch(clazz -> clazz.equals(expression.getClass()))) {
            throw new IllegalArgumentException(errorMessage);
        }

        return expression;
    }

    private static Expression parseSubstituteExpression(String expressionSpec)
    {
        String errorMessage = String.format("Expression spec %s is not in a valid format.", expressionSpec);

        SqlParser sqlParser = new SqlParser();
        Expression expression;
        try {
            expression = sqlParser.createExpression(expressionSpec, PARSING_OPTIONS);
        }
        catch (ParsingException e) {
            throw new IllegalArgumentException(errorMessage, e);
        }

        if (!SUPPORTED_SUBSTITUTE_EXPRESSIONS.stream().anyMatch(clazz -> clazz.isAssignableFrom(expression.getClass()))) {
            throw new IllegalArgumentException(errorMessage);
        }

        return expression;
    }

    private static Map<Identifier, Expression> getIdentifierToArgumentMap(FunctionCall functionCall, FunctionCallSubstitute substituteInfo)
    {
        ImmutableMap.Builder<Identifier, Expression> identifierToArgumentMap = ImmutableMap.builder();

        for (int i = 0; i < substituteInfo.originalArgumentIdentifierList.size(); i++) {
            Identifier identifier = substituteInfo.originalArgumentIdentifierList.get(i);
            try {
                identifierToArgumentMap.put(identifier, functionCall.getArguments().get(i));
            }
            catch (IndexOutOfBoundsException exception) {
                throw new IllegalArgumentException(String.format("Failed to extract the argument for identifier %s from function call %s.", identifier, functionCall));
            }
        }
        for (int i = 0; i < substituteInfo.originalWindowPartitionIdentifierList.size(); i++) {
            Identifier identifier = substituteInfo.originalWindowPartitionIdentifierList.get(i);
            try {
                identifierToArgumentMap.put(identifier, functionCall.getWindow().get().getPartitionBy().get(i));
            }
            catch (IndexOutOfBoundsException exception) {
                throw new IllegalArgumentException(String.format("Failed to extract the argument for identifier %s from function call %s.", identifier, functionCall));
            }
        }
        for (int i = 0; i < substituteInfo.originalWindowOrderIdentifierList.size(); i++) {
            Identifier identifier = substituteInfo.originalWindowOrderIdentifierList.get(i);
            try {
                identifierToArgumentMap.put(identifier, functionCall.getWindow().get().getOrderBy().get().getSortItems().get(i).getSortKey());
            }
            catch (IndexOutOfBoundsException exception) {
                throw new IllegalArgumentException(String.format("Failed to extract the argument for identifier %s from function call %s.", identifier, functionCall));
            }
        }
        return identifierToArgumentMap.build();
    }

    private static List<Identifier> toIdentifierList(List<Expression> expressionList, String errorMessage)
    {
        return expressionList.stream().map(argument -> {
            if (!(argument instanceof Identifier)) {
                throw new IllegalArgumentException(errorMessage);
            }
            return (Identifier) argument;
        }).collect(ImmutableList.toImmutableList());
    }

    public static class Context
    {
        private List<List<Expression>> functionCallSubstitutes = new ArrayList<>();

        public void addFunctionCallSubstitute(Expression original, Expression substitute)
        {
            functionCallSubstitutes.add(Arrays.asList(original, substitute));
        }

        public List<List<Expression>> getFunctionCallSubstitutes()
        {
            return ImmutableList.copyOf(functionCallSubstitutes);
        }
    }
}
