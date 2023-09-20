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
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.ExpressionRewriter;
import com.facebook.presto.sql.tree.ExpressionTreeRewriter;
import com.facebook.presto.sql.tree.FunctionCall;
import com.facebook.presto.sql.tree.Node;
import com.facebook.presto.sql.tree.QualifiedName;
import com.facebook.presto.sql.tree.Query;
import com.facebook.presto.sql.tree.SubqueryExpression;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;

public class FunctionCallRewriter
        extends DefaultTreeRewriter<FunctionCallRewriter.Context>
{
    public static class FunctionCallSubstitute
    {
        private final QualifiedName name;
        private final List<Integer> originalArgumentIndices;

        public FunctionCallSubstitute(QualifiedName name, List<Integer> originalArgumentIndices)
        {
            this.name = requireNonNull(name, "name is null");
            this.originalArgumentIndices = ImmutableList.copyOf(requireNonNull(originalArgumentIndices, "originalArgumentIndices is null"));
        }

        public QualifiedName name()
        {
            return name;
        }

        public List<Integer> originalArgumentIndices()
        {
            return originalArgumentIndices;
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
            return name.equals(that.name) && Objects.equals(originalArgumentIndices, that.originalArgumentIndices);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(name, originalArgumentIndices);
        }
    }

    // Pattern of a function call with function name and a comma-separated argument list, ex., func_x(c_0,c_1), func_x(_,c_1) or func_x(,c_1).
    private static final Pattern FUNCTION_CALL_PATTERN = Pattern.compile("(\\w+)\\(([\\w|,]+)\\)");
    // Pattern to specify function call substitution, with the first element as the original and the second element as the substitute.
    private static final Pattern FUNCTION_CALL_SUBSTITUTION_PATTERN = Pattern.compile(String.format("/%s/%s/", FUNCTION_CALL_PATTERN, FUNCTION_CALL_PATTERN));

    private final Map<QualifiedName, FunctionCallSubstitute> functionCallSubstituteMap;

    private List<List<FunctionCall>> processedFunctionCallSubstitutes = ImmutableList.of();

    private FunctionCallRewriter(Map<QualifiedName, FunctionCallSubstitute> functionCallSubstituteMap)
    {
        this.functionCallSubstituteMap = requireNonNull(functionCallSubstituteMap, "functionCallSubstituteMap is null.");
    }

    public static FunctionCallRewriter getInstance(String functionCallSubstitutes)
    {
        Map<QualifiedName, FunctionCallSubstitute> functionCallSubstituteMap = constructFunctionCallSubstituteMap(functionCallSubstitutes);
        return functionCallSubstituteMap.isEmpty() ? null : new FunctionCallRewriter(functionCallSubstituteMap);
    }

    public static boolean validateFunctionCallSubstitutes(String functionCallSubstitutes)
    {
        if (functionCallSubstitutes == null) {
            return false;
        }

        Matcher matcher = FUNCTION_CALL_SUBSTITUTION_PATTERN.matcher(functionCallSubstitutes);
        return matcher.find();
    }

    public static Map<QualifiedName, FunctionCallSubstitute> constructFunctionCallSubstituteMap(String functionCallSubstitutes)
    {
        ImmutableMap.Builder<QualifiedName, FunctionCallSubstitute> map = ImmutableMap.builder();
        if (functionCallSubstitutes == null) {
            return map.build();
        }

        Matcher matcher = FUNCTION_CALL_SUBSTITUTION_PATTERN.matcher(functionCallSubstitutes);

        while (matcher.find()) {
            String originalName = matcher.group(1);
            List<String> originalArgumentList = ImmutableList.copyOf(matcher.group(2).split(","));

            String substituteName = matcher.group(3);
            List<String> substituteArgumentList = ImmutableList.copyOf(matcher.group(4).split(","));
            List<Integer> originalArgumentIndices = substituteArgumentList.stream().map(originalArgumentList::indexOf).collect(Collectors.toList());
            FunctionCallSubstitute substitute = new FunctionCallSubstitute(QualifiedName.of(substituteName), originalArgumentIndices);

            map.put(QualifiedName.of(originalName), substitute);
        }

        return map.build();
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
                    return String.format("% is substituted with %s", formattedOriginal, formattedSubstitute);
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
            public Expression rewriteFunctionCall(FunctionCall expression, Void voidContext, ExpressionTreeRewriter<Void> treeRewriter)
            {
                if (!functionCallSubstituteMap.containsKey(expression.getName())) {
                    return expression;
                }

                FunctionCall defaultRewrite = treeRewriter.defaultRewrite(expression, voidContext);

                FunctionCallSubstitute substitute = functionCallSubstituteMap.get(expression.getName());
                List<Expression> originalArguments = expression.getArguments();
                List<Expression> rewrittenArguments = substitute.originalArgumentIndices.stream()
                        .map(originalIndex -> {
                            Expression originalArgument = originalArguments.get(originalIndex);
                            return treeRewriter.rewrite(originalArgument, voidContext);
                        }).collect(toImmutableList());

                FunctionCall rewritten = new FunctionCall(substitute.name, defaultRewrite.getWindow(), defaultRewrite.getFilter(), defaultRewrite.getOrderBy(),
                        defaultRewrite.isDistinct(), defaultRewrite.isIgnoreNulls(), rewrittenArguments);
                context.addFunctionCallSubstitute(expression, rewritten);

                return rewritten;
            }

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

    public static class Context
    {
        private List<List<FunctionCall>> functionCallSubstitutes = new ArrayList<>();

        public void addFunctionCallSubstitute(FunctionCall original, FunctionCall substitute)
        {
            functionCallSubstitutes.add(Arrays.asList(original, substitute));
        }

        public List<List<FunctionCall>> getFunctionCallSubstitutes()
        {
            return ImmutableList.copyOf(functionCallSubstitutes);
        }
    }
}
