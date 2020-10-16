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
import com.facebook.presto.spi.function.FunctionHandle;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.planner.VariablesExtractor;
import com.facebook.presto.sql.planner.plan.WindowNode;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.FunctionCall;
import com.facebook.presto.sql.tree.QualifiedName;
import com.google.common.collect.ImmutableSet;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.facebook.presto.sql.relational.OriginalExpressionUtils.castToExpression;
import static com.facebook.presto.sql.relational.OriginalExpressionUtils.isExpression;
import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;

public class WindowFunctionMatcher
        implements RvalueMatcher
{
    private final ExpectedValueProvider<FunctionCall> callMaker;
    private final Optional<FunctionHandle> functionHandle;
    private final Optional<ExpectedValueProvider<WindowNode.Frame>> frameMaker;

    /**
     * @param callMaker Always validates the function call
     * @param functionHandle Optionally validates the function handle
     * @param frameMaker Optionally validates the frame
     */
    public WindowFunctionMatcher(
            ExpectedValueProvider<FunctionCall> callMaker,
            Optional<FunctionHandle> functionHandle,
            Optional<ExpectedValueProvider<WindowNode.Frame>> frameMaker)
    {
        this.callMaker = requireNonNull(callMaker, "functionCall is null");
        this.functionHandle = requireNonNull(functionHandle, "functionHandle is null");
        this.frameMaker = requireNonNull(frameMaker, "frameMaker is null");
    }

    @Override
    public Optional<VariableReferenceExpression> getAssignedVariable(PlanNode node, Session session, Metadata metadata, SymbolAliases symbolAliases)
    {
        Optional<VariableReferenceExpression> result = Optional.empty();
        if (!(node instanceof WindowNode)) {
            return result;
        }

        WindowNode windowNode = (WindowNode) node;

        FunctionCall expectedCall = callMaker.getExpectedValue(symbolAliases);
        Optional<WindowNode.Frame> expectedFrame = frameMaker.map(maker -> maker.getExpectedValue(symbolAliases));

        List<VariableReferenceExpression> matchedOutputs = windowNode.getWindowFunctions().entrySet().stream()
                .filter(assignment -> {
                    if (!expectedCall.getName().equals(QualifiedName.of(metadata.getFunctionAndTypeManager().getFunctionMetadata(assignment.getValue().getFunctionCall().getFunctionHandle()).getName().getObjectName()))) {
                        return false;
                    }
                    if (!functionHandle.map(assignment.getValue().getFunctionHandle()::equals).orElse(true)) {
                        return false;
                    }
                    if (!expectedFrame.map(assignment.getValue().getFrame()::equals).orElse(true)) {
                        return false;
                    }
                    List<Expression> expectedExpressions = expectedCall.getArguments();
                    List<RowExpression> actualExpressions = assignment.getValue().getFunctionCall().getArguments();
                    if (expectedExpressions.size() != actualExpressions.size()) {
                        return false;
                    }
                    for (int i = 0; i < expectedExpressions.size(); i++) {
                        Expression expectedExpression = expectedExpressions.get(i);
                        RowExpression actualExpression = actualExpressions.get(i);
                        if (!isExpression(actualExpression)) {
                            SymbolAliases.Builder builder = SymbolAliases.builder();
                            ImmutableSet.copyOf(VariablesExtractor.extractAllSymbols(expectedExpression)).forEach(symbol -> builder.put(symbol.getName(), symbol.toSymbolReference()));
                            if (!new RowExpressionVerifier(builder.build(), metadata, session).process(expectedExpression, actualExpression)) {
                                return false;
                            }
                        }
                        else {
                            if (!expectedExpression.equals(castToExpression(actualExpression))) {
                                return false;
                            }
                        }
                    }
                    return true;
                })
                .map(Map.Entry::getKey)
                .collect(toImmutableList());

        checkState(matchedOutputs.size() <= 1, "Ambiguous function calls in %s", windowNode);

        if (matchedOutputs.isEmpty()) {
            return Optional.empty();
        }
        return Optional.of(matchedOutputs.get(0));
    }

    @Override
    public String toString()
    {
        // Only include fields in the description if they are actual constraints.
        return toStringHelper(this)
                .omitNullValues()
                .add("callMaker", callMaker)
                .add("functionHandle", functionHandle.orElse(null))
                .add("frameMaker", frameMaker.orElse(null))
                .toString();
    }
}
