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
package com.facebook.presto.sql.planner.iterative.rule;

import com.facebook.presto.Session;
import com.facebook.presto.common.function.OperatorType;
import com.facebook.presto.common.type.ArrayType;
import com.facebook.presto.common.type.MapType;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.expressions.LogicalRowExpressions;
import com.facebook.presto.expressions.RowExpressionRewriter;
import com.facebook.presto.expressions.RowExpressionTreeRewriter;
import com.facebook.presto.metadata.FunctionAndTypeManager;
import com.facebook.presto.spi.relation.ConstantExpression;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.SpecialFormExpression;
import com.facebook.presto.sql.planner.iterative.Rule;
import com.facebook.presto.sql.relational.FunctionResolution;
import com.facebook.presto.sql.relational.RowExpressionDeterminismEvaluator;
import com.google.common.collect.ImmutableSet;

import java.lang.invoke.MethodHandle;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static com.facebook.presto.SystemSessionProperties.REWRITE_CASE_TO_MAP_ENABLED;
import static com.facebook.presto.common.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.relation.SpecialFormExpression.Form.IF;
import static com.facebook.presto.spi.relation.SpecialFormExpression.Form.IN;
import static com.facebook.presto.spi.relation.SpecialFormExpression.Form.SWITCH;
import static com.facebook.presto.spi.relation.SpecialFormExpression.Form.WHEN;
import static com.facebook.presto.sql.analyzer.TypeSignatureProvider.fromTypes;
import static com.facebook.presto.sql.relational.Expressions.call;
import static com.facebook.presto.sql.relational.Expressions.coalesce;
import static com.facebook.presto.sql.relational.Expressions.constant;
import static com.facebook.presto.sql.relational.Expressions.constantNull;
import static com.facebook.presto.sql.relational.Expressions.specialForm;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

public class RewriteCaseToMap
        extends RowExpressionRewriteRuleSet
{
    public RewriteCaseToMap(FunctionAndTypeManager functionAndTypeManager)
    {
        super(new Rewriter(functionAndTypeManager));
    }

    private static class Rewriter
            implements PlanRowExpressionRewriter
    {
        private final CaseToMapRewriter caseToMapRewriter;

        public Rewriter(FunctionAndTypeManager functionAndTypeManager)
        {
            requireNonNull(functionAndTypeManager, "functionAndTypeManager is null");
            this.caseToMapRewriter = new CaseToMapRewriter(functionAndTypeManager);
        }

        @Override
        public RowExpression rewrite(RowExpression expression, Rule.Context context)
        {
            return RowExpressionTreeRewriter.rewriteWith(caseToMapRewriter, expression);
        }
    }

    private static class CaseToMapRewriter
            extends RowExpressionRewriter<Void>
    {
        private final FunctionAndTypeManager functionAndTypeManager;
        private final FunctionResolution functionResolution;
        private final LogicalRowExpressions logicalRowExpressions;

        private CaseToMapRewriter(FunctionAndTypeManager functionAndTypeManager)
        {
            this.functionAndTypeManager = functionAndTypeManager;
            this.functionResolution = new FunctionResolution(functionAndTypeManager.getFunctionAndTypeResolver());
            this.logicalRowExpressions = new LogicalRowExpressions(
                    new RowExpressionDeterminismEvaluator(functionAndTypeManager),
                    functionResolution,
                    functionAndTypeManager);
        }

        private boolean addKeyValue(RowExpression key, Set<RowExpression> keySet, List<RowExpression> keys, RowExpression value, List<RowExpression> values)
        {
            // matching types and non-null values only allowed
            if (!(key instanceof ConstantExpression) ||
                    ((ConstantExpression) key).getValue() == null ||
                    (keys.size() > 0 && !keys.get(0).getType().equals(key.getType()))) {
                return false;
            }

            if (keySet.add(key)) {
                // We allow all same type only
                if (values.size() > 0 && !values.get(0).getType().equals(value.getType())) {
                    return false;
                }

                keys.add(key);
                values.add(value);
            }

            return true;
        }

        @Override
        public RowExpression rewriteSpecialForm(SpecialFormExpression node, Void context, RowExpressionTreeRewriter<Void> treeRewriter)
        {
            if (node.getForm() != SWITCH) {
                return rewriteRowExpression(node, context, treeRewriter);
            }

            // by construction we should have at least one WHEN and ELSE is always added if missing
            int numArgs = node.getArguments().size();
            RowExpression lastArg = node.getArguments().get(numArgs - 1);

            checkState(numArgs >= 2);
            checkState(!(lastArg instanceof SpecialFormExpression && ((SpecialFormExpression) lastArg).getForm().equals(WHEN)));

            if (!(lastArg instanceof ConstantExpression)) {
                return node;
            }

            RowExpression firstArg = node.getArguments().get(0);
            Set<RowExpression> keySet = new HashSet<>();
            List<RowExpression> whens = new ArrayList<RowExpression>(node.getArguments().size());
            List<RowExpression> thens = new ArrayList<RowExpression>(node.getArguments().size());
            RowExpression checkExpr;
            int start;

            if (!(firstArg instanceof SpecialFormExpression && ((SpecialFormExpression) firstArg).getForm().equals(WHEN))) {
                if (firstArg.equals(constant(true, BOOLEAN))) {
                    // We generate weird CASE (true) WHEN p1 THEN v1 etc. for non-searched case
                    // So drop the true
                    checkExpr = null;
                }
                else {
                    checkExpr = firstArg;
                }

                start = 1;
            }
            else {
                checkExpr = null;
                start = 0;
            }

            for (int i = start; i < numArgs - 1; i++) {
                RowExpression whenClause = node.getArguments().get(i);
                RowExpression value = whenClause.getChildren().get(1);

                if (!(value instanceof ConstantExpression)) {
                    // THEN is not a constant
                    return node;
                }

                RowExpression when = whenClause.getChildren().get(0);
                RowExpression key;
                RowExpression curCheck;

                if (when instanceof ConstantExpression) {
                    if (!addKeyValue(when, keySet, whens, value, thens)) {
                        return node;
                    }
                }
                else if (logicalRowExpressions.isEqualsExpression(when)) {
                    RowExpression lhs = when.getChildren().get(0);
                    RowExpression rhs = when.getChildren().get(1);

                    if (!lhs.getType().equals(rhs.getType())) {
                        // We keep it simple
                        return node;
                    }

                    if (lhs instanceof ConstantExpression) {
                        curCheck = rhs;
                        key = lhs;
                    }
                    else if (rhs instanceof ConstantExpression) {
                        curCheck = lhs;
                        key = rhs;
                    }
                    else {
                        return node;
                    }

                    if (checkExpr == null) {
                        checkExpr = curCheck;
                    }
                    else if (!curCheck.equals(checkExpr)) {
                        return node;
                    }

                    if (!addKeyValue(key, keySet, whens, value, thens)) {
                        return node;
                    }
                }
                else if (when instanceof SpecialFormExpression && ((SpecialFormExpression) when).getForm() == IN) {
                    curCheck = ((SpecialFormExpression) when).getArguments().get(0);
                    if (checkExpr == null) {
                        checkExpr = curCheck;
                    }
                    else if (!curCheck.equals(checkExpr)) {
                        return node;
                    }

                    // For IN also we try to gather the args
                    for (int j = 1; j < ((SpecialFormExpression) when).getArguments().size(); j++) {
                        key = ((SpecialFormExpression) when).getArguments().get(j);
                        if (!addKeyValue(key, keySet, whens, value, thens)) {
                            return node;
                        }
                    }
                }
                else {
                    return node;
                }
            }

            if (checkExpr == null) {
                return node;
            }

            // Here we have all values!
            RowExpression mapLookup = makeMapAndAccess(whens, thens, checkExpr);

            // if there is a non-trivial else, we coalesce
            if (lastArg != null && !lastArg.equals(constantNull(thens.get(0).getType()))) {
                // Null could be a legit value so we coalesce to the else part only if there was no key match
                RowExpression keyArray = call("ARRAY", functionResolution.arrayConstructor(whens.stream().map(x -> x.getType()).collect(Collectors.toList())), new ArrayType(whens.get(0).getType()), whens);
                RowExpression contains = call(functionAndTypeManager, "contains", BOOLEAN, keyArray, checkExpr);
                return coalesce(mapLookup, specialForm(IF, mapLookup.getType(), contains, constant(null, mapLookup.getType()), lastArg));
            }

            return mapLookup;
        }

        private RowExpression makeMapAndAccess(List<RowExpression> keys, List<RowExpression> values, RowExpression mapIndex)
        {
            RowExpression keyArray = call("ARRAY", functionResolution.arrayConstructor(keys.stream().map(x -> x.getType()).collect(Collectors.toList())), new ArrayType(keys.get(0).getType()), keys);
            RowExpression valueArray = call("ARRAY", functionResolution.arrayConstructor(values.stream().map(x -> x.getType()).collect(Collectors.toList())), new ArrayType(values.get(0).getType()), values);
            Type keyType = keys.get(0).getType();
            Type valueType = values.get(0).getType();
            MethodHandle keyEquals =
                    functionAndTypeManager.getJavaScalarFunctionImplementation(
                            functionAndTypeManager.resolveOperator(OperatorType.EQUAL, fromTypes(keyType, keyType))).getMethodHandle();
            MethodHandle keyHashcode =
                    functionAndTypeManager.getJavaScalarFunctionImplementation(
                            functionAndTypeManager.resolveOperator(OperatorType.HASH_CODE, fromTypes(keyType))).getMethodHandle();
            RowExpression map = call(functionAndTypeManager, "MAP", new MapType(keyType, valueType, keyEquals, keyHashcode), keyArray, valueArray);
            return call(functionAndTypeManager, "element_at", valueType, map, mapIndex);
        }
    }

    @Override
    public boolean isRewriterEnabled(Session session)
    {
        return session.getSystemProperty(REWRITE_CASE_TO_MAP_ENABLED, Boolean.class);
    }

    @Override
    public Set<Rule<?>> rules()
    {
        return ImmutableSet.of(
                projectRowExpressionRewriteRule(),
                filterRowExpressionRewriteRule(),
                joinRowExpressionRewriteRule());
    }
}
