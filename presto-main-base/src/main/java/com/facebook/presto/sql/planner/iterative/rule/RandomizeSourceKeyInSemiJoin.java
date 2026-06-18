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
import com.facebook.presto.expressions.LogicalRowExpressions;
import com.facebook.presto.matching.Captures;
import com.facebook.presto.matching.Pattern;
import com.facebook.presto.metadata.FunctionAndTypeManager;
import com.facebook.presto.spi.plan.Assignments;
import com.facebook.presto.spi.plan.ProjectNode;
import com.facebook.presto.spi.plan.SemiJoinNode;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.SpecialFormExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.analyzer.FeaturesConfig;
import com.facebook.presto.sql.planner.PlannerUtils;
import com.facebook.presto.sql.planner.iterative.Rule;
import com.google.common.collect.ImmutableMap;

import java.util.Optional;
import java.util.stream.Stream;

import static com.facebook.presto.SystemSessionProperties.getRandomizeNullSourceKeyInSemiJoinStrategy;
import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.BooleanType.BOOLEAN;
import static com.facebook.presto.common.type.DateType.DATE;
import static com.facebook.presto.common.type.IntegerType.INTEGER;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static com.facebook.presto.metadata.CastType.CAST;
import static com.facebook.presto.spi.plan.ProjectNode.Locality.LOCAL;
import static com.facebook.presto.sql.planner.plan.Patterns.semiJoin;
import static com.facebook.presto.sql.relational.Expressions.call;
import static com.facebook.presto.sql.relational.Expressions.constantNull;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static java.util.Objects.requireNonNull;
import static java.util.function.Function.identity;

/**
 * Randomizes semi-join source keys to improve hash distribution and avoid data skew on NULL values in semi-join.
 *
 * <p>The rule transforms a semi-join with potentially skewed keys:
 * <pre>
 * - SemiJoin
 *      output: semi_output
 *      condition: source_key = filtering_key
 *      - Source (table scan)
 *          source_key (value are skewed on NULLs)
 *      - FilteringSource (table scan)
 *          filtering_key
 * </pre>
 * <p>
 * into a semi-join with randomized keys and NULL-aware logic:
 * <pre>
 * - Project
 *      semi_output := new_semi_output OR (source_key IS NULL ? NULL : FALSE)
 *      - SemiJoin
 *          output: new_semi_output
 *          condition: randomized_source_key = cast_filtering_key
 *          - Project
 *              randomized_source_key := COALESCE(source_key, Randomize(source_key))
 *              - Source (table scan)
 *                  source_key
 *          - Project
 *              cast_filtering_key := CAST(filtering_key AS VARCHAR)
 *              - FilteringSource (table scan)
 *                  filtering_key
 * </pre>
 * Since the randomization will turn the semi join output for NULL source key to be false, we add one more projection
 * semi_output := new_semi_output OR (source_key IS NULL ? NULL : FALSE) to project the semi join output back to NULL.
 */
public class RandomizeSourceKeyInSemiJoin
        implements Rule<SemiJoinNode>
{
    private static final String LEFT_PREFIX = "l";
    private final FunctionAndTypeManager functionAndTypeManager;

    public RandomizeSourceKeyInSemiJoin(FunctionAndTypeManager functionAndTypeManager)
    {
        this.functionAndTypeManager = requireNonNull(functionAndTypeManager, "functionAndTypeManager is null");
    }

    private static boolean isSupportedType(VariableReferenceExpression variable)
    {
        return Stream.of(INTEGER, BIGINT, DATE).anyMatch(x -> x.equals(variable.getType()));
    }

    @Override
    public Pattern<SemiJoinNode> getPattern()
    {
        return semiJoin();
    }

    @Override
    public boolean isEnabled(Session session)
    {
        return getRandomizeNullSourceKeyInSemiJoinStrategy(session).equals(FeaturesConfig.RandomizeNullSourceKeyInSemiJoinStrategy.ALWAYS);
    }

    @Override
    public Result apply(SemiJoinNode node, Captures captures, Context context)
    {
        if ((node.getDistributionType().isPresent() && node.getDistributionType().get().equals(SemiJoinNode.DistributionType.REPLICATED))) {
            return Result.empty();
        }

        // Only process supported types
        if (!isSupportedType(node.getSourceJoinVariable()) || !isSupportedType(node.getFilteringSourceJoinVariable())) {
            return Result.empty();
        }

        VariableReferenceExpression randomizedSourceKey;
        RowExpression sourceRandomExpression = PlannerUtils.randomizeJoinKey(context.getSession(), functionAndTypeManager, node.getSourceJoinVariable(), LEFT_PREFIX);
        randomizedSourceKey = context.getVariableAllocator().newVariable(
                sourceRandomExpression,
                RandomizeSourceKeyInSemiJoin.class.getSimpleName());

        // Create project nodes to add randomized keys
        Assignments.Builder sourceAssignments = Assignments.builder();
        sourceAssignments.putAll(node.getSource().getOutputVariables().stream()
                .collect(toImmutableMap(identity(), identity())));
        sourceAssignments.put(randomizedSourceKey, sourceRandomExpression);

        ProjectNode newSource = new ProjectNode(
                node.getSource().getSourceLocation(),
                context.getIdAllocator().getNextId(),
                node.getSource().getStatsEquivalentPlanNode(),
                node.getSource(),
                sourceAssignments.build(),
                LOCAL);

        RowExpression newFilterExpression = call("CAST", functionAndTypeManager.lookupCast(CAST, node.getFilteringSourceJoinVariable().getType(), VARCHAR), VARCHAR, node.getFilteringSourceJoinVariable());
        VariableReferenceExpression newFilteringSourceKey = context.getVariableAllocator().newVariable(newFilterExpression);

        Assignments.Builder filteringSourceAssignments = Assignments.builder();
        filteringSourceAssignments.putAll(node.getFilteringSource().getOutputVariables().stream()
                .collect(toImmutableMap(identity(), identity())));
        filteringSourceAssignments.put(newFilteringSourceKey, newFilterExpression);

        ProjectNode newFilteringSource = new ProjectNode(
                node.getFilteringSource().getSourceLocation(),
                context.getIdAllocator().getNextId(),
                node.getFilteringSource().getStatsEquivalentPlanNode(),
                node.getFilteringSource(),
                filteringSourceAssignments.build(),
                LOCAL);

        VariableReferenceExpression newSemiJoinOutput = context.getVariableAllocator().newVariable(node.getSemiJoinOutput());
        SemiJoinNode newSemiJoin = new SemiJoinNode(
                node.getSourceLocation(),
                context.getIdAllocator().getNextId(),
                node.getStatsEquivalentPlanNode(),
                newSource,
                newFilteringSource,
                randomizedSourceKey,
                newFilteringSourceKey,
                newSemiJoinOutput,
                Optional.empty(),
                Optional.empty(),
                node.getDistributionType(),
                ImmutableMap.of());
        RowExpression outputExpression = LogicalRowExpressions.or(
                newSemiJoinOutput,
                new SpecialFormExpression(
                        node.getSemiJoinOutput().getSourceLocation(),
                        SpecialFormExpression.Form.IF,
                        BOOLEAN,
                        new SpecialFormExpression(
                                SpecialFormExpression.Form.IS_NULL,
                                BOOLEAN,
                                node.getSourceJoinVariable()),
                        constantNull(BOOLEAN),
                        LogicalRowExpressions.FALSE_CONSTANT));
        Assignments.Builder outputAssignments = Assignments.builder();
        outputAssignments.putAll(node.getOutputVariables().stream().collect(toImmutableMap(identity(), x -> x.equals(node.getSemiJoinOutput()) ? outputExpression : x)));
        return Result.ofPlanNode(new ProjectNode(
                node.getSourceLocation(),
                context.getIdAllocator().getNextId(),
                node.getStatsEquivalentPlanNode(),
                newSemiJoin,
                outputAssignments.build(),
                LOCAL));
    }
}
