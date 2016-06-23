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
package com.facebook.presto.sql.planner;

import com.facebook.presto.Session;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.sql.analyzer.Analysis;
import com.facebook.presto.sql.planner.plan.ApplyNode;
import com.facebook.presto.sql.planner.plan.EnforceSingleRowNode;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.InPredicate;
import com.facebook.presto.sql.tree.Node;
import com.facebook.presto.sql.tree.SubqueryExpression;
import com.facebook.presto.sql.tree.SymbolReference;
import com.google.common.collect.ImmutableList;

import java.util.Collection;
import java.util.Set;

import static com.facebook.presto.sql.util.AstUtils.nodeContains;
import static com.facebook.presto.util.ImmutableCollectors.toImmutableSet;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.Iterables.getOnlyElement;
import static java.util.Objects.requireNonNull;

class SubqueryPlanner
{
    private final Analysis analysis;
    private final SymbolAllocator symbolAllocator;
    private final PlanNodeIdAllocator idAllocator;
    private final Metadata metadata;
    private final Session session;

    SubqueryPlanner(Analysis analysis, SymbolAllocator symbolAllocator, PlanNodeIdAllocator idAllocator, Metadata metadata, Session session)
    {
        requireNonNull(analysis, "analysis is null");
        requireNonNull(symbolAllocator, "symbolAllocator is null");
        requireNonNull(idAllocator, "idAllocator is null");
        requireNonNull(metadata, "metadata is null");
        requireNonNull(session, "session is null");

        this.analysis = analysis;
        this.symbolAllocator = symbolAllocator;
        this.idAllocator = idAllocator;
        this.metadata = metadata;
        this.session = session;
    }

    public PlanBuilder handleSubqueries(PlanBuilder builder, Collection<Expression> expressions, Node node)
    {
        for (Expression expression : expressions) {
            builder = handleSubqueries(builder, expression, node);
        }
        return builder;
    }

    public PlanBuilder handleSubqueries(PlanBuilder builder, Expression expression, Node node)
    {
        builder = appendInPredicateApplyNodes(
                builder,
                analysis.getInPredicateSubqueries(node)
                        .stream()
                        .filter(inPredicate -> nodeContains(expression, inPredicate.getValueList()))
                        .collect(toImmutableSet()));
        builder = appendScalarSubqueryApplyNodes(
                builder,
                analysis.getScalarSubqueries(node)
                        .stream()
                        .filter(subquery -> nodeContains(expression, subquery))
                        .collect(toImmutableSet()));
        return builder;
    }

    private PlanBuilder appendInPredicateApplyNodes(PlanBuilder subPlan, Set<InPredicate> inPredicates)
    {
        for (InPredicate inPredicate : inPredicates) {
            subPlan = appendInPredicateApplyNode(subPlan, inPredicate);
        }
        return subPlan;
    }

    private PlanBuilder appendInPredicateApplyNode(PlanBuilder subPlan, InPredicate inPredicate)
    {
        subPlan = subPlan.appendProjections(ImmutableList.of(inPredicate.getValue()), symbolAllocator, idAllocator);

        checkState(inPredicate.getValueList() instanceof SubqueryExpression);
        RelationPlan valueListRelation = createRelationPlan((SubqueryExpression) inPredicate.getValueList());

        TranslationMap translationMap = subPlan.copyTranslations();
        SymbolReference valueList = getOnlyElement(valueListRelation.getOutputSymbols()).toSymbolReference();
        translationMap.put(inPredicate, new InPredicate(inPredicate.getValue(), valueList));

        return new PlanBuilder(translationMap,
                // TODO handle correlation
                new ApplyNode(idAllocator.getNextId(),
                        subPlan.getRoot(),
                        valueListRelation.getRoot(),
                        ImmutableList.of()),
                subPlan.getSampleWeight());
    }

    private PlanBuilder appendScalarSubqueryApplyNodes(PlanBuilder builder, Set<SubqueryExpression> scalarSubqueries)
    {
        for (SubqueryExpression scalarSubquery : scalarSubqueries) {
            builder = appendScalarSubqueryApplyNode(builder, scalarSubquery);
        }
        return builder;
    }

    private PlanBuilder appendScalarSubqueryApplyNode(PlanBuilder subPlan, SubqueryExpression scalarSubquery)
    {
        if (subPlan.canTranslate(scalarSubquery)) {
            // given subquery is already appended
            return subPlan;
        }

        EnforceSingleRowNode enforceSingleRowNode = new EnforceSingleRowNode(idAllocator.getNextId(), createRelationPlan(scalarSubquery).getRoot());

        TranslationMap translations = subPlan.copyTranslations();
        translations.put(scalarSubquery, getOnlyElement(enforceSingleRowNode.getOutputSymbols()));

        PlanNode root = subPlan.getRoot();
        if (root.getOutputSymbols().isEmpty()) {
            // there is nothing to join with - e.g. SELECT (SELECT 1)
            return new PlanBuilder(translations, enforceSingleRowNode, subPlan.getSampleWeight());
        }
        else {
            return new PlanBuilder(translations,
                    // TODO handle parameter list
                    new ApplyNode(idAllocator.getNextId(),
                            root,
                            enforceSingleRowNode,
                            ImmutableList.of()),
                    subPlan.getSampleWeight());
        }
    }

    private RelationPlan createRelationPlan(SubqueryExpression subqueryExpression)
    {
        return new RelationPlanner(analysis, symbolAllocator, idAllocator, metadata, session)
                .process(subqueryExpression.getQuery(), null);
    }
}
