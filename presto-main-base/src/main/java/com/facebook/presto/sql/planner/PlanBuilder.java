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
import com.facebook.presto.spi.VariableAllocator;
import com.facebook.presto.spi.plan.Assignments;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.PlanNodeIdAllocator;
import com.facebook.presto.spi.plan.ProjectNode;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.analyzer.Analysis;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.LambdaArgumentDeclaration;
import com.facebook.presto.sql.tree.NodeRef;
import com.google.common.collect.ImmutableMap;

import java.util.Map;

import static com.facebook.presto.sql.planner.PlannerUtils.newVariable;
import static com.facebook.presto.sql.planner.TranslateExpressionsUtil.toRowExpression;
import static java.util.Objects.requireNonNull;

class PlanBuilder
{
    private final TranslationMap translations;
    private final PlanNode root;

    public PlanBuilder(TranslationMap translations, PlanNode root)
    {
        requireNonNull(translations, "translations is null");
        requireNonNull(root, "root is null");

        this.translations = translations;
        this.root = root;
    }

    public static PlanBuilder newPlanBuilder(
            RelationPlan plan,
            Analysis analysis,
            Map<NodeRef<LambdaArgumentDeclaration>, VariableReferenceExpression> lambdaArguments)
    {
        return new PlanBuilder(
                new TranslationMap(plan, analysis, lambdaArguments),
                plan.getRoot());
    }

    public TranslationMap copyTranslations()
    {
        TranslationMap translations = new TranslationMap(getRelationPlan(), getAnalysis(), getTranslations().getLambdaDeclarationToVariableMap());
        translations.copyMappingsFrom(getTranslations());
        return translations;
    }

    private Analysis getAnalysis()
    {
        return translations.getAnalysis();
    }

    public PlanBuilder withNewRoot(PlanNode root)
    {
        return new PlanBuilder(translations, root);
    }

    public RelationPlan getRelationPlan()
    {
        return translations.getRelationPlan();
    }

    public PlanNode getRoot()
    {
        return root;
    }

    public boolean canTranslate(Expression expression)
    {
        return translations.containsSymbol(expression);
    }

    public VariableReferenceExpression translate(Expression expression)
    {
        return translations.get(expression);
    }

    public VariableReferenceExpression translateToVariable(Expression expression)
    {
        return translations.get(expression);
    }

    public Expression rewrite(Expression expression)
    {
        return translations.rewrite(expression);
    }

    public TranslationMap getTranslations()
    {
        return translations;
    }

    public PlanBuilder appendProjections(
            Iterable<Expression> expressions,
            VariableAllocator variableAllocator,
            PlanNodeIdAllocator idAllocator,
            Session session,
            Metadata metadata,
            SqlParser sqlParser,
            Analysis analysis,
            SqlPlannerContext context)
    {
        TranslationMap translations = copyTranslations();

        Assignments.Builder projections = Assignments.builder();

        // add an identity projection for underlying plan
        for (VariableReferenceExpression variable : getRoot().getOutputVariables()) {
            projections.put(variable, variable);
        }

        ImmutableMap.Builder<VariableReferenceExpression, Expression> newTranslations = ImmutableMap.builder();
        for (Expression expression : expressions) {
            VariableReferenceExpression variable = newVariable(variableAllocator, expression, getAnalysis().getTypeWithCoercions(expression));
            projections.put(variable, rowExpression(translations.rewrite(expression), context, session, metadata, sqlParser, analysis, variableAllocator));
            newTranslations.put(variable, expression);
        }
        // Now append the new translations into the TranslationMap
        for (Map.Entry<VariableReferenceExpression, Expression> entry : newTranslations.build().entrySet()) {
            translations.put(entry.getValue(), entry.getKey());
        }

        return new PlanBuilder(translations, new ProjectNode(idAllocator.getNextId(), getRoot(), projections.build()));
    }

    private RowExpression rowExpression(Expression expression, SqlPlannerContext context, Session session, Metadata metadata, SqlParser sqlParser, Analysis analysis, VariableAllocator variableAllocator)
    {
        return toRowExpression(
                expression,
                metadata,
                session,
                sqlParser,
                variableAllocator,
                analysis,
                context.getTranslatorContext());
    }
}
