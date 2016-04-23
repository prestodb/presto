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

import com.facebook.presto.sql.analyzer.FieldOrExpression;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.sql.tree.Expression;

import java.util.Optional;

import static java.util.Objects.requireNonNull;

class PlanBuilder
{
    private final TranslationMap translations;
    private final PlanNode root;
    private final Optional<Symbol> sampleWeight;

    public PlanBuilder(TranslationMap translations, PlanNode root, Optional<Symbol> sampleWeight)
    {
        requireNonNull(translations, "translations is null");
        requireNonNull(root, "root is null");
        requireNonNull(sampleWeight, "sampleWeight is null");

        this.translations = translations;
        this.root = root;
        this.sampleWeight = sampleWeight;
    }

    public PlanBuilder withNewRoot(PlanNode root)
    {
        return new PlanBuilder(translations, root, sampleWeight);
    }

    public Optional<Symbol> getSampleWeight()
    {
        return sampleWeight;
    }

    public RelationPlan getRelationPlan()
    {
        return translations.getRelationPlan();
    }

    public PlanNode getRoot()
    {
        return root;
    }

    public Symbol translate(Expression expression)
    {
        return translations.get(expression);
    }

    public Symbol translate(FieldOrExpression fieldOrExpression)
    {
        return translations.get(fieldOrExpression);
    }

    public Expression rewrite(Expression expression)
    {
        return translations.rewrite(expression);
    }

    public Expression rewrite(FieldOrExpression fieldOrExpression)
    {
        return translations.rewrite(fieldOrExpression);
    }

    public TranslationMap getTranslations()
    {
        return translations;
    }
}
