package com.facebook.presto.sql.planner;

import com.facebook.presto.sql.analyzer.FieldOrExpression;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.sql.tree.Expression;
import com.google.common.base.Preconditions;

class PlanBuilder
{
    private final TranslationMap translations;
    private final PlanNode root;

    public PlanBuilder(TranslationMap translations, PlanNode root)
    {
        Preconditions.checkNotNull(translations, "translations is null");
        Preconditions.checkNotNull(root, "root is null");

        this.translations = translations;
        this.root = root;
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
