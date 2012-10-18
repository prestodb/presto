package com.facebook.presto.sql.compiler.transforms;

import com.facebook.presto.sql.compiler.NameGenerator;
import com.facebook.presto.sql.compiler.NodeRewriter;
import com.facebook.presto.sql.compiler.TreeRewriter;
import com.facebook.presto.sql.tree.AliasedExpression;
import com.facebook.presto.sql.tree.AllColumns;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.FunctionCall;
import com.facebook.presto.sql.tree.QualifiedNameReference;
import com.facebook.presto.sql.tree.Select;
import com.google.common.collect.ImmutableList;

import static com.facebook.presto.sql.compiler.IterableUtils.sameElements;

/**
 * Rewrites
 *
 * <pre>
 * SELECT a foo, b
 * FROM (
 *  SELECT T.a, b, SUM(c)
 *  FROM T
 *  GROUP BY ...
 * ) U
 * </pre>
 *
 * as
 *
 * <pre>
 * SELECT a foo, b b
 * FROM (
 *  SELECT T.a a, b b, SUM(c) _a0
 *  FROM T
 *  GROUP BY ...
 * ) U
 * </pre>
 */
public class MaterializeImplicitAliases
    extends NodeRewriter<Void>
{
    private final NameGenerator namer;

    public MaterializeImplicitAliases(NameGenerator namer)
    {
        this.namer = namer;
    }

    @Override
    public Select rewriteSelect(Select node, Void context, TreeRewriter<Void> treeRewriter)
    {
        // process expressions recursively
        Select rewritten = treeRewriter.defaultRewrite(node, context);

        ImmutableList.Builder<Expression> builder = ImmutableList.builder();
        for (Expression expression : rewritten.getSelectItems()) {
            if (expression instanceof AliasedExpression || expression instanceof AllColumns) {
                builder.add(expression);
                continue;
            }

            String alias;
            if (expression instanceof QualifiedNameReference) {
                alias = ((QualifiedNameReference) expression).getSuffix().toString();
            }
            else if (expression instanceof FunctionCall) {
                alias = ((FunctionCall) expression).getName().getSuffix();
            }
            else {
                alias = namer.newFieldAlias();
            }

            builder.add(new AliasedExpression(expression, alias));
        }

        if (!sameElements(rewritten.getSelectItems(), builder.build())) {
            return new Select(rewritten.isDistinct(), builder.build());
        }

        return rewritten;
    }
}
