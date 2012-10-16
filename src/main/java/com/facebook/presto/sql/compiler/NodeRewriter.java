package com.facebook.presto.sql.compiler;

import com.facebook.presto.sql.compiler.TreeRewriter;
import com.facebook.presto.sql.tree.AliasedExpression;
import com.facebook.presto.sql.tree.AliasedRelation;
import com.facebook.presto.sql.tree.AllColumns;
import com.facebook.presto.sql.tree.ArithmeticExpression;
import com.facebook.presto.sql.tree.ComparisonExpression;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.FunctionCall;
import com.facebook.presto.sql.tree.InPredicate;
import com.facebook.presto.sql.tree.LikePredicate;
import com.facebook.presto.sql.tree.Literal;
import com.facebook.presto.sql.tree.LogicalBinaryExpression;
import com.facebook.presto.sql.tree.Node;
import com.facebook.presto.sql.tree.QualifiedNameReference;
import com.facebook.presto.sql.tree.Query;
import com.facebook.presto.sql.tree.Relation;
import com.facebook.presto.sql.tree.Select;
import com.facebook.presto.sql.tree.Subquery;
import com.facebook.presto.sql.tree.SubqueryExpression;
import com.facebook.presto.sql.tree.Table;

public class NodeRewriter<C>
{
    public Node rewriteNode(Node node, C context, TreeRewriter<C> treeRewriter)
    {
        return null;
    }

    public Node rewriteStatement(Node node, C context, TreeRewriter<C> treeRewriter)
    {
        return rewriteNode(node, context, treeRewriter);
    }

    public Node rewriteQuery(Query node, C context, TreeRewriter<C> treeRewriter)
    {
        return rewriteStatement(node, context, treeRewriter);
    }

    public Node rewriteSelect(Select node, C context, TreeRewriter<C> treeRewriter)
    {
        return rewriteNode(node, context, treeRewriter);
    }

    public Node rewriteRelation(Relation node, C context, TreeRewriter<C> treeRewriter)
    {
        return rewriteNode(node, context, treeRewriter);
    }

    public Node rewriteTable(Table node, C context, TreeRewriter<C> treeRewriter)
    {
        return rewriteRelation(node, context, treeRewriter);
    }

    public Node rewriteAliasedRelation(AliasedRelation node, C context, TreeRewriter<C> treeRewriter)
    {
        return rewriteRelation(node, context, treeRewriter);
    }

    public Node rewriteSubquery(Subquery node, C context, TreeRewriter<C> treeRewriter)
    {
        return rewriteRelation(node, context, treeRewriter);
    }

    public Node rewriteExpression(Expression node, C context, TreeRewriter<C> treeRewriter)
    {
        return rewriteNode(node, context, treeRewriter);
    }

    public Node rewriteAliasedExpression(AliasedExpression node, C context, TreeRewriter<C> treeRewriter)
    {
        return rewriteExpression(node, context, treeRewriter);
    }

    public Node rewriteArithmeticExpression(ArithmeticExpression node, C context, TreeRewriter<C> treeRewriter)
    {
        return rewriteExpression(node, context, treeRewriter);
    }

    public Node rewriteComparisonExpression(ComparisonExpression node, C context, TreeRewriter<C> treeRewriter)
    {
        return rewriteExpression(node, context, treeRewriter);
    }

    public Node rewriteLogicalBinaryExpression(LogicalBinaryExpression node, C context, TreeRewriter<C> treeRewriter)
    {
        return rewriteExpression(node, context, treeRewriter);
    }

    public Node rewriteFunctionCall(FunctionCall node, C context, TreeRewriter<C> treeRewriter)
    {
        return rewriteExpression(node, context, treeRewriter);
    }

    public Node rewriteLikePredicate(LikePredicate node, C context, TreeRewriter<C> treeRewriter)
    {
        return rewriteExpression(node, context, treeRewriter);
    }

    public Node rewriteInPredicate(InPredicate node, C context, TreeRewriter<C> treeRewriter)
    {
        return rewriteExpression(node, context, treeRewriter);
    }

    public Node rewriteSubqueryExpression(SubqueryExpression node, C context, TreeRewriter<C> treeRewriter)
    {
        return rewriteExpression(node, context, treeRewriter);
    }

    public Node rewriteAllColumns(AllColumns node, C context, TreeRewriter<C> treeRewriter)
    {
        return rewriteExpression(node, context, treeRewriter);
    }

    public Node rewriteLiteral(Literal node, C context, TreeRewriter<C> treeRewriter)
    {
        return rewriteExpression(node, context, treeRewriter);
    }

    public Node rewriteQualifiedNameReference(QualifiedNameReference node, C context, TreeRewriter<C> treeRewriter)
    {
        return rewriteExpression(node, context, treeRewriter);
    }
}
