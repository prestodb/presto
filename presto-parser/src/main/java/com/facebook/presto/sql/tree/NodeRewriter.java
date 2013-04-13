package com.facebook.presto.sql.tree;

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

    public Node rewriteWith(With node, C context, TreeRewriter<C> treeRewriter)
    {
        return rewriteNode(node, context, treeRewriter);
    }

    public Node rewriteWithQuery(WithQuery node, C context, TreeRewriter<C> treeRewriter)
    {
        return rewriteNode(node, context, treeRewriter);
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

    public Node rewriteNegativeExpression(NegativeExpression node, C context, TreeRewriter<C> treeRewriter)
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

    public Node rewriteBetweenPredicate(BetweenPredicate node, C context, TreeRewriter<C> treeRewriter)
    {
        return rewriteExpression(node, context, treeRewriter);
    }

    public Node rewriteLogicalBinaryExpression(LogicalBinaryExpression node, C context, TreeRewriter<C> treeRewriter)
    {
        return rewriteExpression(node, context, treeRewriter);
    }

    public Node rewriteNotExpression(NotExpression node, C context, TreeRewriter<C> treeRewriter)
    {
        return rewriteExpression(node, context, treeRewriter);
    }

    public Node rewriteIsNullPredicate(IsNullPredicate node, C context, TreeRewriter<C> treeRewriter)
    {
        return rewriteExpression(node, context, treeRewriter);
    }

    public Node rewriteIsNotNullPredicate(IsNotNullPredicate node, C context, TreeRewriter<C> treeRewriter)
    {
        return rewriteExpression(node, context, treeRewriter);
    }

    public Node rewriteNullIfExpression(NullIfExpression node, C context, TreeRewriter<C> treeRewriter)
    {
        return rewriteExpression(node, context, treeRewriter);
    }

    public Node rewriteSearchedCaseExpression(SearchedCaseExpression node, C context, TreeRewriter<C> treeRewriter)
    {
        return rewriteExpression(node, context, treeRewriter);
    }

    public Node rewriteSimpleCaseExpression(SimpleCaseExpression node, C context, TreeRewriter<C> treeRewriter)
    {
        return rewriteExpression(node, context, treeRewriter);
    }

    public Node rewriteWhenClause(WhenClause node, C context, TreeRewriter<C> treeRewriter)
    {
        return rewriteExpression(node, context, treeRewriter);
    }

    public Node rewriteCoalesceExpression(CoalesceExpression node, C context, TreeRewriter<C> treeRewriter)
    {
        return rewriteExpression(node, context, treeRewriter);
    }

    public Node rewriteInListExpression(InListExpression node, C context, TreeRewriter<C> treeRewriter)
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

    public Node rewriteExtract(Extract node, C context, TreeRewriter<C> treeRewriter)
    {
        return rewriteExpression(node, context, treeRewriter);
    }

    public Node rewriteCurrentTime(CurrentTime node, C context, TreeRewriter<C> treeRewriter)
    {
        return rewriteExpression(node, context, treeRewriter);
    }

    public Node rewriteCast(Cast node, C context, TreeRewriter<C> treeRewriter)
    {
        return rewriteExpression(node, context, treeRewriter);
    }

    public Node rewriteSortItem(SortItem node, C context, TreeRewriter<C> treeRewriter)
    {
        return rewriteNode(node, context, treeRewriter);
    }
}
