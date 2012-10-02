package com.facebook.presto.sql.tree;

import com.facebook.presto.sql.compiler.Schema;
import com.google.common.base.Joiner;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;

import java.io.PrintStream;
import java.util.Map;

public class TreePrinter
{
    private static final String INDENT = "   ";

    private final Map<QualifiedNameReference, QualifiedName> resolvedNameReferences;
    private final ImmutableMap<Node, Schema> outputSchemas;
    private final PrintStream out;

    public TreePrinter(Map<QualifiedNameReference, QualifiedName> resolvedNameReferences,
            Map<Node, Schema> outputSchemas,
            PrintStream out)
    {
        this.resolvedNameReferences = ImmutableMap.copyOf(resolvedNameReferences);
        this.outputSchemas = ImmutableMap.copyOf(outputSchemas);
        this.out = out;
    }

    public void print(Node root)
    {
        AstVisitor<Void, Integer> printer = new DefaultTraversalVisitor<Void, Integer>()
        {
            @Override
            public Void visitNode(Node node, Integer indentLevel)
            {
                throw new UnsupportedOperationException("not yet implemented: " + node);
            }

            @Override
            public Void visitQuery(Query node, Integer indentLevel)
            {
                print(indentLevel, "Query " + outputSchemas.get(node));

                indentLevel++;
                process(node.getSelect(), indentLevel);

                print(indentLevel, "From");
                for (Relation relation : node.getFrom()) {
                    process(relation, indentLevel + 1);
                }

                if (node.getWhere() != null) {
                    print(indentLevel, "Where");
                    process(node.getWhere(), indentLevel + 1);
                }

                if (!node.getGroupBy().isEmpty()) {
                    print(indentLevel, "GroupBy");
                    for (Expression expression : node.getGroupBy()) {
                        process(expression, indentLevel + 1);
                    }
                }

                if (node.getHaving() != null) {
                    print(indentLevel, "Having");
                    process(node.getHaving(), indentLevel + 1);
                }

                if (!node.getOrderBy().isEmpty()) {
                    print(indentLevel, "OrderBy");
                    for (SortItem sortItem : node.getOrderBy()) {
                        process(sortItem, indentLevel + 1);
                    }
                }

                if (node.getLimit() != null) {
                    print(indentLevel, "Limit: " + node.getLimit());
                }

                return null;
            }

            @Override
            public Void visitSelect(Select node, Integer indentLevel)
            {
                String distinct = "";
                if (node.isDistinct()) {
                    distinct = "[DISTINCT]";
                }
                print(indentLevel, "Select" + distinct);

                super.visitSelect(node, indentLevel + 1); // visit children

                return null;
            }

            @Override
            public Void visitComparisonExpression(ComparisonExpression node, Integer indentLevel)
            {
                print(indentLevel, node.getType().toString());

                super.visitComparisonExpression(node, indentLevel + 1);

                return null;
            }

            @Override
            public Void visitArithmeticExpression(ArithmeticExpression node, Integer indentLevel)
            {
                print(indentLevel, node.getType().toString());

                super.visitArithmeticExpression(node, indentLevel + 1);

                return null;
            }

            @Override
            public Void visitLogicalBinaryExpression(LogicalBinaryExpression node, Integer indentLevel)
            {
                print(indentLevel, node.getType().toString());

                super.visitLogicalBinaryExpression(node, indentLevel + 1);

                return null;
            }

            @Override
            public Void visitStringLiteral(StringLiteral node, Integer indentLevel)
            {
                print(indentLevel, "String[" + node.getValue() + "]");
                return null;
            }

            @Override
            public Void visitLongLiteral(LongLiteral node, Integer indentLevel)
            {
                print(indentLevel, "Long[" + node.getValue() + "]");
                return null;
            }

            @Override
            public Void visitLikePredicate(LikePredicate node, Integer indentLevel)
            {
                print(indentLevel, "LIKE");

                super.visitLikePredicate(node, indentLevel + 1);

                return null;
            }

            @Override
            public Void visitQualifiedNameReference(QualifiedNameReference node, Integer indentLevel)
            {
                QualifiedName resolved = resolvedNameReferences.get(node);
                String resolvedName = "";
                if (resolved != null) {
                    resolvedName = "=>" + resolved.toString();
                }
                print(indentLevel, "QualifiedName[" + node.getName() + resolvedName + "]");
                return null;
            }

            @Override
            public Void visitFunctionCall(FunctionCall node, Integer indentLevel)
            {
                String name = Joiner.on('.').join(node.getName().getParts());
                print(indentLevel, "FunctionCall[" + name + "]");

                super.visitFunctionCall(node, indentLevel + 1);

                return null;
            }

            @Override
            public Void visitTable(Table node, Integer indentLevel)
            {
                String name = Joiner.on('.').join(node.getName().getParts());
                print(indentLevel, "Table[" + name + "] " + outputSchemas.get(node));

                return null;
            }

            @Override
            public Void visitAliasedRelation(AliasedRelation node, Integer indentLevel)
            {
                print(indentLevel, "Alias[" + node.getAlias() + "] " + outputSchemas.get(node));

                super.visitAliasedRelation(node, indentLevel + 1);

                return null;
            }

            @Override
            public Void visitAliasedExpression(AliasedExpression node, Integer indentLevel)
            {
                print(indentLevel, "Alias[" + node.getAlias() + "]");

                super.visitAliasedExpression(node, indentLevel + 1);

                return null;
            }

            @Override
            public Void visitSubquery(Subquery node, Integer indentLevel)
            {
                print(indentLevel, "SubQuery " + outputSchemas.get(node));

                super.visitSubquery(node, indentLevel + 1);

                return null;
            }

            @Override
            protected Void visitInPredicate(InPredicate node, Integer indentLevel)
            {
                print(indentLevel, "IN");

                super.visitInPredicate(node, indentLevel + 1);

                return null;
            }

            @Override
            protected Void visitSubqueryExpression(SubqueryExpression node, Integer indentLevel)
            {
                print(indentLevel, "SubQuery " + outputSchemas.get(node));

                super.visitSubqueryExpression(node, indentLevel + 1);

                return null;
            }
        };

        printer.process(root, 0);
    }

    private void print(Integer indentLevel, String value)
    {
        out.println(Strings.repeat(INDENT, indentLevel) + value);
    }
}
