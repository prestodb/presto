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
package com.facebook.presto.sql.analyzer.crux;

/**
 * A base class for visiting SemanticTree nodes.
 *
 * <p>Derived classes override the various visitXXX() methods to add custom logic for a given node.
 *
 * <p>Derived classes call super.visitXXX() to visit all of the children of a given node.
 */
public abstract class SemanticTreeVisitor
{
    public void visitSemanticTree(SemanticTree node)
    {
        switch (node.getSemanticTreeKind()) {
            case STATEMENT:
                visitStatement(node.asStatement());
                break;
            case EXPRESSION:
                visitExpression(node.asExpression());
                break;
            case SELECT_ITEM:
                visitSelectItem(node.asSelectItem());
                break;
            default:
                throw new UnsupportedOperationException(
                        "Missing case: " + node.getSemanticTreeKind().name());
        }
    }

    public void visitStatement(Statement node)
    {
        switch (node.getStatementKind()) {
            case QUERY:
                visitQuery(node.asQuery());
                break;
            default:
                throw new UnsupportedOperationException(
                        "Missing case: " + node.getSemanticTreeKind().name());
        }
    }

    public void visitQuery(Query node)
    {
        switch (node.getQueryKind()) {
            case EMPTY:
                visitEmptyQuery(node.asEmpty());
                break;
            case SELECT:
                visitSelectQuery(node.asSelect());
                break;
            case DATA_SET:
                visitDataSet(node.asDataSet());
                break;
            case ALIAS:
                visitAliasQuery(node.asAlias());
                break;
            default:
                throw new UnsupportedOperationException("Missing case: " + node.getQueryKind().name());
        }
    }

    public void visitEmptyQuery(EmptyQuery node)
    {
        // nothing
    }

    public void visitSelectQuery(SelectQuery node)
    {
        visitQuery(node.getQuery());
        for (SelectItem item : node.getItems()) {
            visitSelectItem(item);
        }
    }

    public void visitSelectItem(SelectItem node)
    {
        visitExpression(node.getValue());
    }

    public void visitDataSet(DataSet node)
    {
        switch (node.getDataSetKind()) {
            case HIVE:
                visitHiveDataSet(node.asHive());
                break;
            default:
                throw new UnsupportedOperationException("Missing case: " + node.getDataSetKind().name());
        }
    }

    public void visitHiveDataSet(HiveDataSet node)
    {
        // nothing
    }

    public void visitAliasQuery(AliasQuery node)
    {
        visitQuery(node.getParent());
    }

    public void visitExpression(Expression node)
    {
        switch (node.getExpressionKind()) {
            case COLUMN_REFERENCE:
                visitColumnReferenceExpression(node.asColumnReference());
                break;
            case LITERAL:
                visitLiteralExpression(node.asLiteral());
                break;
            case CALL:
                visitCallExpression(node.asCall());
                break;
            case FUNCTION:
                visitFunctionExpression(node.asFunction());
                break;
            default:
                throw new UnsupportedOperationException("Missing case: " + node.getExpressionKind().name());
        }
    }

    public void visitColumnReferenceExpression(ColumnReferenceExpression node)
    {
        // nothing - Note that node.getQuery() will typically get visited from
        // a parent in the query chain
    }

    public void visitLiteralExpression(LiteralExpression node)
    {
        // nothing
    }

    public void visitFunctionExpression(FunctionExpression node)
    {
        visitConcreteFunctionOverload(node.getFunction());
        visitFunctionOverload(node.getSourceOverload());
    }

    public void visitConcreteFunctionOverload(ConcreteFunctionOverload node)
    {
        // nothing
    }

    public void visitFunctionOverload(FunctionOverload node)
    {
        // nothing
    }

    public void visitCallExpression(CallExpression node)
    {
        visitExpression(node.getFunction());
        for (Expression argument : node.getArguments()) {
            visitExpression(argument);
        }
    }
}
