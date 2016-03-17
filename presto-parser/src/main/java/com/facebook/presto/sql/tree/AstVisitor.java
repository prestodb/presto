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
package com.facebook.presto.sql.tree;

import javax.annotation.Nullable;

public abstract class AstVisitor<R, C>
{
    public R process(Node node, @Nullable C context)
    {
        return node.accept(this, context);
    }

    protected R visitNode(Node node, C context)
    {
        return null;
    }

    protected R visitExpression(Expression node, C context)
    {
        return visitNode(node, context);
    }

    protected R visitCurrentTime(CurrentTime node, C context)
    {
        return visitExpression(node, context);
    }

    protected R visitExtract(Extract node, C context)
    {
        return visitExpression(node, context);
    }

    protected R visitArithmeticBinary(ArithmeticBinaryExpression node, C context)
    {
        return visitExpression(node, context);
    }

    protected R visitBetweenPredicate(BetweenPredicate node, C context)
    {
        return visitExpression(node, context);
    }

    protected R visitCoalesceExpression(CoalesceExpression node, C context)
    {
        return visitExpression(node, context);
    }

    protected R visitComparisonExpression(ComparisonExpression node, C context)
    {
        return visitExpression(node, context);
    }

    protected R visitLiteral(Literal node, C context)
    {
        return visitExpression(node, context);
    }

    protected R visitDoubleLiteral(DoubleLiteral node, C context)
    {
        return visitLiteral(node, context);
    }

    protected R visitDecimalLiteral(DecimalLiteral node, C context)
    {
        return visitLiteral(node, context);
    }

    protected R visitStatement(Statement node, C context)
    {
        return visitNode(node, context);
    }

    protected R visitQuery(Query node, C context)
    {
        return visitStatement(node, context);
    }

    protected R visitExplain(Explain node, C context)
    {
        return visitStatement(node, context);
    }

    protected R visitShowTables(ShowTables node, C context)
    {
        return visitStatement(node, context);
    }

    protected R visitShowSchemas(ShowSchemas node, C context)
    {
        return visitStatement(node, context);
    }

    protected R visitShowCatalogs(ShowCatalogs node, C context)
    {
        return visitStatement(node, context);
    }

    protected R visitShowColumns(ShowColumns node, C context)
    {
        return visitStatement(node, context);
    }

    protected R visitShowPartitions(ShowPartitions node, C context)
    {
        return visitStatement(node, context);
    }

    protected R visitShowFunctions(ShowFunctions node, C context)
    {
        return visitStatement(node, context);
    }

    protected R visitUse(Use node, C context)
    {
        return visitStatement(node, context);
    }

    protected R visitShowSession(ShowSession node, C context)
    {
        return visitStatement(node, context);
    }

    protected R visitSetSession(SetSession node, C context)
    {
        return visitStatement(node, context);
    }

    public R visitResetSession(ResetSession node, C context)
    {
        return visitStatement(node, context);
    }

    protected R visitGenericLiteral(GenericLiteral node, C context)
    {
        return visitLiteral(node, context);
    }

    protected R visitTimeLiteral(TimeLiteral node, C context)
    {
        return visitLiteral(node, context);
    }

    protected R visitExplainOption(ExplainOption node, C context)
    {
        return visitNode(node, context);
    }

    protected R visitWith(With node, C context)
    {
        return visitNode(node, context);
    }

    protected R visitApproximate(Approximate node, C context)
    {
        return visitNode(node, context);
    }

    protected R visitWithQuery(WithQuery node, C context)
    {
        return visitNode(node, context);
    }

    protected R visitSelect(Select node, C context)
    {
        return visitNode(node, context);
    }

    protected R visitRelation(Relation node, C context)
    {
        return visitNode(node, context);
    }

    protected R visitQueryBody(QueryBody node, C context)
    {
        return visitRelation(node, context);
    }

    protected R visitQuerySpecification(QuerySpecification node, C context)
    {
        return visitQueryBody(node, context);
    }

    protected R visitSetOperation(SetOperation node, C context)
    {
        return visitQueryBody(node, context);
    }

    protected R visitUnion(Union node, C context)
    {
        return visitSetOperation(node, context);
    }

    protected R visitIntersect(Intersect node, C context)
    {
        return visitSetOperation(node, context);
    }

    protected R visitExcept(Except node, C context)
    {
        return visitSetOperation(node, context);
    }

    protected R visitTimestampLiteral(TimestampLiteral node, C context)
    {
        return visitLiteral(node, context);
    }

    protected R visitWhenClause(WhenClause node, C context)
    {
        return visitExpression(node, context);
    }

    protected R visitIntervalLiteral(IntervalLiteral node, C context)
    {
        return visitLiteral(node, context);
    }

    protected R visitInPredicate(InPredicate node, C context)
    {
        return visitExpression(node, context);
    }

    protected R visitFunctionCall(FunctionCall node, C context)
    {
        return visitExpression(node, context);
    }

    protected R visitLambdaExpression(LambdaExpression node, C context)
    {
        return visitExpression(node, context);
    }

    protected R visitSimpleCaseExpression(SimpleCaseExpression node, C context)
    {
        return visitExpression(node, context);
    }

    protected R visitStringLiteral(StringLiteral node, C context)
    {
        return visitLiteral(node, context);
    }

    protected R visitBinaryLiteral(BinaryLiteral node, C context)
    {
        return visitLiteral(node, context);
    }

    protected R visitBooleanLiteral(BooleanLiteral node, C context)
    {
        return visitLiteral(node, context);
    }

    protected R visitInListExpression(InListExpression node, C context)
    {
        return visitExpression(node, context);
    }

    protected R visitQualifiedNameReference(QualifiedNameReference node, C context)
    {
        return visitExpression(node, context);
    }

    protected R visitDereferenceExpression(DereferenceExpression node, C context)
    {
        return visitExpression(node, context);
    }

    protected R visitNullIfExpression(NullIfExpression node, C context)
    {
        return visitExpression(node, context);
    }

    protected R visitIfExpression(IfExpression node, C context)
    {
        return visitExpression(node, context);
    }

    protected R visitNullLiteral(NullLiteral node, C context)
    {
        return visitLiteral(node, context);
    }

    protected R visitArithmeticUnary(ArithmeticUnaryExpression node, C context)
    {
        return visitExpression(node, context);
    }

    protected R visitNotExpression(NotExpression node, C context)
    {
        return visitExpression(node, context);
    }

    protected R visitSelectItem(SelectItem node, C context)
    {
        return visitNode(node, context);
    }

    protected R visitSingleColumn(SingleColumn node, C context)
    {
        return visitSelectItem(node, context);
    }

    protected R visitAllColumns(AllColumns node, C context)
    {
        return visitSelectItem(node, context);
    }

    protected R visitSearchedCaseExpression(SearchedCaseExpression node, C context)
    {
        return visitExpression(node, context);
    }

    protected R visitLikePredicate(LikePredicate node, C context)
    {
        return visitExpression(node, context);
    }

    protected R visitIsNotNullPredicate(IsNotNullPredicate node, C context)
    {
        return visitExpression(node, context);
    }

    protected R visitIsNullPredicate(IsNullPredicate node, C context)
    {
        return visitExpression(node, context);
    }

    protected R visitArrayConstructor(ArrayConstructor node, C context)
    {
        return visitExpression(node, context);
    }

    protected R visitSubscriptExpression(SubscriptExpression node, C context)
    {
        return visitExpression(node, context);
    }

    protected R visitLongLiteral(LongLiteral node, C context)
    {
        return visitLiteral(node, context);
    }

    protected R visitLogicalBinaryExpression(LogicalBinaryExpression node, C context)
    {
        return visitExpression(node, context);
    }

    protected R visitSubqueryExpression(SubqueryExpression node, C context)
    {
        return visitExpression(node, context);
    }

    protected R visitSortItem(SortItem node, C context)
    {
        return visitNode(node, context);
    }

    protected R visitTable(Table node, C context)
    {
        return visitQueryBody(node, context);
    }

    protected R visitUnnest(Unnest node, C context)
    {
        return visitRelation(node, context);
    }

    protected R visitValues(Values node, C context)
    {
        return visitQueryBody(node, context);
    }

    protected R visitRow(Row node, C context)
    {
        return visitNode(node, context);
    }

    protected R visitTableSubquery(TableSubquery node, C context)
    {
        return visitQueryBody(node, context);
    }

    protected R visitAliasedRelation(AliasedRelation node, C context)
    {
        return visitRelation(node, context);
    }

    protected R visitSampledRelation(SampledRelation node, C context)
    {
        return visitRelation(node, context);
    }

    protected R visitJoin(Join node, C context)
    {
        return visitRelation(node, context);
    }

    protected R visitExists(ExistsPredicate node, C context)
    {
        return visitExpression(node, context);
    }

    protected R visitTryExpression(TryExpression node, C context)
    {
        return visitExpression(node, context);
    }

    protected R visitCast(Cast node, C context)
    {
        return visitExpression(node, context);
    }

    protected R visitInputReference(InputReference node, C context)
    {
        return visitExpression(node, context);
    }

    protected R visitWindow(Window node, C context)
    {
        return visitNode(node, context);
    }

    protected R visitWindowFrame(WindowFrame node, C context)
    {
        return visitNode(node, context);
    }

    protected R visitFrameBound(FrameBound node, C context)
    {
        return visitNode(node, context);
    }

    protected R visitCallArgument(CallArgument node, C context)
    {
        return visitNode(node, context);
    }

    protected R visitTableElement(TableElement node, C context)
    {
        return visitNode(node, context);
    }

    protected R visitCreateTable(CreateTable node, C context)
    {
        return visitStatement(node, context);
    }

    protected R visitCreateTableAsSelect(CreateTableAsSelect node, C context)
    {
        return visitStatement(node, context);
    }

    protected R visitDropTable(DropTable node, C context)
    {
        return visitStatement(node, context);
    }

    protected R visitRenameTable(RenameTable node, C context)
    {
        return visitStatement(node, context);
    }

    protected R visitRenameColumn(RenameColumn node, C context)
    {
        return visitStatement(node, context);
    }

    protected R visitAddColumn(AddColumn node, C context)
    {
        return visitStatement(node, context);
    }

    protected R visitCreateView(CreateView node, C context)
    {
        return visitStatement(node, context);
    }

    protected R visitDropView(DropView node, C context)
    {
        return visitStatement(node, context);
    }

    protected R visitInsert(Insert node, C context)
    {
        return visitNode(node, context);
    }

    protected R visitCall(Call node, C context)
    {
        return visitNode(node, context);
    }

    protected R visitDelete(Delete node, C context)
    {
        return visitStatement(node, context);
    }

    protected R visitStartTransaction(StartTransaction node, C context)
    {
        return visitStatement(node, context);
    }

    protected R visitGrant(Grant node, C context)
    {
        return visitStatement(node, context);
    }

    protected R visitTransactionMode(TransactionMode node, C context)
    {
        return visitNode(node, context);
    }

    protected R visitIsolationLevel(Isolation node, C context)
    {
        return visitTransactionMode(node, context);
    }

    protected R visitTransactionAccessMode(TransactionAccessMode node, C context)
    {
        return visitTransactionMode(node, context);
    }

    protected R visitCommit(Commit node, C context)
    {
        return visitStatement(node, context);
    }

    protected R visitRollback(Rollback node, C context)
    {
        return visitStatement(node, context);
    }

    protected R visitAtTimeZone(AtTimeZone node, C context)
    {
        return visitExpression(node, context);
    }
}
