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
package com.facebook.presto.sql.parser;

import com.facebook.presto.sql.tree.AliasedRelation;
import com.facebook.presto.sql.tree.AllColumns;
import com.facebook.presto.sql.tree.Approximate;
import com.facebook.presto.sql.tree.ArithmeticBinaryExpression;
import com.facebook.presto.sql.tree.ArithmeticUnaryExpression;
import com.facebook.presto.sql.tree.ArrayConstructor;
import com.facebook.presto.sql.tree.BetweenPredicate;
import com.facebook.presto.sql.tree.BooleanLiteral;
import com.facebook.presto.sql.tree.Cast;
import com.facebook.presto.sql.tree.CoalesceExpression;
import com.facebook.presto.sql.tree.ComparisonExpression;
import com.facebook.presto.sql.tree.CreateTable;
import com.facebook.presto.sql.tree.CreateTableAsSelect;
import com.facebook.presto.sql.tree.CreateView;
import com.facebook.presto.sql.tree.CurrentTime;
import com.facebook.presto.sql.tree.DoubleLiteral;
import com.facebook.presto.sql.tree.DropTable;
import com.facebook.presto.sql.tree.DropView;
import com.facebook.presto.sql.tree.Except;
import com.facebook.presto.sql.tree.ExistsPredicate;
import com.facebook.presto.sql.tree.Explain;
import com.facebook.presto.sql.tree.ExplainFormat;
import com.facebook.presto.sql.tree.ExplainOption;
import com.facebook.presto.sql.tree.ExplainType;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.Extract;
import com.facebook.presto.sql.tree.FrameBound;
import com.facebook.presto.sql.tree.FunctionCall;
import com.facebook.presto.sql.tree.GenericLiteral;
import com.facebook.presto.sql.tree.IfExpression;
import com.facebook.presto.sql.tree.InListExpression;
import com.facebook.presto.sql.tree.InPredicate;
import com.facebook.presto.sql.tree.Insert;
import com.facebook.presto.sql.tree.Intersect;
import com.facebook.presto.sql.tree.IntervalLiteral;
import com.facebook.presto.sql.tree.IsNotNullPredicate;
import com.facebook.presto.sql.tree.IsNullPredicate;
import com.facebook.presto.sql.tree.Join;
import com.facebook.presto.sql.tree.JoinCriteria;
import com.facebook.presto.sql.tree.JoinOn;
import com.facebook.presto.sql.tree.JoinUsing;
import com.facebook.presto.sql.tree.LikePredicate;
import com.facebook.presto.sql.tree.LogicalBinaryExpression;
import com.facebook.presto.sql.tree.LongLiteral;
import com.facebook.presto.sql.tree.NaturalJoin;
import com.facebook.presto.sql.tree.Node;
import com.facebook.presto.sql.tree.NotExpression;
import com.facebook.presto.sql.tree.NullIfExpression;
import com.facebook.presto.sql.tree.NullLiteral;
import com.facebook.presto.sql.tree.QualifiedName;
import com.facebook.presto.sql.tree.QualifiedNameReference;
import com.facebook.presto.sql.tree.Query;
import com.facebook.presto.sql.tree.QueryBody;
import com.facebook.presto.sql.tree.QuerySpecification;
import com.facebook.presto.sql.tree.Relation;
import com.facebook.presto.sql.tree.RenameColumn;
import com.facebook.presto.sql.tree.RenameTable;
import com.facebook.presto.sql.tree.ResetSession;
import com.facebook.presto.sql.tree.Row;
import com.facebook.presto.sql.tree.SampledRelation;
import com.facebook.presto.sql.tree.SearchedCaseExpression;
import com.facebook.presto.sql.tree.Select;
import com.facebook.presto.sql.tree.SelectItem;
import com.facebook.presto.sql.tree.SetSession;
import com.facebook.presto.sql.tree.ShowCatalogs;
import com.facebook.presto.sql.tree.ShowColumns;
import com.facebook.presto.sql.tree.ShowFunctions;
import com.facebook.presto.sql.tree.ShowPartitions;
import com.facebook.presto.sql.tree.ShowSchemas;
import com.facebook.presto.sql.tree.ShowSession;
import com.facebook.presto.sql.tree.ShowTables;
import com.facebook.presto.sql.tree.SimpleCaseExpression;
import com.facebook.presto.sql.tree.SingleColumn;
import com.facebook.presto.sql.tree.SortItem;
import com.facebook.presto.sql.tree.Statement;
import com.facebook.presto.sql.tree.StringLiteral;
import com.facebook.presto.sql.tree.SubqueryExpression;
import com.facebook.presto.sql.tree.SubscriptExpression;
import com.facebook.presto.sql.tree.Table;
import com.facebook.presto.sql.tree.TableElement;
import com.facebook.presto.sql.tree.TableSubquery;
import com.facebook.presto.sql.tree.TimeLiteral;
import com.facebook.presto.sql.tree.TimestampLiteral;
import com.facebook.presto.sql.tree.Union;
import com.facebook.presto.sql.tree.Unnest;
import com.facebook.presto.sql.tree.Use;
import com.facebook.presto.sql.tree.Values;
import com.facebook.presto.sql.tree.WhenClause;
import com.facebook.presto.sql.tree.Window;
import com.facebook.presto.sql.tree.WindowFrame;
import com.facebook.presto.sql.tree.With;
import com.facebook.presto.sql.tree.WithQuery;
import com.google.common.collect.ImmutableList;
import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.Token;
import org.antlr.v4.runtime.misc.NotNull;
import org.antlr.v4.runtime.tree.ParseTree;
import org.antlr.v4.runtime.tree.TerminalNode;

import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import static com.facebook.presto.sql.QueryUtil.mangleFieldReference;

class AstBuilder
        extends SqlBaseBaseVisitor<Node>
{
    @Override
    public Node visitSingleStatement(@NotNull SqlBaseParser.SingleStatementContext context)
    {
        return visit(context.statement());
    }

    @Override
    public Node visitSingleExpression(@NotNull SqlBaseParser.SingleExpressionContext context)
    {
        return visit(context.expression());
    }

    // ******************* statements **********************

    @Override
    public Node visitUse(@NotNull SqlBaseParser.UseContext context)
    {
        return new Use(getTextIfPresent(context.catalog), context.schema.getText());
    }

    @Override
    public Node visitCreateTableAsSelect(@NotNull SqlBaseParser.CreateTableAsSelectContext context)
    {
        return new CreateTableAsSelect(getQualifiedName(context.qualifiedName()), (Query) visit(context.query()));
    }

    @Override
    public Node visitCreateTable(@NotNull SqlBaseParser.CreateTableContext context)
    {
        return new CreateTable(getQualifiedName(context.qualifiedName()), visit(context.tableElement(), TableElement.class));
    }

    @Override
    public Node visitDropTable(@NotNull SqlBaseParser.DropTableContext context)
    {
        return new DropTable(getQualifiedName(context.qualifiedName()), context.EXISTS() != null);
    }

    @Override
    public Node visitDropView(@NotNull SqlBaseParser.DropViewContext context)
    {
        return new DropView(getQualifiedName(context.qualifiedName()), context.EXISTS() != null);
    }

    @Override
    public Node visitInsertInto(@NotNull SqlBaseParser.InsertIntoContext context)
    {
        return new Insert(getQualifiedName(context.qualifiedName()), (Query) visit(context.query()));
    }

    @Override
    public Node visitRenameTable(@NotNull SqlBaseParser.RenameTableContext context)
    {
        return new RenameTable(getQualifiedName(context.from), getQualifiedName(context.to));
    }

    @Override
    public Node visitRenameColumn(@NotNull SqlBaseParser.RenameColumnContext context)
    {
        return new RenameColumn(getQualifiedName(context.tableName), context.from.getText(), context.to.getText());
    }

    @Override
    public Node visitCreateView(@NotNull SqlBaseParser.CreateViewContext context)
    {
        return new CreateView(
                getQualifiedName(context.qualifiedName()),
                (Query) visit(context.query()),
                context.REPLACE() != null);
    }

    // ********************** query expressions ********************

    @Override
    public Node visitQuery(@NotNull SqlBaseParser.QueryContext context)
    {
        Query body = (Query) visit(context.queryNoWith());

        return new Query(visitIfPresent(context.with(), With.class),
                body.getQueryBody(),
                body.getOrderBy(),
                body.getLimit(),
                body.getApproximate());
    }

    @Override
    public Node visitWith(@NotNull SqlBaseParser.WithContext context)
    {
        return new With(context.RECURSIVE() != null, visit(context.namedQuery(), WithQuery.class));
    }

    @Override
    public Node visitNamedQuery(@NotNull SqlBaseParser.NamedQueryContext context)
    {
        return new WithQuery(context.name.getText(), (Query) visit(context.query()), getColumnAliases(context.columnAliases()));
    }

    @Override
    public Node visitQueryNoWith(@NotNull SqlBaseParser.QueryNoWithContext context)
    {
        QueryBody term = (QueryBody) visit(context.queryTerm());

        if (term instanceof QuerySpecification) {
            // When we have a simple query specification
            // followed by order by limit, fold the order by and limit
            // clauses into the query specification (analyzer/planner
            // expects this structure to resolve references with respect
            // to columns defined in the query specification)
            QuerySpecification query = (QuerySpecification) term;

            return new Query(
                    Optional.<With>empty(),
                    new QuerySpecification(
                            query.getSelect(),
                            query.getFrom(),
                            query.getWhere(),
                            query.getGroupBy(),
                            query.getHaving(),
                            visit(context.sortItem(), SortItem.class),
                            getTextIfPresent(context.limit)),
                    ImmutableList.of(),
                    Optional.<String>empty(),
                    getTextIfPresent(context.confidence)
                            .map(Approximate::new));
        }

        return new Query(
                Optional.<With>empty(),
                term,
                visit(context.sortItem(), SortItem.class),
                getTextIfPresent(context.limit),
                getTextIfPresent(context.confidence)
                        .map(Approximate::new));
    }

    @Override
    public Node visitQuerySpecification(@NotNull SqlBaseParser.QuerySpecificationContext context)
    {
        Optional<Relation> from = Optional.empty();

        List<Relation> relations = visit(context.relation(), Relation.class);
        if (!relations.isEmpty()) {
            // synthesize implicit join nodes
            Iterator<Relation> iterator = relations.iterator();
            Relation relation = iterator.next();

            while (iterator.hasNext()) {
                relation = new Join(Join.Type.IMPLICIT, relation, iterator.next(), Optional.<JoinCriteria>empty());
            }

            from = Optional.of(relation);
        }

        return new QuerySpecification(
                new Select(isDistinct(context.setQuantifier()), visit(context.selectItem(), SelectItem.class)),
                from,
                visitIfPresent(context.where, Expression.class),
                visit(context.groupBy, Expression.class),
                visitIfPresent(context.having, Expression.class),
                ImmutableList.of(),
                Optional.<String>empty());
    }

    @Override
    public Node visitSetOperation(@NotNull SqlBaseParser.SetOperationContext context)
    {
        QueryBody left = (QueryBody) visit(context.left);
        QueryBody right = (QueryBody) visit(context.right);

        boolean distinct = context.setQuantifier() == null || context.setQuantifier().DISTINCT() != null;

        switch (context.operator.getType()) {
            case SqlBaseLexer.UNION:
                return new Union(ImmutableList.of(left, right), distinct);
            case SqlBaseLexer.INTERSECT:
                return new Intersect(ImmutableList.of(left, right), distinct);
            case SqlBaseLexer.EXCEPT:
                return new Except(left, right, distinct);
        }

        throw new IllegalArgumentException("Unsupported set operation: " + context.operator.getText());
    }

    @Override
    public Node visitSelectAll(@NotNull SqlBaseParser.SelectAllContext context)
    {
        if (context.qualifiedName() != null) {
            return new AllColumns(getQualifiedName(context.qualifiedName()));
        }

        return new AllColumns();
    }

    @Override
    public Node visitSelectSingle(@NotNull SqlBaseParser.SelectSingleContext context)
    {
        Optional<String> alias = getTextIfPresent(context.identifier());

        return new SingleColumn((Expression) visit(context.expression()), alias);
    }

    @Override
    public Node visitTable(@NotNull SqlBaseParser.TableContext context)
    {
        return new Table(getQualifiedName(context.qualifiedName()));
    }

    @Override
    public Node visitSubquery(@NotNull SqlBaseParser.SubqueryContext context)
    {
        return new TableSubquery((Query) visit(context.queryNoWith()));
    }

    @Override
    public Node visitInlineTable(@NotNull SqlBaseParser.InlineTableContext context)
    {
        return new Values(visit(context.expression(), Expression.class));
    }

    @Override
    public Node visitExplain(@NotNull SqlBaseParser.ExplainContext context)
    {
        return new Explain((Statement) visit(context.statement()), visit(context.explainOption(), ExplainOption.class));
    }

    @Override
    public Node visitExplainFormat(@NotNull SqlBaseParser.ExplainFormatContext context)
    {
        switch (context.value.getType()) {
            case SqlBaseLexer.GRAPHVIZ:
                return new ExplainFormat(ExplainFormat.Type.GRAPHVIZ);
            case SqlBaseLexer.TEXT:
                return new ExplainFormat(ExplainFormat.Type.TEXT);
            case SqlBaseLexer.JSON:
                return new ExplainFormat(ExplainFormat.Type.JSON);
        }

        throw new IllegalArgumentException("Unsupported EXPLAIN format: " + context.value.getText());
    }

    @Override
    public Node visitExplainType(@NotNull SqlBaseParser.ExplainTypeContext context)
    {
        switch (context.value.getType()) {
            case SqlBaseLexer.LOGICAL:
                return new ExplainType(ExplainType.Type.LOGICAL);
            case SqlBaseLexer.DISTRIBUTED:
                return new ExplainType(ExplainType.Type.DISTRIBUTED);
        }

        throw new IllegalArgumentException("Unsupported EXPLAIN type: " + context.value.getText());
    }

    @Override
    public Node visitShowTables(@NotNull SqlBaseParser.ShowTablesContext context)
    {
        return new ShowTables(
                Optional.ofNullable(context.qualifiedName())
                        .map(AstBuilder::getQualifiedName),
                getTextIfPresent(context.pattern)
                        .map(AstBuilder::unquote));
    }

    @Override
    public Node visitShowSchemas(@NotNull SqlBaseParser.ShowSchemasContext context)
    {
        return new ShowSchemas(getTextIfPresent(context.identifier()));
    }

    @Override
    public Node visitShowCatalogs(@NotNull SqlBaseParser.ShowCatalogsContext context)
    {
        return new ShowCatalogs();
    }

    @Override
    public Node visitShowColumns(@NotNull SqlBaseParser.ShowColumnsContext context)
    {
        return new ShowColumns(getQualifiedName(context.qualifiedName()));
    }

    @Override
    public Node visitShowPartitions(@NotNull SqlBaseParser.ShowPartitionsContext context)
    {
        return new ShowPartitions(
                getQualifiedName(context.qualifiedName()),
                visitIfPresent(context.booleanExpression(), Expression.class),
                visit(context.sortItem(), SortItem.class),
                getTextIfPresent(context.limit));
    }

    @Override
    public Node visitShowFunctions(@NotNull SqlBaseParser.ShowFunctionsContext context)
    {
        return new ShowFunctions();
    }

    @Override
    public Node visitShowSession(@NotNull SqlBaseParser.ShowSessionContext context)
    {
        return new ShowSession();
    }

    @Override
    public Node visitSetSession(@NotNull SqlBaseParser.SetSessionContext context)
    {
        return new SetSession(getQualifiedName(context.qualifiedName()), unquote(context.STRING().getText()));
    }

    @Override
    public Node visitResetSession(@NotNull SqlBaseParser.ResetSessionContext context)
    {
        return new ResetSession(getQualifiedName(context.qualifiedName()));
    }

    // ***************** boolean expressions ******************

    @Override
    public Node visitLogicalNot(@NotNull SqlBaseParser.LogicalNotContext context)
    {
        return new NotExpression((Expression) visit(context.booleanExpression()));
    }

    @Override
    public Node visitLogicalBinary(@NotNull SqlBaseParser.LogicalBinaryContext context)
    {
        return new LogicalBinaryExpression(
                getLogicalBinaryOperator(context.operator),
                (Expression) visit(context.left),
                (Expression) visit(context.right));
    }

    // *************** from clause *****************

    @Override
    public Node visitJoinRelation(@NotNull SqlBaseParser.JoinRelationContext context)
    {
        Relation left = (Relation) visit(context.left);
        Relation right;

        if (context.CROSS() != null) {
            right = (Relation) visit(context.right);
            return new Join(Join.Type.CROSS, left, right, Optional.<JoinCriteria>empty());
        }

        JoinCriteria criteria;
        if (context.NATURAL() != null) {
            right = (Relation) visit(context.right);
            criteria = new NaturalJoin();
        }
        else {
            right = (Relation) visit(context.rightRelation);
            if (context.joinCriteria().ON() != null) {
                criteria = new JoinOn((Expression) visit(context.joinCriteria().booleanExpression()));
            }
            else if (context.joinCriteria().USING() != null) {
                List<String> columns = context.joinCriteria()
                        .identifier().stream()
                        .map(ParseTree::getText)
                        .collect(Collectors.toList());

                criteria = new JoinUsing(columns);
            }
            else {
                throw new IllegalArgumentException("Unsupported join criteria");
            }
        }

        Join.Type joinType;
        if (context.joinType().LEFT() != null) {
            joinType = Join.Type.LEFT;
        }
        else if (context.joinType().RIGHT() != null) {
            joinType = Join.Type.RIGHT;
        }
        else if (context.joinType().FULL() != null) {
            joinType = Join.Type.FULL;
        }
        else {
            joinType = Join.Type.INNER;
        }

        return new Join(joinType, left, right, Optional.of(criteria));
    }

    @Override
    public Node visitSampledRelation(@NotNull SqlBaseParser.SampledRelationContext context)
    {
        Relation child = (Relation) visit(context.aliasedRelation());

        if (context.TABLESAMPLE() == null) {
            return child;
        }

        Optional<List<Expression>> stratifyOn = Optional.empty();
        if (context.STRATIFY() != null) {
            stratifyOn = Optional.of(visit(context.stratify, Expression.class));
        }

        return new SampledRelation(
                child,
                getSamplingMethod((Token) context.sampleType().getChild(0).getPayload()),
                (Expression) visit(context.percentage),
                context.RESCALED() != null,
                stratifyOn);
    }

    @Override
    public Node visitAliasedRelation(@NotNull SqlBaseParser.AliasedRelationContext context)
    {
        Relation child = (Relation) visit(context.relationPrimary());

        if (context.identifier() == null) {
            return child;
        }

        return new AliasedRelation(child, context.identifier().getText(), getColumnAliases(context.columnAliases()));
    }

    @Override
    public Node visitTableName(@NotNull SqlBaseParser.TableNameContext context)
    {
        return new Table(getQualifiedName(context.qualifiedName()));
    }

    @Override
    public Node visitSubqueryRelation(@NotNull SqlBaseParser.SubqueryRelationContext context)
    {
        return new TableSubquery((Query) visit(context.query()));
    }

    @Override
    public Node visitUnnest(@NotNull SqlBaseParser.UnnestContext context)
    {
        return new Unnest(visit(context.expression(), Expression.class), context.ORDINALITY() != null);
    }

    @Override
    public Node visitParenthesizedRelation(@NotNull SqlBaseParser.ParenthesizedRelationContext context)
    {
        return visit(context.relation());
    }

    // ********************* predicates *******************

    @Override
    public Node visitPredicated(@NotNull SqlBaseParser.PredicatedContext context)
    {
        if (context.predicate() != null) {
            return visit(context.predicate());
        }

        return visit(context.valueExpression);
    }

    @Override
    public Node visitComparison(@NotNull SqlBaseParser.ComparisonContext context)
    {
        return new ComparisonExpression(
                getComparisonOperator(((TerminalNode) context.comparisonOperator().getChild(0)).getSymbol()),
                (Expression) visit(context.value),
                (Expression) visit(context.right));
    }

    @Override
    public Node visitDistinctFrom(@NotNull SqlBaseParser.DistinctFromContext context)
    {
        Expression expression = new ComparisonExpression(
                ComparisonExpression.Type.IS_DISTINCT_FROM,
                (Expression) visit(context.value),
                (Expression) visit(context.right));

        if (context.NOT() != null) {
            expression = new NotExpression(expression);
        }

        return expression;
    }

    @Override
    public Node visitBetween(@NotNull SqlBaseParser.BetweenContext context)
    {
        Expression expression = new BetweenPredicate(
                (Expression) visit(context.value),
                (Expression) visit(context.lower),
                (Expression) visit(context.upper));

        if (context.NOT() != null) {
            expression = new NotExpression(expression);
        }

        return expression;
    }

    @Override
    public Node visitNullPredicate(@NotNull SqlBaseParser.NullPredicateContext context)
    {
        Expression child = (Expression) visit(context.value);

        if (context.NOT() == null) {
            return new IsNullPredicate(child);
        }

        return new IsNotNullPredicate(child);
    }

    @Override
    public Node visitLike(@NotNull SqlBaseParser.LikeContext context)
    {
        Expression escape = null;
        if (context.escape != null) {
            escape = (Expression) visit(context.escape);
        }

        Expression result = new LikePredicate((Expression) visit(context.value), (Expression) visit(context.pattern), escape);

        if (context.NOT() != null) {
            result = new NotExpression(result);
        }

        return result;
    }

    @Override
    public Node visitInList(@NotNull SqlBaseParser.InListContext context)
    {
        Expression result = new InPredicate(
                (Expression) visit(context.value),
                new InListExpression(visit(context.expression(), Expression.class)));

        if (context.NOT() != null) {
            result = new NotExpression(result);
        }

        return result;
    }

    @Override
    public Node visitInSubquery(@NotNull SqlBaseParser.InSubqueryContext context)
    {
        Expression result = new InPredicate(
                (Expression) visit(context.value),
                new SubqueryExpression((Query) visit(context.query())));

        if (context.NOT() != null) {
            result = new NotExpression(result);
        }

        return result;
    }

    @Override
    public Node visitExists(@NotNull SqlBaseParser.ExistsContext context)
    {
        return new ExistsPredicate((Query) visit(context.query()));
    }

    // ************** value expressions **************

    @Override
    public Node visitArithmeticUnary(@NotNull SqlBaseParser.ArithmeticUnaryContext context)
    {
        Expression child = (Expression) visit(context.valueExpression());

        switch (context.operator.getType()) {
            case SqlBaseLexer.MINUS:
                return ArithmeticUnaryExpression.negative(child);
            case SqlBaseLexer.PLUS:
                return ArithmeticUnaryExpression.positive(child);
            default:
                throw new UnsupportedOperationException("Unsupported sign: " + context.operator.getText());
        }
    }

    @Override
    public Node visitArithmeticBinary(@NotNull SqlBaseParser.ArithmeticBinaryContext context)
    {
        return new ArithmeticBinaryExpression(
                getArithmeticBinaryOperator(context.operator),
                (Expression) visit(context.left),
                (Expression) visit(context.right));
    }

    @Override
    public Node visitConcatenation(@NotNull SqlBaseParser.ConcatenationContext context)
    {
        return new FunctionCall(new QualifiedName("concat"), ImmutableList.of(
                (Expression) visit(context.left),
                (Expression) visit(context.right)));
    }

    @Override
    public Node visitAtTimeZone(@NotNull SqlBaseParser.AtTimeZoneContext context)
    {
        return new FunctionCall(QualifiedName.of("at_timezone"), ImmutableList.of(
                (Expression) visit(context.valueExpression()),
                (Expression) visit(context.timeZoneSpecifier())));
    }

    @Override
    public Node visitTimeZoneInterval(@NotNull SqlBaseParser.TimeZoneIntervalContext context)
    {
        return visit(context.interval());
    }

    @Override
    public Node visitTimeZoneString(@NotNull SqlBaseParser.TimeZoneStringContext context)
    {
        return new StringLiteral(unquote(context.STRING().getText()));
    }

    // ********************* primary expressions **********************

    @Override
    public Node visitParenthesizedExpression(@NotNull SqlBaseParser.ParenthesizedExpressionContext context)
    {
        return visit(context.expression());
    }

    @Override
    public Node visitRowConstructor(@NotNull SqlBaseParser.RowConstructorContext context)
    {
        return new Row(visit(context.expression(), Expression.class));
    }

    @Override
    public Node visitArrayConstructor(@NotNull SqlBaseParser.ArrayConstructorContext context)
    {
        return new ArrayConstructor(visit(context.expression(), Expression.class));
    }

    @Override
    public Node visitCast(@NotNull SqlBaseParser.CastContext context)
    {
        boolean isTryCast = context.TRY_CAST() != null;
        return new Cast((Expression) visit(context.expression()), getType(context.type()), isTryCast);
    }

    @Override
    public Node visitSpecialDateTimeFunction(@NotNull SqlBaseParser.SpecialDateTimeFunctionContext context)
    {
        CurrentTime.Type type = getDateTimeFunctionType(context.name);

        if (context.precision != null) {
            return new CurrentTime(type, Integer.parseInt(context.precision.getText()));
        }

        return new CurrentTime(type);
    }

    @Override
    public Node visitExtract(@NotNull SqlBaseParser.ExtractContext context)
    {
        return new Extract((Expression) visit(context.valueExpression()), Extract.Field.valueOf(context.identifier().getText().toUpperCase()));
    }

    @Override
    public Node visitSubstring(@NotNull SqlBaseParser.SubstringContext context)
    {
        return new FunctionCall(new QualifiedName("substr"), visit(context.valueExpression(), Expression.class));
    }

    @Override
    public Node visitSubscript(@NotNull SqlBaseParser.SubscriptContext context)
    {
        return new SubscriptExpression((Expression) visit(context.value), (Expression) visit(context.index));
    }

    @Override
    public Node visitFieldReference(@NotNull SqlBaseParser.FieldReferenceContext context)
    {
        // TODO: This should be done during the conversion to RowExpression
        return new FunctionCall(new QualifiedName(mangleFieldReference(context.fieldName.getText())), ImmutableList.of((Expression) visit(context.value)));
    }

    @Override
    public Node visitSubqueryExpression(@NotNull SqlBaseParser.SubqueryExpressionContext context)
    {
        return new SubqueryExpression((Query) visit(context.query()));
    }

    @Override
    public Node visitColumnReference(@NotNull SqlBaseParser.ColumnReferenceContext context)
    {
        return new QualifiedNameReference(getQualifiedName(context.qualifiedName()));
    }

    @Override
    public Node visitSimpleCase(@NotNull SqlBaseParser.SimpleCaseContext context)
    {
        return new SimpleCaseExpression(
                (Expression) visit(context.valueExpression()),
                visit(context.whenClause(), WhenClause.class),
                visitIfPresent(context.elseExpression, Expression.class));
    }

    @Override
    public Node visitSearchedCase(@NotNull SqlBaseParser.SearchedCaseContext context)
    {
        return new SearchedCaseExpression(
                visit(context.whenClause(), WhenClause.class),
                visitIfPresent(context.elseExpression, Expression.class));
    }

    @Override
    public Node visitWhenClause(@NotNull SqlBaseParser.WhenClauseContext context)
    {
        return new WhenClause((Expression) visit(context.condition), (Expression) visit(context.result));
    }

    @Override
    public Node visitFunctionCall(@NotNull SqlBaseParser.FunctionCallContext context)
    {
        Optional<Window> window = visitIfPresent(context.over(), Window.class);

        QualifiedName name = getQualifiedName(context.qualifiedName());

        boolean distinct = isDistinct(context.setQuantifier());

        if (name.toString().equalsIgnoreCase("if")) {
            check(context.expression().size() == 2 || context.expression().size() == 3, "Invalid number of arguments for 'if' function", context);
            check(!window.isPresent(), "OVER clause not valid for 'if' function", context);
            check(!distinct, "DISTINCT not valid for 'if' function", context);

            Expression elseExpression = null;
            if (context.expression().size() == 3) {
                elseExpression = (Expression) visit(context.expression(2));
            }

            return new IfExpression(
                    (Expression) visit(context.expression(0)),
                    (Expression) visit(context.expression(1)),
                    elseExpression);
        }
        else if (name.toString().equalsIgnoreCase("nullif")) {
            check(context.expression().size() == 2, "Invalid number of arguments for 'nullif' function", context);
            check(!window.isPresent(), "OVER clause not valid for 'nullif' function", context);
            check(!distinct, "DISTINCT not valid for 'nullif' function", context);

            return new NullIfExpression(
                    (Expression) visit(context.expression(0)),
                    (Expression) visit(context.expression(1)));
        }
        else if (name.toString().equalsIgnoreCase("coalesce")) {
            check(!window.isPresent(), "OVER clause not valid for 'coalesce' function", context);
            check(!distinct, "DISTINCT not valid for 'coalesce' function", context);

            return new CoalesceExpression(visit(context.expression(), Expression.class));
        }

        return new FunctionCall(
                getQualifiedName(context.qualifiedName()),
                window,
                distinct,
                visit(context.expression(), Expression.class));
    }

    @Override
    public Node visitOver(@NotNull SqlBaseParser.OverContext context)
    {
        return new Window(
                visit(context.partition, Expression.class),
                visit(context.sortItem(), SortItem.class),
                visitIfPresent(context.windowFrame(), WindowFrame.class));
    }

    @Override
    public Node visitTableElement(@NotNull SqlBaseParser.TableElementContext context)
    {
        return new TableElement(context.identifier().getText(), getType(context.type()));
    }

    @Override
    public Node visitSortItem(@NotNull SqlBaseParser.SortItemContext context)
    {
        return new SortItem(
                (Expression) visit(context.expression()),
                Optional.ofNullable(context.ordering)
                        .map(AstBuilder::getOrderingType)
                        .orElse(SortItem.Ordering.ASCENDING),
                Optional.ofNullable(context.nullOrdering)
                        .map(AstBuilder::getNullOrderingType)
                        .orElse(SortItem.NullOrdering.UNDEFINED));
    }

    @Override
    public Node visitWindowFrame(@NotNull SqlBaseParser.WindowFrameContext context)
    {
        return new WindowFrame(
                getFrameType(context.frameType),
                (FrameBound) visit(context.start),
                visitIfPresent(context.end, FrameBound.class));
    }

    @Override
    public Node visitUnboundedFrame(@NotNull SqlBaseParser.UnboundedFrameContext context)
    {
        return new FrameBound(getUnboundedFrameBoundType(context.boundType));
    }

    @Override
    public Node visitBoundedFrame(@NotNull SqlBaseParser.BoundedFrameContext context)
    {
        return new FrameBound(getBoundedFrameBoundType(context.boundType), (Expression) visit(context.expression()));
    }

    @Override
    public Node visitCurrentRowBound(@NotNull SqlBaseParser.CurrentRowBoundContext context)
    {
        return new FrameBound(FrameBound.Type.CURRENT_ROW);
    }

    // ************** literals **************

    @Override
    public Node visitNullLiteral(@NotNull SqlBaseParser.NullLiteralContext context)
    {
        return new NullLiteral();
    }

    @Override
    public Node visitStringLiteral(@NotNull SqlBaseParser.StringLiteralContext context)
    {
        return new StringLiteral(unquote(context.STRING().getText()));
    }

    @Override
    public Node visitTypeConstructor(@NotNull SqlBaseParser.TypeConstructorContext context)
    {
        String type = context.identifier().getText();
        String value = unquote(context.STRING().getText());

        if (type.equalsIgnoreCase("time")) {
            return new TimeLiteral(value);
        }
        else if (type.equalsIgnoreCase("timestamp")) {
            return new TimestampLiteral(value);
        }

        return new GenericLiteral(type, value);
    }

    @Override
    public Node visitIntegerLiteral(@NotNull SqlBaseParser.IntegerLiteralContext context)
    {
        return new LongLiteral(context.getText());
    }

    @Override
    public Node visitDecimalLiteral(@NotNull SqlBaseParser.DecimalLiteralContext context)
    {
        return new DoubleLiteral(context.getText());
    }

    @Override
    public Node visitBooleanValue(@NotNull SqlBaseParser.BooleanValueContext context)
    {
        return new BooleanLiteral(context.getText());
    }

    @Override
    public Node visitInterval(@NotNull SqlBaseParser.IntervalContext context)
    {
        return new IntervalLiteral(
                unquote(context.STRING().getText()),
                Optional.ofNullable(context.sign)
                        .map(AstBuilder::getIntervalSign)
                        .orElse(IntervalLiteral.Sign.POSITIVE),
                getIntervalFieldType((Token) context.from.getChild(0).getPayload()),
                Optional.ofNullable(context.to)
                        .map((x) -> x.getChild(0).getPayload())
                        .map(Token.class::cast)
                        .map(AstBuilder::getIntervalFieldType));
    }

    // ***************** helpers *****************

    @Override
    protected Node defaultResult()
    {
        return null;
    }

    @Override
    protected Node aggregateResult(Node aggregate, Node nextResult)
    {
        if (nextResult == null) {
            throw new UnsupportedOperationException("not yet implemented");
        }

        if (aggregate == null) {
            return nextResult;
        }

        throw new UnsupportedOperationException("not yet implemented");
    }

    private <T> Optional<T> visitIfPresent(ParserRuleContext context, Class<T> clazz)
    {
        return Optional.ofNullable(context)
                .map(this::visit)
                .map(clazz::cast);
    }

    private <T> List<T> visit(List<? extends ParserRuleContext> contexts, Class<T> clazz)
    {
        return contexts.stream()
                .map(this::visit)
                .map(clazz::cast)
                .collect(Collectors.toList());
    }

    private static String unquote(String string)
    {
        return string.substring(1, string.length() - 1)
                .replace("''", "'");
    }

    private static QualifiedName getQualifiedName(SqlBaseParser.QualifiedNameContext context)
    {
        List<String> parts = context
                .identifier().stream()
                .map(ParseTree::getText)
                .collect(Collectors.toList());

        return new QualifiedName(parts);
    }

    private static boolean isDistinct(SqlBaseParser.SetQuantifierContext setQuantifier)
    {
        return setQuantifier != null && setQuantifier.DISTINCT() != null;
    }

    private static Optional<String> getTextIfPresent(ParserRuleContext context)
    {
        return Optional.ofNullable(context)
                .map(ParseTree::getText);
    }

    private static Optional<String> getTextIfPresent(Token token)
    {
        return Optional.ofNullable(token)
                .map(Token::getText);
    }

    private static List<String> getColumnAliases(SqlBaseParser.ColumnAliasesContext columnAliasesContext)
    {
        if (columnAliasesContext == null) {
            return null;
        }

        return columnAliasesContext
                .identifier().stream()
                .map(ParseTree::getText)
                .collect(Collectors.toList());
    }

    private static ArithmeticBinaryExpression.Type getArithmeticBinaryOperator(Token operator)
    {
        switch (operator.getType()) {
            case SqlBaseLexer.PLUS:
                return ArithmeticBinaryExpression.Type.ADD;
            case SqlBaseLexer.MINUS:
                return ArithmeticBinaryExpression.Type.SUBTRACT;
            case SqlBaseLexer.ASTERISK:
                return ArithmeticBinaryExpression.Type.MULTIPLY;
            case SqlBaseLexer.SLASH:
                return ArithmeticBinaryExpression.Type.DIVIDE;
            case SqlBaseLexer.PERCENT:
                return ArithmeticBinaryExpression.Type.MODULUS;
        }

        throw new UnsupportedOperationException("Unsupported operator: " + operator.getText());
    }

    private static ComparisonExpression.Type getComparisonOperator(Token symbol)
    {
        switch (symbol.getType()) {
            case SqlBaseLexer.EQ:
                return ComparisonExpression.Type.EQUAL;
            case SqlBaseLexer.NEQ:
                return ComparisonExpression.Type.NOT_EQUAL;
            case SqlBaseLexer.LT:
                return ComparisonExpression.Type.LESS_THAN;
            case SqlBaseLexer.LTE:
                return ComparisonExpression.Type.LESS_THAN_OR_EQUAL;
            case SqlBaseLexer.GT:
                return ComparisonExpression.Type.GREATER_THAN;
            case SqlBaseLexer.GTE:
                return ComparisonExpression.Type.GREATER_THAN_OR_EQUAL;
        }

        throw new IllegalArgumentException("Unsupported operator: " + symbol.getText());
    }

    private static CurrentTime.Type getDateTimeFunctionType(Token token)
    {
        switch (token.getType()) {
            case SqlBaseLexer.CURRENT_DATE:
                return CurrentTime.Type.DATE;
            case SqlBaseLexer.CURRENT_TIME:
                return CurrentTime.Type.TIME;
            case SqlBaseLexer.CURRENT_TIMESTAMP:
                return CurrentTime.Type.TIMESTAMP;
            case SqlBaseLexer.LOCALTIME:
                return CurrentTime.Type.LOCALTIME;
            case SqlBaseLexer.LOCALTIMESTAMP:
                return CurrentTime.Type.LOCALTIMESTAMP;
        }

        throw new IllegalArgumentException("Unsupported special function: " + token.getText());
    }

    private static IntervalLiteral.IntervalField getIntervalFieldType(Token token)
    {
        switch (token.getType()) {
            case SqlBaseLexer.YEAR:
                return IntervalLiteral.IntervalField.YEAR;
            case SqlBaseLexer.MONTH:
                return IntervalLiteral.IntervalField.MONTH;
            case SqlBaseLexer.DAY:
                return IntervalLiteral.IntervalField.DAY;
            case SqlBaseLexer.HOUR:
                return IntervalLiteral.IntervalField.HOUR;
            case SqlBaseLexer.MINUTE:
                return IntervalLiteral.IntervalField.MINUTE;
            case SqlBaseLexer.SECOND:
                return IntervalLiteral.IntervalField.SECOND;
        }

        throw new IllegalArgumentException("Unsupported interval field: " + token.getText());
    }

    private static IntervalLiteral.Sign getIntervalSign(Token token)
    {
        switch (token.getType()) {
            case SqlBaseLexer.MINUS:
                return IntervalLiteral.Sign.NEGATIVE;
            case SqlBaseLexer.PLUS:
                return IntervalLiteral.Sign.POSITIVE;
        }

        throw new IllegalArgumentException("Unsupported sign: " + token.getText());
    }

    private static WindowFrame.Type getFrameType(Token type)
    {
        switch (type.getType()) {
            case SqlBaseLexer.RANGE:
                return WindowFrame.Type.RANGE;
            case SqlBaseLexer.ROWS:
                return WindowFrame.Type.ROWS;
        }

        throw new IllegalArgumentException("Unsupported frame type: " + type.getText());
    }

    private static FrameBound.Type getBoundedFrameBoundType(Token token)
    {
        switch (token.getType()) {
            case SqlBaseLexer.PRECEDING:
                return FrameBound.Type.PRECEDING;
            case SqlBaseLexer.FOLLOWING:
                return FrameBound.Type.FOLLOWING;
        }

        throw new IllegalArgumentException("Unsupported bound type: " + token.getText());
    }

    private static FrameBound.Type getUnboundedFrameBoundType(Token token)
    {
        switch (token.getType()) {
            case SqlBaseLexer.PRECEDING:
                return FrameBound.Type.UNBOUNDED_PRECEDING;
            case SqlBaseLexer.FOLLOWING:
                return FrameBound.Type.UNBOUNDED_FOLLOWING;
        }

        throw new IllegalArgumentException("Unsupported bound type: " + token.getText());
    }

    private static SampledRelation.Type getSamplingMethod(Token token)
    {
        switch (token.getType()) {
            case SqlBaseLexer.BERNOULLI:
                return SampledRelation.Type.BERNOULLI;
            case SqlBaseLexer.SYSTEM:
                return SampledRelation.Type.SYSTEM;
            case SqlBaseLexer.POISSONIZED:
                return SampledRelation.Type.POISSONIZED;
        }

        throw new IllegalArgumentException("Unsupported sampling method: " + token.getText());
    }

    private static LogicalBinaryExpression.Type getLogicalBinaryOperator(Token token)
    {
        switch (token.getType()) {
            case SqlBaseLexer.AND:
                return LogicalBinaryExpression.Type.AND;
            case SqlBaseLexer.OR:
                return LogicalBinaryExpression.Type.OR;
        }

        throw new IllegalArgumentException("Unsupported operator: " + token.getText());
    }

    private static SortItem.NullOrdering getNullOrderingType(Token token)
    {
        switch (token.getType()) {
            case SqlBaseLexer.FIRST:
                return SortItem.NullOrdering.FIRST;
            case SqlBaseLexer.LAST:
                return SortItem.NullOrdering.LAST;
        }

        throw new IllegalArgumentException("Unsupported ordering: " + token.getText());
    }

    private static SortItem.Ordering getOrderingType(Token token)
    {
        switch (token.getType()) {
            case SqlBaseLexer.ASC:
                return SortItem.Ordering.ASCENDING;
            case SqlBaseLexer.DESC:
                return SortItem.Ordering.DESCENDING;
        }

        throw new IllegalArgumentException("Unsupported ordering: " + token.getText());
    }

    private static String getType(SqlBaseParser.TypeContext type)
    {
        if (type.simpleType() != null) {
            return type.simpleType().getText();
        }

        if (type.ARRAY() != null) {
            return "ARRAY<" + getType(type.type(0)) + ">";
        }

        if (type.MAP() != null) {
            return "MAP<" + getType(type.type(0)) + "," + getType(type.type(1)) + ">";
        }

        throw new IllegalArgumentException("Unsupported type specification: " + type.getText());
    }

    private static void check(boolean condition, String message, ParserRuleContext context)
    {
        if (!condition) {
            throw new ParsingException(message, null, context.getStart().getLine(), context.getStart().getCharPositionInLine());
        }
    }
}
