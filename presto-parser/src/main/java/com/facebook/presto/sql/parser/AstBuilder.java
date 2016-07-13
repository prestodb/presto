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

import com.facebook.presto.sql.parser.SqlBaseParser.TablePropertiesContext;
import com.facebook.presto.sql.parser.SqlBaseParser.TablePropertyContext;
import com.facebook.presto.sql.tree.AddColumn;
import com.facebook.presto.sql.tree.AliasedRelation;
import com.facebook.presto.sql.tree.AllColumns;
import com.facebook.presto.sql.tree.Approximate;
import com.facebook.presto.sql.tree.ArithmeticBinaryExpression;
import com.facebook.presto.sql.tree.ArithmeticUnaryExpression;
import com.facebook.presto.sql.tree.ArrayConstructor;
import com.facebook.presto.sql.tree.AtTimeZone;
import com.facebook.presto.sql.tree.BetweenPredicate;
import com.facebook.presto.sql.tree.BinaryLiteral;
import com.facebook.presto.sql.tree.BooleanLiteral;
import com.facebook.presto.sql.tree.Call;
import com.facebook.presto.sql.tree.CallArgument;
import com.facebook.presto.sql.tree.Cast;
import com.facebook.presto.sql.tree.CoalesceExpression;
import com.facebook.presto.sql.tree.Commit;
import com.facebook.presto.sql.tree.ComparisonExpression;
import com.facebook.presto.sql.tree.CreateTable;
import com.facebook.presto.sql.tree.CreateTableAsSelect;
import com.facebook.presto.sql.tree.CreateView;
import com.facebook.presto.sql.tree.Cube;
import com.facebook.presto.sql.tree.CurrentTime;
import com.facebook.presto.sql.tree.Deallocate;
import com.facebook.presto.sql.tree.DecimalLiteral;
import com.facebook.presto.sql.tree.Delete;
import com.facebook.presto.sql.tree.DereferenceExpression;
import com.facebook.presto.sql.tree.DoubleLiteral;
import com.facebook.presto.sql.tree.DropTable;
import com.facebook.presto.sql.tree.DropView;
import com.facebook.presto.sql.tree.Except;
import com.facebook.presto.sql.tree.Execute;
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
import com.facebook.presto.sql.tree.Grant;
import com.facebook.presto.sql.tree.GroupBy;
import com.facebook.presto.sql.tree.GroupingElement;
import com.facebook.presto.sql.tree.GroupingSets;
import com.facebook.presto.sql.tree.IfExpression;
import com.facebook.presto.sql.tree.InListExpression;
import com.facebook.presto.sql.tree.InPredicate;
import com.facebook.presto.sql.tree.Insert;
import com.facebook.presto.sql.tree.Intersect;
import com.facebook.presto.sql.tree.IntervalLiteral;
import com.facebook.presto.sql.tree.IsNotNullPredicate;
import com.facebook.presto.sql.tree.IsNullPredicate;
import com.facebook.presto.sql.tree.Isolation;
import com.facebook.presto.sql.tree.Join;
import com.facebook.presto.sql.tree.JoinCriteria;
import com.facebook.presto.sql.tree.JoinOn;
import com.facebook.presto.sql.tree.JoinUsing;
import com.facebook.presto.sql.tree.LambdaExpression;
import com.facebook.presto.sql.tree.LikePredicate;
import com.facebook.presto.sql.tree.LogicalBinaryExpression;
import com.facebook.presto.sql.tree.LongLiteral;
import com.facebook.presto.sql.tree.NaturalJoin;
import com.facebook.presto.sql.tree.Node;
import com.facebook.presto.sql.tree.NodeLocation;
import com.facebook.presto.sql.tree.NotExpression;
import com.facebook.presto.sql.tree.NullIfExpression;
import com.facebook.presto.sql.tree.NullLiteral;
import com.facebook.presto.sql.tree.Prepare;
import com.facebook.presto.sql.tree.QualifiedName;
import com.facebook.presto.sql.tree.QualifiedNameReference;
import com.facebook.presto.sql.tree.Query;
import com.facebook.presto.sql.tree.QueryBody;
import com.facebook.presto.sql.tree.QuerySpecification;
import com.facebook.presto.sql.tree.Relation;
import com.facebook.presto.sql.tree.RenameColumn;
import com.facebook.presto.sql.tree.RenameTable;
import com.facebook.presto.sql.tree.ResetSession;
import com.facebook.presto.sql.tree.Revoke;
import com.facebook.presto.sql.tree.Rollback;
import com.facebook.presto.sql.tree.Rollup;
import com.facebook.presto.sql.tree.Row;
import com.facebook.presto.sql.tree.SampledRelation;
import com.facebook.presto.sql.tree.SearchedCaseExpression;
import com.facebook.presto.sql.tree.Select;
import com.facebook.presto.sql.tree.SelectItem;
import com.facebook.presto.sql.tree.SetSession;
import com.facebook.presto.sql.tree.ShowCatalogs;
import com.facebook.presto.sql.tree.ShowColumns;
import com.facebook.presto.sql.tree.ShowCreate;
import com.facebook.presto.sql.tree.ShowFunctions;
import com.facebook.presto.sql.tree.ShowPartitions;
import com.facebook.presto.sql.tree.ShowSchemas;
import com.facebook.presto.sql.tree.ShowSession;
import com.facebook.presto.sql.tree.ShowTables;
import com.facebook.presto.sql.tree.SimpleCaseExpression;
import com.facebook.presto.sql.tree.SimpleGroupBy;
import com.facebook.presto.sql.tree.SingleColumn;
import com.facebook.presto.sql.tree.SortItem;
import com.facebook.presto.sql.tree.StartTransaction;
import com.facebook.presto.sql.tree.Statement;
import com.facebook.presto.sql.tree.StringLiteral;
import com.facebook.presto.sql.tree.SubqueryExpression;
import com.facebook.presto.sql.tree.SubscriptExpression;
import com.facebook.presto.sql.tree.Table;
import com.facebook.presto.sql.tree.TableElement;
import com.facebook.presto.sql.tree.TableSubquery;
import com.facebook.presto.sql.tree.TimeLiteral;
import com.facebook.presto.sql.tree.TimestampLiteral;
import com.facebook.presto.sql.tree.TransactionAccessMode;
import com.facebook.presto.sql.tree.TransactionMode;
import com.facebook.presto.sql.tree.TryExpression;
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
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.Token;
import org.antlr.v4.runtime.tree.ParseTree;
import org.antlr.v4.runtime.tree.TerminalNode;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static com.google.common.collect.Iterables.getOnlyElement;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

class AstBuilder
        extends SqlBaseBaseVisitor<Node>
{
    @Override
    public Node visitSingleStatement(SqlBaseParser.SingleStatementContext context)
    {
        return visit(context.statement());
    }

    @Override
    public Node visitSingleExpression(SqlBaseParser.SingleExpressionContext context)
    {
        return visit(context.expression());
    }

    // ******************* statements **********************

    @Override
    public Node visitUse(SqlBaseParser.UseContext context)
    {
        return new Use(getLocation(context), getTextIfPresent(context.catalog), context.schema.getText());
    }

    @Override
    public Node visitCreateTableAsSelect(SqlBaseParser.CreateTableAsSelectContext context)
    {
        return new CreateTableAsSelect(getLocation(context), getQualifiedName(context.qualifiedName()), (Query) visit(context.query()), context.EXISTS() != null, processTableProperties(context.tableProperties()), context.NO() == null);
    }

    @Override
    public Node visitCreateTable(SqlBaseParser.CreateTableContext context)
    {
        return new CreateTable(getLocation(context), getQualifiedName(context.qualifiedName()), visit(context.tableElement(), TableElement.class), context.EXISTS() != null, processTableProperties(context.tableProperties()));
    }

    private Map<String, Expression> processTableProperties(TablePropertiesContext tablePropertiesContext)
    {
        ImmutableMap.Builder<String, Expression> properties = ImmutableMap.builder();
        if (tablePropertiesContext != null) {
            for (TablePropertyContext tablePropertyContext : tablePropertiesContext.tableProperty()) {
                properties.put(tablePropertyContext.identifier().getText(), (Expression) visit(tablePropertyContext.expression()));
            }
        }
        return properties.build();
    }

    @Override
    public Node visitShowCreateTable(SqlBaseParser.ShowCreateTableContext context)
    {
        return new ShowCreate(getLocation(context), ShowCreate.Type.TABLE, getQualifiedName(context.qualifiedName()));
    }

    @Override
    public Node visitDropTable(SqlBaseParser.DropTableContext context)
    {
        return new DropTable(getLocation(context), getQualifiedName(context.qualifiedName()), context.EXISTS() != null);
    }

    @Override
    public Node visitDropView(SqlBaseParser.DropViewContext context)
    {
        return new DropView(getLocation(context), getQualifiedName(context.qualifiedName()), context.EXISTS() != null);
    }

    @Override
    public Node visitInsertInto(SqlBaseParser.InsertIntoContext context)
    {
        return new Insert(
                getQualifiedName(context.qualifiedName()),
                Optional.ofNullable(getColumnAliases(context.columnAliases())),
                (Query) visit(context.query()));
    }

    @Override
    public Node visitDelete(SqlBaseParser.DeleteContext context)
    {
        return new Delete(
                getLocation(context),
                new Table(getLocation(context), getQualifiedName(context.qualifiedName())),
                visitIfPresent(context.booleanExpression(), Expression.class));
    }

    @Override
    public Node visitRenameTable(SqlBaseParser.RenameTableContext context)
    {
        return new RenameTable(getLocation(context), getQualifiedName(context.from), getQualifiedName(context.to));
    }

    @Override
    public Node visitRenameColumn(SqlBaseParser.RenameColumnContext context)
    {
        return new RenameColumn(getLocation(context), getQualifiedName(context.tableName), context.from.getText(), context.to.getText());
    }

    @Override
    public Node visitAddColumn(SqlBaseParser.AddColumnContext context)
    {
        return new AddColumn(getLocation(context), getQualifiedName(context.qualifiedName()), (TableElement) visit(context.tableElement()));
    }

    @Override
    public Node visitCreateView(SqlBaseParser.CreateViewContext context)
    {
        return new CreateView(
                getLocation(context),
                getQualifiedName(context.qualifiedName()),
                (Query) visit(context.query()),
                context.REPLACE() != null);
    }

    @Override
    public Node visitStartTransaction(SqlBaseParser.StartTransactionContext context)
    {
        return new StartTransaction(visit(context.transactionMode(), TransactionMode.class));
    }

    @Override
    public Node visitCommit(SqlBaseParser.CommitContext context)
    {
        return new Commit(getLocation(context));
    }

    @Override
    public Node visitRollback(SqlBaseParser.RollbackContext context)
    {
        return new Rollback(getLocation(context));
    }

    @Override
    public Node visitTransactionAccessMode(SqlBaseParser.TransactionAccessModeContext context)
    {
        return new TransactionAccessMode(getLocation(context), context.accessMode.getType() == SqlBaseLexer.ONLY);
    }

    @Override
    public Node visitIsolationLevel(SqlBaseParser.IsolationLevelContext context)
    {
        return visit(context.levelOfIsolation());
    }

    @Override
    public Node visitReadUncommitted(SqlBaseParser.ReadUncommittedContext context)
    {
        return new Isolation(getLocation(context), Isolation.Level.READ_UNCOMMITTED);
    }

    @Override
    public Node visitReadCommitted(SqlBaseParser.ReadCommittedContext context)
    {
        return new Isolation(getLocation(context), Isolation.Level.READ_COMMITTED);
    }

    @Override
    public Node visitRepeatableRead(SqlBaseParser.RepeatableReadContext context)
    {
        return new Isolation(getLocation(context), Isolation.Level.REPEATABLE_READ);
    }

    @Override
    public Node visitSerializable(SqlBaseParser.SerializableContext context)
    {
        return new Isolation(getLocation(context), Isolation.Level.SERIALIZABLE);
    }

    @Override
    public Node visitCall(SqlBaseParser.CallContext context)
    {
        return new Call(
                getLocation(context),
                getQualifiedName(context.qualifiedName()),
                visit(context.callArgument(), CallArgument.class));
    }

    @Override
    public Node visitPrepare(SqlBaseParser.PrepareContext context)
    {
        String name = context.identifier().getText();
        return new Prepare(getLocation(context), name, (Statement) visit(context.statement()));
    }

    @Override
    public Node visitDeallocate(SqlBaseParser.DeallocateContext context)
    {
        String name = context.identifier().getText();
        return new Deallocate(getLocation(context), name);
    }

    @Override
    public Node visitExecute(SqlBaseParser.ExecuteContext context)
    {
        String name = context.identifier().getText();
        return new Execute(getLocation(context), name);
    }

    // ********************** query expressions ********************

    @Override
    public Node visitQuery(SqlBaseParser.QueryContext context)
    {
        Query body = (Query) visit(context.queryNoWith());

        return new Query(
                getLocation(context),
                visitIfPresent(context.with(), With.class),
                body.getQueryBody(),
                body.getOrderBy(),
                body.getLimit(),
                body.getApproximate());
    }

    @Override
    public Node visitWith(SqlBaseParser.WithContext context)
    {
        return new With(getLocation(context), context.RECURSIVE() != null, visit(context.namedQuery(), WithQuery.class));
    }

    @Override
    public Node visitNamedQuery(SqlBaseParser.NamedQueryContext context)
    {
        return new WithQuery(getLocation(context), context.name.getText(), (Query) visit(context.query()), Optional.ofNullable(getColumnAliases(context.columnAliases())));
    }

    @Override
    public Node visitQueryNoWith(SqlBaseParser.QueryNoWithContext context)
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
                    getLocation(context),
                    Optional.<With>empty(),
                    new QuerySpecification(
                            getLocation(context),
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
                            .map(confidence -> new Approximate(getLocation(context), confidence)));
        }

        return new Query(
                getLocation(context),
                Optional.<With>empty(),
                term,
                visit(context.sortItem(), SortItem.class),
                getTextIfPresent(context.limit),
                getTextIfPresent(context.confidence)
                        .map(confidence -> new Approximate(getLocation(context), confidence)));
    }

    @Override
    public Node visitQuerySpecification(SqlBaseParser.QuerySpecificationContext context)
    {
        Optional<Relation> from = Optional.empty();

        List<Relation> relations = visit(context.relation(), Relation.class);
        if (!relations.isEmpty()) {
            // synthesize implicit join nodes
            Iterator<Relation> iterator = relations.iterator();
            Relation relation = iterator.next();

            while (iterator.hasNext()) {
                relation = new Join(getLocation(context), Join.Type.IMPLICIT, relation, iterator.next(), Optional.<JoinCriteria>empty());
            }

            from = Optional.of(relation);
        }

        return new QuerySpecification(
                getLocation(context),
                new Select(getLocation(context.SELECT()), isDistinct(context.setQuantifier()), visit(context.selectItem(), SelectItem.class)),
                from,
                visitIfPresent(context.where, Expression.class),
                visitIfPresent(context.groupBy(), GroupBy.class),
                visitIfPresent(context.having, Expression.class),
                ImmutableList.of(),
                Optional.<String>empty());
    }

    @Override
    public Node visitGroupBy(SqlBaseParser.GroupByContext context)
    {
        return new GroupBy(getLocation(context), isDistinct(context.setQuantifier()), visit(context.groupingElement(), GroupingElement.class));
    }

    @Override
    public Node visitSingleGroupingSet(SqlBaseParser.SingleGroupingSetContext context)
    {
        return new SimpleGroupBy(getLocation(context), visit(context.groupingExpressions().expression(), Expression.class));
    }

    @Override
    public Node visitRollup(SqlBaseParser.RollupContext context)
    {
        return new Rollup(getLocation(context), context.qualifiedName().stream()
                .map(AstBuilder::getQualifiedName)
                .collect(toList()));
    }

    @Override
    public Node visitCube(SqlBaseParser.CubeContext context)
    {
        return new Cube(getLocation(context), context.qualifiedName().stream()
                .map(AstBuilder::getQualifiedName)
                .collect(toList()));
    }

    @Override
    public Node visitMultipleGroupingSets(SqlBaseParser.MultipleGroupingSetsContext context)
    {
        return new GroupingSets(getLocation(context), context.groupingSet().stream()
                .map(groupingSet -> groupingSet.qualifiedName().stream()
                        .map(AstBuilder::getQualifiedName)
                        .collect(toList()))
                .collect(toList()));
    }

    @Override
    public Node visitSetOperation(SqlBaseParser.SetOperationContext context)
    {
        QueryBody left = (QueryBody) visit(context.left);
        QueryBody right = (QueryBody) visit(context.right);

        boolean distinct = context.setQuantifier() == null || context.setQuantifier().DISTINCT() != null;

        switch (context.operator.getType()) {
            case SqlBaseLexer.UNION:
                return new Union(getLocation(context.UNION()), ImmutableList.of(left, right), distinct);
            case SqlBaseLexer.INTERSECT:
                return new Intersect(getLocation(context.INTERSECT()), ImmutableList.of(left, right), distinct);
            case SqlBaseLexer.EXCEPT:
                return new Except(getLocation(context.EXCEPT()), left, right, distinct);
        }

        throw new IllegalArgumentException("Unsupported set operation: " + context.operator.getText());
    }

    @Override
    public Node visitSelectAll(SqlBaseParser.SelectAllContext context)
    {
        if (context.qualifiedName() != null) {
            return new AllColumns(getLocation(context), getQualifiedName(context.qualifiedName()));
        }

        return new AllColumns(getLocation(context));
    }

    @Override
    public Node visitSelectSingle(SqlBaseParser.SelectSingleContext context)
    {
        Optional<String> alias = getTextIfPresent(context.identifier());

        return new SingleColumn(getLocation(context), (Expression) visit(context.expression()), alias);
    }

    @Override
    public Node visitTable(SqlBaseParser.TableContext context)
    {
        return new Table(getLocation(context), getQualifiedName(context.qualifiedName()));
    }

    @Override
    public Node visitSubquery(SqlBaseParser.SubqueryContext context)
    {
        return new TableSubquery(getLocation(context), (Query) visit(context.queryNoWith()));
    }

    @Override
    public Node visitInlineTable(SqlBaseParser.InlineTableContext context)
    {
        return new Values(getLocation(context), visit(context.expression(), Expression.class));
    }

    @Override
    public Node visitExplain(SqlBaseParser.ExplainContext context)
    {
        return new Explain(getLocation(context), context.ANALYZE() != null, (Statement) visit(context.statement()), visit(context.explainOption(), ExplainOption.class));
    }

    @Override
    public Node visitExplainFormat(SqlBaseParser.ExplainFormatContext context)
    {
        switch (context.value.getType()) {
            case SqlBaseLexer.GRAPHVIZ:
                return new ExplainFormat(getLocation(context), ExplainFormat.Type.GRAPHVIZ);
            case SqlBaseLexer.TEXT:
                return new ExplainFormat(getLocation(context), ExplainFormat.Type.TEXT);
        }

        throw new IllegalArgumentException("Unsupported EXPLAIN format: " + context.value.getText());
    }

    @Override
    public Node visitExplainType(SqlBaseParser.ExplainTypeContext context)
    {
        switch (context.value.getType()) {
            case SqlBaseLexer.LOGICAL:
                return new ExplainType(getLocation(context), ExplainType.Type.LOGICAL);
            case SqlBaseLexer.DISTRIBUTED:
                return new ExplainType(getLocation(context), ExplainType.Type.DISTRIBUTED);
        }

        throw new IllegalArgumentException("Unsupported EXPLAIN type: " + context.value.getText());
    }

    @Override
    public Node visitShowTables(SqlBaseParser.ShowTablesContext context)
    {
        return new ShowTables(
                getLocation(context),
                Optional.ofNullable(context.qualifiedName())
                        .map(AstBuilder::getQualifiedName),
                getTextIfPresent(context.pattern)
                        .map(AstBuilder::unquote));
    }

    @Override
    public Node visitShowSchemas(SqlBaseParser.ShowSchemasContext context)
    {
        return new ShowSchemas(
                getLocation(context),
                getTextIfPresent(context.identifier()),
                getTextIfPresent(context.pattern)
                        .map(AstBuilder::unquote));
    }

    @Override
    public Node visitShowCatalogs(SqlBaseParser.ShowCatalogsContext context)
    {
        return new ShowCatalogs(getLocation(context),
                getTextIfPresent(context.pattern)
                        .map(AstBuilder::unquote));
    }

    @Override
    public Node visitShowColumns(SqlBaseParser.ShowColumnsContext context)
    {
        return new ShowColumns(getLocation(context), getQualifiedName(context.qualifiedName()));
    }

    @Override
    public Node visitShowPartitions(SqlBaseParser.ShowPartitionsContext context)
    {
        return new ShowPartitions(
                getLocation(context),
                getQualifiedName(context.qualifiedName()),
                visitIfPresent(context.booleanExpression(), Expression.class),
                visit(context.sortItem(), SortItem.class),
                getTextIfPresent(context.limit));
    }

    @Override
    public Node visitShowCreateView(SqlBaseParser.ShowCreateViewContext context)
    {
        return new ShowCreate(getLocation(context), ShowCreate.Type.VIEW, getQualifiedName(context.qualifiedName()));
    }

    @Override
    public Node visitShowFunctions(SqlBaseParser.ShowFunctionsContext context)
    {
        return new ShowFunctions(getLocation(context));
    }

    @Override
    public Node visitShowSession(SqlBaseParser.ShowSessionContext context)
    {
        return new ShowSession(getLocation(context));
    }

    @Override
    public Node visitSetSession(SqlBaseParser.SetSessionContext context)
    {
        return new SetSession(getLocation(context), getQualifiedName(context.qualifiedName()), (Expression) visit(context.expression()));
    }

    @Override
    public Node visitResetSession(SqlBaseParser.ResetSessionContext context)
    {
        return new ResetSession(getLocation(context), getQualifiedName(context.qualifiedName()));
    }

    @Override
    public Node visitGrant(SqlBaseParser.GrantContext context)
    {
        String grantee = context.grantee.getText();

        Optional<List<String>> privileges;
        if (context.ALL() != null) {
            privileges = Optional.empty();
        }
        else {
            privileges = Optional.of(context.privilege().stream()
                    .map(SqlBaseParser.PrivilegeContext::getText)
                    .collect(toList()));
        }
        return new Grant(
                getLocation(context),
                privileges,
                context.TABLE() != null,
                getQualifiedName(context.qualifiedName()),
                grantee,
                context.OPTION() != null);
    }

    @Override
    public Node visitRevoke(SqlBaseParser.RevokeContext context)
    {
        Optional<List<String>> privileges;
        if (context.ALL() != null) {
            privileges = Optional.empty();
        }
        else {
            privileges = Optional.of(context.privilege().stream()
                    .map(SqlBaseParser.PrivilegeContext::getText)
                    .collect(toList()));
        }
        return new Revoke(
                getLocation(context),
                context.OPTION() != null,
                privileges,
                context.TABLE() != null,
                getQualifiedName(context.qualifiedName()),
                context.grantee.getText());
    }

    // ***************** boolean expressions ******************

    @Override
    public Node visitLogicalNot(SqlBaseParser.LogicalNotContext context)
    {
        return new NotExpression(getLocation(context), (Expression) visit(context.booleanExpression()));
    }

    @Override
    public Node visitLogicalBinary(SqlBaseParser.LogicalBinaryContext context)
    {
        return new LogicalBinaryExpression(
                getLocation(context.operator),
                getLogicalBinaryOperator(context.operator),
                (Expression) visit(context.left),
                (Expression) visit(context.right));
    }

    // *************** from clause *****************

    @Override
    public Node visitJoinRelation(SqlBaseParser.JoinRelationContext context)
    {
        Relation left = (Relation) visit(context.left);
        Relation right;

        if (context.CROSS() != null) {
            right = (Relation) visit(context.right);
            return new Join(getLocation(context), Join.Type.CROSS, left, right, Optional.<JoinCriteria>empty());
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
                        .collect(toList());

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

        return new Join(getLocation(context), joinType, left, right, Optional.of(criteria));
    }

    @Override
    public Node visitSampledRelation(SqlBaseParser.SampledRelationContext context)
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
                getLocation(context),
                child,
                getSamplingMethod((Token) context.sampleType().getChild(0).getPayload()),
                (Expression) visit(context.percentage),
                context.RESCALED() != null,
                stratifyOn);
    }

    @Override
    public Node visitAliasedRelation(SqlBaseParser.AliasedRelationContext context)
    {
        Relation child = (Relation) visit(context.relationPrimary());

        if (context.identifier() == null) {
            return child;
        }

        return new AliasedRelation(getLocation(context), child, context.identifier().getText(), getColumnAliases(context.columnAliases()));
    }

    @Override
    public Node visitTableName(SqlBaseParser.TableNameContext context)
    {
        return new Table(getLocation(context), getQualifiedName(context.qualifiedName()));
    }

    @Override
    public Node visitSubqueryRelation(SqlBaseParser.SubqueryRelationContext context)
    {
        return new TableSubquery(getLocation(context), (Query) visit(context.query()));
    }

    @Override
    public Node visitUnnest(SqlBaseParser.UnnestContext context)
    {
        return new Unnest(getLocation(context), visit(context.expression(), Expression.class), context.ORDINALITY() != null);
    }

    @Override
    public Node visitParenthesizedRelation(SqlBaseParser.ParenthesizedRelationContext context)
    {
        return visit(context.relation());
    }

    // ********************* predicates *******************

    @Override
    public Node visitPredicated(SqlBaseParser.PredicatedContext context)
    {
        if (context.predicate() != null) {
            return visit(context.predicate());
        }

        return visit(context.valueExpression);
    }

    @Override
    public Node visitComparison(SqlBaseParser.ComparisonContext context)
    {
        return new ComparisonExpression(
                getLocation(context.comparisonOperator()),
                getComparisonOperator(((TerminalNode) context.comparisonOperator().getChild(0)).getSymbol()),
                (Expression) visit(context.value),
                (Expression) visit(context.right));
    }

    @Override
    public Node visitDistinctFrom(SqlBaseParser.DistinctFromContext context)
    {
        Expression expression = new ComparisonExpression(
                getLocation(context),
                ComparisonExpression.Type.IS_DISTINCT_FROM,
                (Expression) visit(context.value),
                (Expression) visit(context.right));

        if (context.NOT() != null) {
            expression = new NotExpression(getLocation(context), expression);
        }

        return expression;
    }

    @Override
    public Node visitBetween(SqlBaseParser.BetweenContext context)
    {
        Expression expression = new BetweenPredicate(
                getLocation(context),
                (Expression) visit(context.value),
                (Expression) visit(context.lower),
                (Expression) visit(context.upper));

        if (context.NOT() != null) {
            expression = new NotExpression(getLocation(context), expression);
        }

        return expression;
    }

    @Override
    public Node visitNullPredicate(SqlBaseParser.NullPredicateContext context)
    {
        Expression child = (Expression) visit(context.value);

        if (context.NOT() == null) {
            return new IsNullPredicate(getLocation(context), child);
        }

        return new IsNotNullPredicate(getLocation(context), child);
    }

    @Override
    public Node visitLike(SqlBaseParser.LikeContext context)
    {
        Expression escape = null;
        if (context.escape != null) {
            escape = (Expression) visit(context.escape);
        }

        Expression result = new LikePredicate(getLocation(context), (Expression) visit(context.value), (Expression) visit(context.pattern), escape);

        if (context.NOT() != null) {
            result = new NotExpression(getLocation(context), result);
        }

        return result;
    }

    @Override
    public Node visitInList(SqlBaseParser.InListContext context)
    {
        Expression result = new InPredicate(
                getLocation(context),
                (Expression) visit(context.value),
                new InListExpression(getLocation(context), visit(context.expression(), Expression.class)));

        if (context.NOT() != null) {
            result = new NotExpression(getLocation(context), result);
        }

        return result;
    }

    @Override
    public Node visitInSubquery(SqlBaseParser.InSubqueryContext context)
    {
        Expression result = new InPredicate(
                getLocation(context),
                (Expression) visit(context.value),
                new SubqueryExpression(getLocation(context), (Query) visit(context.query())));

        if (context.NOT() != null) {
            result = new NotExpression(getLocation(context), result);
        }

        return result;
    }

    @Override
    public Node visitExists(SqlBaseParser.ExistsContext context)
    {
        return new ExistsPredicate(getLocation(context), (Query) visit(context.query()));
    }

    // ************** value expressions **************

    @Override
    public Node visitArithmeticUnary(SqlBaseParser.ArithmeticUnaryContext context)
    {
        Expression child = (Expression) visit(context.valueExpression());

        switch (context.operator.getType()) {
            case SqlBaseLexer.MINUS:
                return ArithmeticUnaryExpression.negative(getLocation(context), child);
            case SqlBaseLexer.PLUS:
                return ArithmeticUnaryExpression.positive(getLocation(context), child);
            default:
                throw new UnsupportedOperationException("Unsupported sign: " + context.operator.getText());
        }
    }

    @Override
    public Node visitArithmeticBinary(SqlBaseParser.ArithmeticBinaryContext context)
    {
        return new ArithmeticBinaryExpression(
                getLocation(context.operator),
                getArithmeticBinaryOperator(context.operator),
                (Expression) visit(context.left),
                (Expression) visit(context.right));
    }

    @Override
    public Node visitConcatenation(SqlBaseParser.ConcatenationContext context)
    {
        return new FunctionCall(
                getLocation(context.CONCAT()),
                QualifiedName.of("concat"), ImmutableList.of(
                (Expression) visit(context.left),
                (Expression) visit(context.right)));
    }

    @Override
    public Node visitAtTimeZone(SqlBaseParser.AtTimeZoneContext context)
    {
        return new AtTimeZone(
                getLocation(context.AT()),
                (Expression) visit(context.valueExpression()),
                (Expression) visit(context.timeZoneSpecifier()));
    }

    @Override
    public Node visitTimeZoneInterval(SqlBaseParser.TimeZoneIntervalContext context)
    {
        return visit(context.interval());
    }

    @Override
    public Node visitTimeZoneString(SqlBaseParser.TimeZoneStringContext context)
    {
        return new StringLiteral(getLocation(context), unquote(context.STRING().getText()));
    }

    // ********************* primary expressions **********************

    @Override
    public Node visitParenthesizedExpression(SqlBaseParser.ParenthesizedExpressionContext context)
    {
        return visit(context.expression());
    }

    @Override
    public Node visitRowConstructor(SqlBaseParser.RowConstructorContext context)
    {
        return new Row(getLocation(context), visit(context.expression(), Expression.class));
    }

    @Override
    public Node visitArrayConstructor(SqlBaseParser.ArrayConstructorContext context)
    {
        return new ArrayConstructor(getLocation(context), visit(context.expression(), Expression.class));
    }

    @Override
    public Node visitCast(SqlBaseParser.CastContext context)
    {
        boolean isTryCast = context.TRY_CAST() != null;
        return new Cast(getLocation(context), (Expression) visit(context.expression()), getType(context.type()), isTryCast);
    }

    @Override
    public Node visitSpecialDateTimeFunction(SqlBaseParser.SpecialDateTimeFunctionContext context)
    {
        CurrentTime.Type type = getDateTimeFunctionType(context.name);

        if (context.precision != null) {
            return new CurrentTime(getLocation(context), type, Integer.parseInt(context.precision.getText()));
        }

        return new CurrentTime(getLocation(context), type);
    }

    @Override
    public Node visitExtract(SqlBaseParser.ExtractContext context)
    {
        String fieldString = context.identifier().getText();
        Extract.Field field;
        try {
            field = Extract.Field.valueOf(fieldString.toUpperCase());
        }
        catch (IllegalArgumentException e) {
            throw new ParsingException(format("Invalid EXTRACT field: %s", fieldString), null, context.getStart().getLine(), context.getStart().getCharPositionInLine());
        }
        return new Extract(getLocation(context), (Expression) visit(context.valueExpression()), field);
    }

    @Override
    public Node visitSubstring(SqlBaseParser.SubstringContext context)
    {
        return new FunctionCall(getLocation(context), QualifiedName.of("substr"), visit(context.valueExpression(), Expression.class));
    }

    @Override
    public Node visitPosition(SqlBaseParser.PositionContext context)
    {
        List<Expression> arguments = Lists.reverse(visit(context.valueExpression(), Expression.class));
        return new FunctionCall(getLocation(context), QualifiedName.of("strpos"), arguments);
    }

    @Override
    public Node visitNormalize(SqlBaseParser.NormalizeContext context)
    {
        Expression str = (Expression) visit(context.valueExpression());
        String normalForm = Optional.ofNullable(context.normalForm()).map(ParserRuleContext::getText).orElse("NFC");
        return new FunctionCall(getLocation(context), QualifiedName.of("normalize"), ImmutableList.of(str, new StringLiteral(getLocation(context), normalForm)));
    }

    @Override
    public Node visitSubscript(SqlBaseParser.SubscriptContext context)
    {
        return new SubscriptExpression(getLocation(context), (Expression) visit(context.value), (Expression) visit(context.index));
    }

    @Override
    public Node visitSubqueryExpression(SqlBaseParser.SubqueryExpressionContext context)
    {
        return new SubqueryExpression(getLocation(context), (Query) visit(context.query()));
    }

    @Override
    public Node visitDereference(SqlBaseParser.DereferenceContext context)
    {
        return new DereferenceExpression(getLocation(context), (Expression) visit(context.base), context.fieldName.getText());
    }

    @Override
    public Node visitColumnReference(SqlBaseParser.ColumnReferenceContext context)
    {
        return new QualifiedNameReference(getLocation(context), QualifiedName.of(context.getText()));
    }

    @Override
    public Node visitSimpleCase(SqlBaseParser.SimpleCaseContext context)
    {
        return new SimpleCaseExpression(
                getLocation(context),
                (Expression) visit(context.valueExpression()),
                visit(context.whenClause(), WhenClause.class),
                visitIfPresent(context.elseExpression, Expression.class));
    }

    @Override
    public Node visitSearchedCase(SqlBaseParser.SearchedCaseContext context)
    {
        return new SearchedCaseExpression(
                getLocation(context),
                visit(context.whenClause(), WhenClause.class),
                visitIfPresent(context.elseExpression, Expression.class));
    }

    @Override
    public Node visitWhenClause(SqlBaseParser.WhenClauseContext context)
    {
        return new WhenClause(getLocation(context), (Expression) visit(context.condition), (Expression) visit(context.result));
    }

    @Override
    public Node visitFunctionCall(SqlBaseParser.FunctionCallContext context)
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
                    getLocation(context),
                    (Expression) visit(context.expression(0)),
                    (Expression) visit(context.expression(1)),
                    elseExpression);
        }
        if (name.toString().equalsIgnoreCase("nullif")) {
            check(context.expression().size() == 2, "Invalid number of arguments for 'nullif' function", context);
            check(!window.isPresent(), "OVER clause not valid for 'nullif' function", context);
            check(!distinct, "DISTINCT not valid for 'nullif' function", context);

            return new NullIfExpression(
                    getLocation(context),
                    (Expression) visit(context.expression(0)),
                    (Expression) visit(context.expression(1)));
        }
        if (name.toString().equalsIgnoreCase("coalesce")) {
            check(!window.isPresent(), "OVER clause not valid for 'coalesce' function", context);
            check(!distinct, "DISTINCT not valid for 'coalesce' function", context);

            return new CoalesceExpression(getLocation(context), visit(context.expression(), Expression.class));
        }
        if (name.toString().equalsIgnoreCase("try")) {
            check(!window.isPresent(), "OVER clause not valid for 'try' function", context);
            check(!distinct, "DISTINCT not valid for 'try' function", context);

            return new TryExpression(getLocation(context), (Expression) visit(getOnlyElement(context.expression())));
        }

        return new FunctionCall(
                getLocation(context),
                getQualifiedName(context.qualifiedName()),
                window,
                distinct,
                visit(context.expression(), Expression.class));
    }

    @Override
    public Node visitLambda(SqlBaseParser.LambdaContext context)
    {
        List<String> arguments = context.identifier().stream()
                .map(SqlBaseParser.IdentifierContext::getText)
                .collect(toList());

        Expression body = (Expression) visit(context.expression());

        return new LambdaExpression(arguments, body);
    }

    @Override
    public Node visitOver(SqlBaseParser.OverContext context)
    {
        return new Window(
                getLocation(context),
                visit(context.partition, Expression.class),
                visit(context.sortItem(), SortItem.class),
                visitIfPresent(context.windowFrame(), WindowFrame.class));
    }

    @Override
    public Node visitTableElement(SqlBaseParser.TableElementContext context)
    {
        return new TableElement(getLocation(context), context.identifier().getText(), getType(context.type()));
    }

    @Override
    public Node visitSortItem(SqlBaseParser.SortItemContext context)
    {
        return new SortItem(
                getLocation(context),
                (Expression) visit(context.expression()),
                Optional.ofNullable(context.ordering)
                        .map(AstBuilder::getOrderingType)
                        .orElse(SortItem.Ordering.ASCENDING),
                Optional.ofNullable(context.nullOrdering)
                        .map(AstBuilder::getNullOrderingType)
                        .orElse(SortItem.NullOrdering.UNDEFINED));
    }

    @Override
    public Node visitWindowFrame(SqlBaseParser.WindowFrameContext context)
    {
        return new WindowFrame(
                getLocation(context),
                getFrameType(context.frameType),
                (FrameBound) visit(context.start),
                visitIfPresent(context.end, FrameBound.class));
    }

    @Override
    public Node visitUnboundedFrame(SqlBaseParser.UnboundedFrameContext context)
    {
        return new FrameBound(getLocation(context), getUnboundedFrameBoundType(context.boundType));
    }

    @Override
    public Node visitBoundedFrame(SqlBaseParser.BoundedFrameContext context)
    {
        return new FrameBound(getLocation(context), getBoundedFrameBoundType(context.boundType), (Expression) visit(context.expression()));
    }

    @Override
    public Node visitCurrentRowBound(SqlBaseParser.CurrentRowBoundContext context)
    {
        return new FrameBound(getLocation(context), FrameBound.Type.CURRENT_ROW);
    }

    // ************** literals **************

    @Override
    public Node visitNullLiteral(SqlBaseParser.NullLiteralContext context)
    {
        return new NullLiteral(getLocation(context));
    }

    @Override
    public Node visitStringLiteral(SqlBaseParser.StringLiteralContext context)
    {
        return new StringLiteral(getLocation(context), unquote(context.STRING().getText()));
    }

    @Override
    public Node visitBinaryLiteral(SqlBaseParser.BinaryLiteralContext context)
    {
        String raw = context.BINARY_LITERAL().getText();
        return new BinaryLiteral(getLocation(context), unquote(raw.substring(1)));
    }

    @Override
    public Node visitTypeConstructor(SqlBaseParser.TypeConstructorContext context)
    {
        String type = context.identifier().getText();
        String value = unquote(context.STRING().getText());

        if (type.equalsIgnoreCase("time")) {
            return new TimeLiteral(getLocation(context), value);
        }
        if (type.equalsIgnoreCase("timestamp")) {
            return new TimestampLiteral(getLocation(context), value);
        }
        if (type.equalsIgnoreCase("decimal")) {
            return new DecimalLiteral(getLocation(context), value);
        }

        return new GenericLiteral(getLocation(context), type, value);
    }

    @Override
    public Node visitIntegerLiteral(SqlBaseParser.IntegerLiteralContext context)
    {
        return new LongLiteral(getLocation(context), context.getText());
    }

    @Override
    public Node visitDecimalLiteral(SqlBaseParser.DecimalLiteralContext context)
    {
        return new DoubleLiteral(getLocation(context), context.getText());
    }

    @Override
    public Node visitBooleanValue(SqlBaseParser.BooleanValueContext context)
    {
        return new BooleanLiteral(getLocation(context), context.getText());
    }

    @Override
    public Node visitInterval(SqlBaseParser.IntervalContext context)
    {
        return new IntervalLiteral(
                getLocation(context),
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

    // ***************** arguments *****************

    @Override
    public Node visitPositionalArgument(SqlBaseParser.PositionalArgumentContext context)
    {
        return new CallArgument(getLocation(context), (Expression) visit(context.expression()));
    }

    @Override
    public Node visitNamedArgument(SqlBaseParser.NamedArgumentContext context)
    {
        return new CallArgument(getLocation(context), context.identifier().getText(), (Expression) visit(context.expression()));
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
                .collect(toList());
    }

    private static String unquote(String value)
    {
        return value.substring(1, value.length() - 1)
                .replace("''", "'");
    }

    private static QualifiedName getQualifiedName(SqlBaseParser.QualifiedNameContext context)
    {
        List<String> parts = context
                .identifier().stream()
                .map(ParseTree::getText)
                .collect(toList());

        return QualifiedName.of(parts);
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
                .collect(toList());
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
        if (type.baseType() != null) {
            String signature = type.baseType().getText();
            if (!type.typeParameter().isEmpty()) {
                String typeParameterSignature = type
                        .typeParameter()
                        .stream()
                        .map(AstBuilder::typeParameterToString)
                        .collect(Collectors.joining(","));
                signature += "(" + typeParameterSignature + ")";
            }
            return signature;
        }

        if (type.ARRAY() != null) {
            return "ARRAY(" + getType(type.type(0)) + ")";
        }

        if (type.MAP() != null) {
            return "MAP(" + getType(type.type(0)) + "," + getType(type.type(1)) + ")";
        }

        if (type.ROW() != null) {
            StringBuilder builder = new StringBuilder("(");
            for (int i = 0; i < type.identifier().size(); i++) {
                if (i != 0) {
                    builder.append(",");
                }
                builder.append(type.identifier(i).getText())
                        .append(" ")
                        .append(getType(type.type(i)));
            }
            builder.append(")");
            return "ROW" + builder.toString();
        }

        throw new IllegalArgumentException("Unsupported type specification: " + type.getText());
    }

    private static String typeParameterToString(SqlBaseParser.TypeParameterContext typeParameter)
    {
        if (typeParameter.INTEGER_VALUE() != null) {
            return typeParameter.INTEGER_VALUE().toString();
        }
        if (typeParameter.type() != null) {
            return getType(typeParameter.type());
        }
        throw new IllegalArgumentException("Unsupported typeParameter: " + typeParameter.getText());
    }

    private static void check(boolean condition, String message, ParserRuleContext context)
    {
        if (!condition) {
            throw new ParsingException(message, null, context.getStart().getLine(), context.getStart().getCharPositionInLine());
        }
    }

    public static NodeLocation getLocation(TerminalNode terminalNode)
    {
        requireNonNull(terminalNode, "terminalNode is null");
        return getLocation(terminalNode.getSymbol());
    }

    public static NodeLocation getLocation(ParserRuleContext parserRuleContext)
    {
        requireNonNull(parserRuleContext, "parserRuleContext is null");
        return getLocation(parserRuleContext.getStart());
    }

    public static NodeLocation getLocation(Token token)
    {
        requireNonNull(token, "token is null");
        return new NodeLocation(token.getLine(), token.getCharPositionInLine());
    }
}
