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

import com.facebook.presto.sql.tree.AddColumn;
import com.facebook.presto.sql.tree.AddConstraint;
import com.facebook.presto.sql.tree.AliasedRelation;
import com.facebook.presto.sql.tree.AllColumns;
import com.facebook.presto.sql.tree.AlterColumnNotNull;
import com.facebook.presto.sql.tree.AlterFunction;
import com.facebook.presto.sql.tree.AlterRoutineCharacteristics;
import com.facebook.presto.sql.tree.Analyze;
import com.facebook.presto.sql.tree.ArithmeticBinaryExpression;
import com.facebook.presto.sql.tree.ArithmeticUnaryExpression;
import com.facebook.presto.sql.tree.ArrayConstructor;
import com.facebook.presto.sql.tree.AtTimeZone;
import com.facebook.presto.sql.tree.BetweenPredicate;
import com.facebook.presto.sql.tree.BinaryLiteral;
import com.facebook.presto.sql.tree.BindExpression;
import com.facebook.presto.sql.tree.BooleanLiteral;
import com.facebook.presto.sql.tree.Call;
import com.facebook.presto.sql.tree.CallArgument;
import com.facebook.presto.sql.tree.Cast;
import com.facebook.presto.sql.tree.CharLiteral;
import com.facebook.presto.sql.tree.CoalesceExpression;
import com.facebook.presto.sql.tree.ColumnDefinition;
import com.facebook.presto.sql.tree.Commit;
import com.facebook.presto.sql.tree.ComparisonExpression;
import com.facebook.presto.sql.tree.ConstraintSpecification;
import com.facebook.presto.sql.tree.CreateFunction;
import com.facebook.presto.sql.tree.CreateMaterializedView;
import com.facebook.presto.sql.tree.CreateRole;
import com.facebook.presto.sql.tree.CreateSchema;
import com.facebook.presto.sql.tree.CreateTable;
import com.facebook.presto.sql.tree.CreateTableAsSelect;
import com.facebook.presto.sql.tree.CreateType;
import com.facebook.presto.sql.tree.CreateView;
import com.facebook.presto.sql.tree.Cube;
import com.facebook.presto.sql.tree.CurrentTime;
import com.facebook.presto.sql.tree.CurrentUser;
import com.facebook.presto.sql.tree.Deallocate;
import com.facebook.presto.sql.tree.DecimalLiteral;
import com.facebook.presto.sql.tree.Delete;
import com.facebook.presto.sql.tree.DereferenceExpression;
import com.facebook.presto.sql.tree.DescribeInput;
import com.facebook.presto.sql.tree.DescribeOutput;
import com.facebook.presto.sql.tree.DoubleLiteral;
import com.facebook.presto.sql.tree.DropColumn;
import com.facebook.presto.sql.tree.DropConstraint;
import com.facebook.presto.sql.tree.DropFunction;
import com.facebook.presto.sql.tree.DropMaterializedView;
import com.facebook.presto.sql.tree.DropRole;
import com.facebook.presto.sql.tree.DropSchema;
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
import com.facebook.presto.sql.tree.ExternalBodyReference;
import com.facebook.presto.sql.tree.Extract;
import com.facebook.presto.sql.tree.FrameBound;
import com.facebook.presto.sql.tree.FunctionCall;
import com.facebook.presto.sql.tree.GenericLiteral;
import com.facebook.presto.sql.tree.Grant;
import com.facebook.presto.sql.tree.GrantRoles;
import com.facebook.presto.sql.tree.GrantorSpecification;
import com.facebook.presto.sql.tree.GroupBy;
import com.facebook.presto.sql.tree.GroupingElement;
import com.facebook.presto.sql.tree.GroupingOperation;
import com.facebook.presto.sql.tree.GroupingSets;
import com.facebook.presto.sql.tree.Identifier;
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
import com.facebook.presto.sql.tree.LambdaArgumentDeclaration;
import com.facebook.presto.sql.tree.LambdaExpression;
import com.facebook.presto.sql.tree.Lateral;
import com.facebook.presto.sql.tree.LikeClause;
import com.facebook.presto.sql.tree.LikePredicate;
import com.facebook.presto.sql.tree.LogicalBinaryExpression;
import com.facebook.presto.sql.tree.LongLiteral;
import com.facebook.presto.sql.tree.Merge;
import com.facebook.presto.sql.tree.MergeCase;
import com.facebook.presto.sql.tree.MergeInsert;
import com.facebook.presto.sql.tree.MergeUpdate;
import com.facebook.presto.sql.tree.NaturalJoin;
import com.facebook.presto.sql.tree.Node;
import com.facebook.presto.sql.tree.NodeLocation;
import com.facebook.presto.sql.tree.NotExpression;
import com.facebook.presto.sql.tree.NullIfExpression;
import com.facebook.presto.sql.tree.NullLiteral;
import com.facebook.presto.sql.tree.Offset;
import com.facebook.presto.sql.tree.OrderBy;
import com.facebook.presto.sql.tree.Parameter;
import com.facebook.presto.sql.tree.Prepare;
import com.facebook.presto.sql.tree.PrincipalSpecification;
import com.facebook.presto.sql.tree.Property;
import com.facebook.presto.sql.tree.QualifiedName;
import com.facebook.presto.sql.tree.QuantifiedComparisonExpression;
import com.facebook.presto.sql.tree.Query;
import com.facebook.presto.sql.tree.QueryBody;
import com.facebook.presto.sql.tree.QuerySpecification;
import com.facebook.presto.sql.tree.RefreshMaterializedView;
import com.facebook.presto.sql.tree.Relation;
import com.facebook.presto.sql.tree.RenameColumn;
import com.facebook.presto.sql.tree.RenameSchema;
import com.facebook.presto.sql.tree.RenameTable;
import com.facebook.presto.sql.tree.RenameView;
import com.facebook.presto.sql.tree.ResetSession;
import com.facebook.presto.sql.tree.Return;
import com.facebook.presto.sql.tree.Revoke;
import com.facebook.presto.sql.tree.RevokeRoles;
import com.facebook.presto.sql.tree.Rollback;
import com.facebook.presto.sql.tree.Rollup;
import com.facebook.presto.sql.tree.RoutineBody;
import com.facebook.presto.sql.tree.RoutineCharacteristics;
import com.facebook.presto.sql.tree.RoutineCharacteristics.Language;
import com.facebook.presto.sql.tree.Row;
import com.facebook.presto.sql.tree.SampledRelation;
import com.facebook.presto.sql.tree.SearchedCaseExpression;
import com.facebook.presto.sql.tree.Select;
import com.facebook.presto.sql.tree.SelectItem;
import com.facebook.presto.sql.tree.SetProperties;
import com.facebook.presto.sql.tree.SetRole;
import com.facebook.presto.sql.tree.SetSession;
import com.facebook.presto.sql.tree.ShowCatalogs;
import com.facebook.presto.sql.tree.ShowColumns;
import com.facebook.presto.sql.tree.ShowCreate;
import com.facebook.presto.sql.tree.ShowCreateFunction;
import com.facebook.presto.sql.tree.ShowFunctions;
import com.facebook.presto.sql.tree.ShowGrants;
import com.facebook.presto.sql.tree.ShowRoleGrants;
import com.facebook.presto.sql.tree.ShowRoles;
import com.facebook.presto.sql.tree.ShowSchemas;
import com.facebook.presto.sql.tree.ShowSession;
import com.facebook.presto.sql.tree.ShowStats;
import com.facebook.presto.sql.tree.ShowTables;
import com.facebook.presto.sql.tree.SimpleCaseExpression;
import com.facebook.presto.sql.tree.SimpleGroupBy;
import com.facebook.presto.sql.tree.SingleColumn;
import com.facebook.presto.sql.tree.SortItem;
import com.facebook.presto.sql.tree.SqlParameterDeclaration;
import com.facebook.presto.sql.tree.StartTransaction;
import com.facebook.presto.sql.tree.Statement;
import com.facebook.presto.sql.tree.StringLiteral;
import com.facebook.presto.sql.tree.SubqueryExpression;
import com.facebook.presto.sql.tree.SubscriptExpression;
import com.facebook.presto.sql.tree.Table;
import com.facebook.presto.sql.tree.TableElement;
import com.facebook.presto.sql.tree.TableSubquery;
import com.facebook.presto.sql.tree.TableVersionExpression;
import com.facebook.presto.sql.tree.TimeLiteral;
import com.facebook.presto.sql.tree.TimestampLiteral;
import com.facebook.presto.sql.tree.TransactionAccessMode;
import com.facebook.presto.sql.tree.TransactionMode;
import com.facebook.presto.sql.tree.TruncateTable;
import com.facebook.presto.sql.tree.TryExpression;
import com.facebook.presto.sql.tree.Union;
import com.facebook.presto.sql.tree.Unnest;
import com.facebook.presto.sql.tree.Update;
import com.facebook.presto.sql.tree.UpdateAssignment;
import com.facebook.presto.sql.tree.Use;
import com.facebook.presto.sql.tree.Values;
import com.facebook.presto.sql.tree.WhenClause;
import com.facebook.presto.sql.tree.Window;
import com.facebook.presto.sql.tree.WindowFrame;
import com.facebook.presto.sql.tree.With;
import com.facebook.presto.sql.tree.WithQuery;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.Token;
import org.antlr.v4.runtime.tree.ParseTree;
import org.antlr.v4.runtime.tree.TerminalNode;

import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static com.facebook.presto.sql.tree.ConstraintSpecification.ConstraintType;
import static com.facebook.presto.sql.tree.ConstraintSpecification.ConstraintType.PRIMARY_KEY;
import static com.facebook.presto.sql.tree.ConstraintSpecification.ConstraintType.UNIQUE;
import static com.facebook.presto.sql.tree.RoutineCharacteristics.Determinism;
import static com.facebook.presto.sql.tree.RoutineCharacteristics.Determinism.DETERMINISTIC;
import static com.facebook.presto.sql.tree.RoutineCharacteristics.Determinism.NOT_DETERMINISTIC;
import static com.facebook.presto.sql.tree.RoutineCharacteristics.NullCallClause;
import static com.facebook.presto.sql.tree.RoutineCharacteristics.NullCallClause.CALLED_ON_NULL_INPUT;
import static com.facebook.presto.sql.tree.RoutineCharacteristics.NullCallClause.RETURNS_NULL_ON_NULL_INPUT;
import static com.facebook.presto.sql.tree.SetProperties.Type.TABLE;
import static com.facebook.presto.sql.tree.TableVersionExpression.TableVersionOperator;
import static com.facebook.presto.sql.tree.TableVersionExpression.TableVersionOperator.EQUAL;
import static com.facebook.presto.sql.tree.TableVersionExpression.TableVersionOperator.LESS_THAN;
import static com.facebook.presto.sql.tree.TableVersionExpression.timestampExpression;
import static com.facebook.presto.sql.tree.TableVersionExpression.versionExpression;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.Iterables.getOnlyElement;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

class AstBuilder
        extends SqlBaseBaseVisitor<Node>
{
    private int parameterPosition;
    private final ParsingOptions parsingOptions;
    private final Consumer<ParsingWarning> warningConsumer;

    AstBuilder(ParsingOptions parsingOptions)
    {
        this.parsingOptions = requireNonNull(parsingOptions, "parsingOptions is null");
        this.warningConsumer = requireNonNull(parsingOptions.getWarningConsumer(), "warningConsumer is null");
    }

    @Override
    public Node visitSingleStatement(SqlBaseParser.SingleStatementContext context)
    {
        return visit(context.statement());
    }

    @Override
    public Node visitStandaloneExpression(SqlBaseParser.StandaloneExpressionContext context)
    {
        return visit(context.expression());
    }

    @Override
    public Node visitStandaloneRoutineBody(SqlBaseParser.StandaloneRoutineBodyContext context)
    {
        return visit(context.routineBody());
    }

    // ******************* statements **********************

    @Override
    public Node visitUse(SqlBaseParser.UseContext context)
    {
        return new Use(
                getLocation(context),
                visitIfPresent(context.catalog, Identifier.class),
                (Identifier) visit(context.schema));
    }

    @Override
    public Node visitCreateSchema(SqlBaseParser.CreateSchemaContext context)
    {
        List<Property> properties = ImmutableList.of();
        if (context.properties() != null) {
            properties = visit(context.properties().property(), Property.class);
        }

        return new CreateSchema(
                getLocation(context),
                getQualifiedName(context.qualifiedName()),
                context.EXISTS() != null,
                properties);
    }

    @Override
    public Node visitDropSchema(SqlBaseParser.DropSchemaContext context)
    {
        return new DropSchema(
                getLocation(context),
                getQualifiedName(context.qualifiedName()),
                context.EXISTS() != null,
                context.CASCADE() != null);
    }

    @Override
    public Node visitRenameSchema(SqlBaseParser.RenameSchemaContext context)
    {
        return new RenameSchema(
                getLocation(context),
                getQualifiedName(context.qualifiedName()),
                (Identifier) visit(context.identifier()));
    }

    @Override
    public Node visitCreateTableAsSelect(SqlBaseParser.CreateTableAsSelectContext context)
    {
        Optional<String> comment = Optional.empty();
        if (context.COMMENT() != null) {
            comment = Optional.of(((StringLiteral) visit(context.string())).getValue());
        }

        Optional<List<Identifier>> columnAliases = Optional.empty();
        if (context.columnAliases() != null) {
            columnAliases = Optional.of(visit(context.columnAliases().identifier(), Identifier.class));
        }

        List<Property> properties = ImmutableList.of();
        if (context.properties() != null) {
            properties = visit(context.properties().property(), Property.class);
        }

        return new CreateTableAsSelect(
                getLocation(context),
                getQualifiedName(context.qualifiedName()),
                (Query) visit(context.query()),
                context.EXISTS() != null,
                properties,
                context.NO() == null,
                columnAliases,
                comment);
    }

    @Override
    public Node visitCreateTable(SqlBaseParser.CreateTableContext context)
    {
        Optional<String> comment = Optional.empty();
        if (context.COMMENT() != null) {
            comment = Optional.of(((StringLiteral) visit(context.string())).getValue());
        }
        List<Property> properties = ImmutableList.of();
        if (context.properties() != null) {
            properties = visit(context.properties().property(), Property.class);
        }
        return new CreateTable(
                getLocation(context),
                getQualifiedName(context.qualifiedName()),
                visit(context.tableElement(), TableElement.class),
                context.EXISTS() != null,
                properties,
                comment);
    }

    @Override
    public Node visitCreateType(SqlBaseParser.CreateTypeContext context)
    {
        if (context.type() == null) {
            try {
                return new CreateType(
                        getQualifiedName(context.qualifiedName()),
                        context.sqlParameterDeclaration().stream().map(p -> ((Identifier) visit(p.identifier())).getValue()).collect(toImmutableList()),
                        context.sqlParameterDeclaration().stream().map(p -> getType(p.type())).collect(toImmutableList()));
            }
            catch (IllegalArgumentException e) {
                throw parseError(e.getMessage(), context);
            }
        }
        else {
            return new CreateType(
                    getQualifiedName(context.qualifiedName()),
                    getType(context.type()));
        }
    }

    @Override
    public Node visitShowCreateTable(SqlBaseParser.ShowCreateTableContext context)
    {
        return new ShowCreate(getLocation(context), ShowCreate.Type.TABLE, getQualifiedName(context.qualifiedName()));
    }

    @Override
    public Node visitShowCreateSchema(SqlBaseParser.ShowCreateSchemaContext context)
    {
        return new ShowCreate(getLocation(context), ShowCreate.Type.SCHEMA, getQualifiedName(context.qualifiedName()));
    }

    @Override
    public Node visitShowCreateFunction(SqlBaseParser.ShowCreateFunctionContext context)
    {
        Optional<List<String>> parameterTypes = context.types() == null ? Optional.empty() : Optional.of(getTypes(context.types()));
        return new ShowCreateFunction(getLocation(context), getQualifiedName(context.qualifiedName()), parameterTypes);
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
    public Node visitDropMaterializedView(SqlBaseParser.DropMaterializedViewContext context)
    {
        return new DropMaterializedView(Optional.of(getLocation(context)), getQualifiedName(context.qualifiedName()), context.EXISTS() != null);
    }

    @Override
    public Node visitRefreshMaterializedView(SqlBaseParser.RefreshMaterializedViewContext context)
    {
        return new RefreshMaterializedView(
                getLocation(context),
                new Table(getLocation(context), getQualifiedName(context.qualifiedName())),
                (Expression) visit(context.booleanExpression()));
    }

    @Override
    public Node visitInsertInto(SqlBaseParser.InsertIntoContext context)
    {
        Optional<List<Identifier>> columnAliases = Optional.empty();
        if (context.columnAliases() != null) {
            columnAliases = Optional.of(visit(context.columnAliases().identifier(), Identifier.class));
        }

        return new Insert(
                getQualifiedName(context.qualifiedName()),
                columnAliases,
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
    public Node visitUpdate(SqlBaseParser.UpdateContext context)
    {
        return new Update(
                getLocation(context),
                new Table(getLocation(context), getQualifiedName(context.qualifiedName())),
                visit(context.updateAssignment(), UpdateAssignment.class),
                visitIfPresent(context.booleanExpression(), Expression.class));
    }

    @Override
    public Node visitUpdateAssignment(SqlBaseParser.UpdateAssignmentContext context)
    {
        return new UpdateAssignment((Identifier) visit(context.identifier()), (Expression) visit(context.expression()));
    }

    @Override
    public Node visitTruncateTable(SqlBaseParser.TruncateTableContext context)
    {
        return new TruncateTable(getLocation(context), getQualifiedName(context.qualifiedName()));
    }

    @Override
    public Node visitMergeInto(SqlBaseParser.MergeIntoContext context)
    {
        Table table = new Table(getLocation(context), getQualifiedName(context.qualifiedName()));
        Relation targetRelation = table;
        if (context.identifier() != null) {
            targetRelation = new AliasedRelation(table, (Identifier) visit(context.identifier()), null);
        }
        return new Merge(
                getLocation(context),
                targetRelation,
                (Relation) visit(context.relation()),
                (Expression) visit(context.expression()),
                visit(context.mergeCase(), MergeCase.class));
    }

    @Override
    public Node visitMergeInsert(SqlBaseParser.MergeInsertContext context)
    {
        return new MergeInsert(
                getLocation(context),
                visitIdentifiers(context.columns),
                visit(context.values, Expression.class));
    }

    private List<Identifier> visitIdentifiers(List<SqlBaseParser.IdentifierContext> identifiers)
    {
        return identifiers.stream()
                .map(identifier -> (Identifier) visit(identifier))
                .collect(toImmutableList());
    }

    @Override
    public Node visitMergeUpdate(SqlBaseParser.MergeUpdateContext context)
    {
        ImmutableList.Builder<MergeUpdate.Assignment> assignments = ImmutableList.builder();
        for (int i = 0; i < context.targetColumns.size(); i++) {
            assignments.add(new MergeUpdate.Assignment(
                    (Identifier) visit(context.targetColumns.get(i)),
                    (Expression) visit(context.values.get(i))));
        }

        return new MergeUpdate(getLocation(context), assignments.build());
    }

    @Override
    public Node visitRenameTable(SqlBaseParser.RenameTableContext context)
    {
        return new RenameTable(getLocation(context), getQualifiedName(context.from), getQualifiedName(context.to), context.EXISTS() != null);
    }

    @Override
    public Node visitSetTableProperties(SqlBaseParser.SetTablePropertiesContext context)
    {
        List<Property> properties = ImmutableList.of();
        if (context.properties() != null) {
            properties = visit(context.properties().property(), Property.class);
        }

        return new SetProperties(getLocation(context),
                TABLE,
                getQualifiedName(context.tableName),
                properties,
                context.EXISTS() != null);
    }

    @Override
    public Node visitRenameColumn(SqlBaseParser.RenameColumnContext context)
    {
        return new RenameColumn(
                getLocation(context),
                getQualifiedName(context.tableName),
                (Identifier) visit(context.from),
                (Identifier) visit(context.to),
                context.EXISTS().stream().anyMatch(node -> node.getSymbol().getTokenIndex() < context.COLUMN().getSymbol().getTokenIndex()),
                context.EXISTS().stream().anyMatch(node -> node.getSymbol().getTokenIndex() > context.COLUMN().getSymbol().getTokenIndex()));
    }

    @Override
    public Node visitRenameView(SqlBaseParser.RenameViewContext context)
    {
        return new RenameView(getLocation(context), getQualifiedName(context.from), getQualifiedName(context.to), context.EXISTS() != null);
    }

    @Override
    public Node visitAnalyze(SqlBaseParser.AnalyzeContext context)
    {
        List<Property> properties = ImmutableList.of();
        if (context.properties() != null) {
            properties = visit(context.properties().property(), Property.class);
        }
        return new Analyze(
                getLocation(context),
                getQualifiedName(context.qualifiedName()),
                properties);
    }

    @Override
    public Node visitAddColumn(SqlBaseParser.AddColumnContext context)
    {
        return new AddColumn(getLocation(context),
                getQualifiedName(context.qualifiedName()),
                (ColumnDefinition) visit(context.columnDefinition()),
                context.EXISTS().stream().anyMatch(node -> node.getSymbol().getTokenIndex() < context.COLUMN().getSymbol().getTokenIndex()),
                context.EXISTS().stream().anyMatch(node -> node.getSymbol().getTokenIndex() > context.COLUMN().getSymbol().getTokenIndex()));
    }

    @Override
    public Node visitDropColumn(SqlBaseParser.DropColumnContext context)
    {
        return new DropColumn(getLocation(context),
                getQualifiedName(context.tableName),
                (Identifier) visit(context.column),
                context.EXISTS().stream().anyMatch(node -> node.getSymbol().getTokenIndex() < context.COLUMN().getSymbol().getTokenIndex()),
                context.EXISTS().stream().anyMatch(node -> node.getSymbol().getTokenIndex() > context.COLUMN().getSymbol().getTokenIndex()));
    }

    @Override
    public Node visitDropConstraint(SqlBaseParser.DropConstraintContext context)
    {
        return new DropConstraint(getLocation(context),
                getQualifiedName(context.tableName),
                visit(context.name).toString(),
                context.EXISTS().stream().anyMatch(node -> node.getSymbol().getTokenIndex() < context.CONSTRAINT().getSymbol().getTokenIndex()),
                context.EXISTS().stream().anyMatch(node -> node.getSymbol().getTokenIndex() > context.CONSTRAINT().getSymbol().getTokenIndex()));
    }

    @Override
    public Node visitAddConstraint(SqlBaseParser.AddConstraintContext context)
    {
        return new AddConstraint(getLocation(context),
                getQualifiedName(context.qualifiedName()),
                context.EXISTS() != null,
                (ConstraintSpecification) visit(context.constraintSpecification()));
    }

    @Override
    public Node visitConstraintSpecification(SqlBaseParser.ConstraintSpecificationContext context)
    {
        return context.namedConstraintSpecification() != null ?
                (ConstraintSpecification) visit(context.namedConstraintSpecification()) :
                (ConstraintSpecification) visit(context.unnamedConstraintSpecification());
    }

    @Override
    public Node visitNamedConstraintSpecification(SqlBaseParser.NamedConstraintSpecificationContext context)
    {
        ConstraintSpecification unnamedConstraint = (ConstraintSpecification) visit(context.unnamedConstraintSpecification());
        return new ConstraintSpecification(getLocation(context),
                Optional.of(visit(context.name).toString()),
                unnamedConstraint.getColumns(),
                unnamedConstraint.getConstraintType(),
                unnamedConstraint.isEnabled(),
                unnamedConstraint.isRely(),
                unnamedConstraint.isEnforced());
    }

    @Override
    public Node visitUnnamedConstraintSpecification(SqlBaseParser.UnnamedConstraintSpecificationContext context)
    {
        List<Identifier> columnAliases = visit(context.columnAliases().identifier(), Identifier.class);
        ConstraintType constraintType = getConstraintType((Token) context.constraintType().getChild(0).getPayload());
        String constraintTypeString = constraintTypeToString(constraintType);

        List<SqlBaseParser.ConstraintQualifierContext> constraintQualifierContext = context.constraintQualifiers().constraintQualifier();
        check(constraintQualifierContext.stream().filter(p -> p.constraintEnabled() != null).count() <= 1,
                "Invalid " + constraintTypeString + " constraint specification ENABLED",
                context.constraintQualifiers());
        check(constraintQualifierContext.stream().filter(p -> p.constraintRely() != null).count() <= 1,
                "Invalid " + constraintTypeString + " constraint specification RELY",
                context.constraintQualifiers());
        check(constraintQualifierContext.stream().filter(p -> p.constraintEnforced() != null).count() <= 1,
                "Invalid " + constraintTypeString + " constraint specification ENFORCED",
                context.constraintQualifiers());

        Optional<SqlBaseParser.ConstraintQualifierContext> enabledSpecification = constraintQualifierContext.stream()
                .filter(p -> p.constraintEnabled() != null)
                .findFirst();
        boolean enabled = !enabledSpecification.isPresent() ||
                enabledSpecification.get().constraintEnabled().DISABLED() == null;

        Optional<SqlBaseParser.ConstraintQualifierContext> relySpecification = constraintQualifierContext.stream()
                .filter(p -> p.constraintRely() != null)
                .findFirst();
        boolean rely = !relySpecification.isPresent() ||
                relySpecification.get().constraintRely().NOT() == null;

        Optional<SqlBaseParser.ConstraintQualifierContext> enforcedSpecification = constraintQualifierContext.stream()
                .filter(p -> p.constraintEnforced() != null)
                .findFirst();
        boolean enforced = !enforcedSpecification.isPresent() ||
                enforcedSpecification.get().constraintEnforced().NOT() == null;

        return new ConstraintSpecification(getLocation(context),
                Optional.empty(),
                columnAliases.stream().map(Identifier::toString).collect(toImmutableList()),
                constraintType,
                enabled,
                rely,
                enforced);
    }

    @Override
    public Node visitAlterColumnSetNotNull(SqlBaseParser.AlterColumnSetNotNullContext context)
    {
        return new AlterColumnNotNull(
                getLocation(context),
                getQualifiedName(context.tableName),
                (Identifier) visit(context.column),
                context.EXISTS() != null,
                false);
    }

    @Override
    public Node visitAlterColumnDropNotNull(SqlBaseParser.AlterColumnDropNotNullContext context)
    {
        return new AlterColumnNotNull(
                getLocation(context),
                getQualifiedName(context.tableName),
                (Identifier) visit(context.column),
                context.EXISTS() != null,
                true);
    }

    @Override
    public Node visitCreateView(SqlBaseParser.CreateViewContext context)
    {
        Optional<CreateView.Security> security = Optional.empty();
        if (context.DEFINER() != null) {
            security = Optional.of(CreateView.Security.DEFINER);
        }
        else if (context.INVOKER() != null) {
            security = Optional.of(CreateView.Security.INVOKER);
        }

        return new CreateView(
                getLocation(context),
                getQualifiedName(context.qualifiedName()),
                (Query) visit(context.query()),
                context.REPLACE() != null,
                security);
    }

    @Override
    public Node visitCreateMaterializedView(SqlBaseParser.CreateMaterializedViewContext context)
    {
        Optional<String> comment = Optional.empty();
        if (context.COMMENT() != null) {
            comment = Optional.of(((StringLiteral) visit(context.string())).getValue());
        }

        List<Property> properties = ImmutableList.of();
        if (context.properties() != null) {
            properties = visit(context.properties().property(), Property.class);
        }

        return new CreateMaterializedView(
                Optional.of(getLocation(context)),
                getQualifiedName(context.qualifiedName()),
                (Query) visit(context.query()),
                context.EXISTS() != null,
                properties,
                comment);
    }

    @Override
    public Node visitCreateFunction(SqlBaseParser.CreateFunctionContext context)
    {
        Optional<String> comment = Optional.empty();
        if (context.COMMENT() != null) {
            comment = Optional.of(((StringLiteral) visit(context.string())).getValue());
        }

        return new CreateFunction(
                getQualifiedName(context.functionName),
                context.REPLACE() != null,
                context.TEMPORARY() != null,
                context.sqlParameterDeclaration().stream()
                        .map(this::getParameterDeclarations)
                        .collect(toImmutableList()),
                getType(context.returnType),
                comment,
                getRoutineCharacteristics(context.routineCharacteristics()),
                (RoutineBody) visit(context.routineBody()));
    }

    @Override
    public Node visitAlterFunction(SqlBaseParser.AlterFunctionContext context)
    {
        Optional<List<String>> parameterTypes = context.types() == null ? Optional.empty() : Optional.of(getTypes(context.types()));
        return new AlterFunction(
                getLocation(context),
                getQualifiedName(context.qualifiedName()),
                parameterTypes,
                getAlterRoutineCharacteristics(context.alterRoutineCharacteristics()));
    }

    @Override
    public Node visitDropFunction(SqlBaseParser.DropFunctionContext context)
    {
        Optional<List<String>> parameterTypes = context.types() == null ? Optional.empty() : Optional.of(getTypes(context.types()));
        return new DropFunction(
                getLocation(context),
                getQualifiedName(context.qualifiedName()),
                parameterTypes,
                context.TEMPORARY() != null,
                context.EXISTS() != null);
    }

    @Override
    public Node visitReturnStatement(SqlBaseParser.ReturnStatementContext context)
    {
        return new Return((Expression) visit(context.expression()));
    }

    @Override
    public Node visitExternalBodyReference(SqlBaseParser.ExternalBodyReferenceContext context)
    {
        if (context.externalRoutineName() != null) {
            return new ExternalBodyReference((Identifier) visit(context.externalRoutineName().identifier()));
        }
        return new ExternalBodyReference();
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
        return new Prepare(
                getLocation(context),
                (Identifier) visit(context.identifier()),
                (Statement) visit(context.statement()));
    }

    @Override
    public Node visitDeallocate(SqlBaseParser.DeallocateContext context)
    {
        return new Deallocate(
                getLocation(context),
                (Identifier) visit(context.identifier()));
    }

    @Override
    public Node visitExecute(SqlBaseParser.ExecuteContext context)
    {
        return new Execute(
                getLocation(context),
                (Identifier) visit(context.identifier()),
                visit(context.expression(), Expression.class));
    }

    @Override
    public Node visitDescribeOutput(SqlBaseParser.DescribeOutputContext context)
    {
        return new DescribeOutput(
                getLocation(context),
                (Identifier) visit(context.identifier()));
    }

    @Override
    public Node visitDescribeInput(SqlBaseParser.DescribeInputContext context)
    {
        return new DescribeInput(
                getLocation(context),
                (Identifier) visit(context.identifier()));
    }

    @Override
    public Node visitProperty(SqlBaseParser.PropertyContext context)
    {
        return new Property(getLocation(context), (Identifier) visit(context.identifier()), (Expression) visit(context.expression()));
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
                body.getOffset(),
                body.getLimit());
    }

    @Override
    public Node visitWith(SqlBaseParser.WithContext context)
    {
        return new With(getLocation(context), context.RECURSIVE() != null, visit(context.namedQuery(), WithQuery.class));
    }

    @Override
    public Node visitNamedQuery(SqlBaseParser.NamedQueryContext context)
    {
        Optional<List<Identifier>> columns = Optional.empty();
        if (context.columnAliases() != null) {
            columns = Optional.of(visit(context.columnAliases().identifier(), Identifier.class));
        }

        return new WithQuery(
                getLocation(context),
                (Identifier) visit(context.name),
                (Query) visit(context.query()),
                columns);
    }

    @Override
    public Node visitQueryNoWith(SqlBaseParser.QueryNoWithContext context)
    {
        QueryBody term = (QueryBody) visit(context.queryTerm());

        Optional<OrderBy> orderBy = Optional.empty();
        if (context.ORDER() != null) {
            orderBy = Optional.of(new OrderBy(getLocation(context.ORDER()), visit(context.sortItem(), SortItem.class)));
        }

        Optional<Offset> offset = Optional.empty();
        if (context.OFFSET() != null) {
            offset = Optional.of(new Offset(Optional.of(getLocation(context.OFFSET())), getTextIfPresent(context.offset).orElseThrow(() -> new IllegalStateException("Missing OFFSET row count"))));
        }

        Optional<String> limit = Optional.empty();
        if (context.LIMIT() != null) {
            limit = getTextIfPresent(context.limit);
        }
        else if (context.FETCH() != null) {
            limit = getTextIfPresent(context.fetchFirstNRows);
        }

        if (term instanceof QuerySpecification) {
            // When we have a simple query specification
            // followed by order by, offset, limit, fold the order by and limit
            // clauses into the query specification (analyzer/planner
            // expects this structure to resolve references with respect
            // to columns defined in the query specification)
            QuerySpecification query = (QuerySpecification) term;

            return new Query(
                    getLocation(context),
                    Optional.empty(),
                    new QuerySpecification(
                            getLocation(context),
                            query.getSelect(),
                            query.getFrom(),
                            query.getWhere(),
                            query.getGroupBy(),
                            query.getHaving(),
                            orderBy,
                            offset,
                            limit),
                    Optional.empty(),
                    Optional.empty(),
                    Optional.empty());
        }

        return new Query(
                getLocation(context),
                Optional.empty(),
                term,
                orderBy,
                offset,
                limit);
    }

    @Override
    public Node visitQuerySpecification(SqlBaseParser.QuerySpecificationContext context)
    {
        Optional<Relation> from = Optional.empty();
        List<SelectItem> selectItems = visit(context.selectItem(), SelectItem.class);

        List<Relation> relations = visit(context.relation(), Relation.class);
        if (!relations.isEmpty()) {
            // synthesize implicit join nodes
            Iterator<Relation> iterator = relations.iterator();
            Relation relation = iterator.next();

            while (iterator.hasNext()) {
                relation = new Join(getLocation(context), Join.Type.IMPLICIT, relation, iterator.next(), Optional.empty());
            }

            from = Optional.of(relation);
        }

        return new QuerySpecification(
                getLocation(context),
                new Select(getLocation(context.SELECT()), isDistinct(context.setQuantifier()), selectItems),
                from,
                visitIfPresent(context.where, Expression.class),
                visitIfPresent(context.groupBy(), GroupBy.class),
                visitIfPresent(context.having, Expression.class),
                Optional.empty(),
                Optional.empty(),
                Optional.empty());
    }

    @Override
    public Node visitGroupBy(SqlBaseParser.GroupByContext context)
    {
        return new GroupBy(getLocation(context), isDistinct(context.setQuantifier()), visit(context.groupingElement(), GroupingElement.class));
    }

    @Override
    public Node visitSingleGroupingSet(SqlBaseParser.SingleGroupingSetContext context)
    {
        return new SimpleGroupBy(getLocation(context), visit(context.groupingSet().expression(), Expression.class));
    }

    @Override
    public Node visitRollup(SqlBaseParser.RollupContext context)
    {
        return new Rollup(getLocation(context), visit(context.expression(), Expression.class));
    }

    @Override
    public Node visitCube(SqlBaseParser.CubeContext context)
    {
        return new Cube(getLocation(context), visit(context.expression(), Expression.class));
    }

    @Override
    public Node visitMultipleGroupingSets(SqlBaseParser.MultipleGroupingSetsContext context)
    {
        return new GroupingSets(getLocation(context), context.groupingSet().stream()
                .map(groupingSet -> visit(groupingSet.expression(), Expression.class))
                .collect(toList()));
    }

    @Override
    public Node visitSetOperation(SqlBaseParser.SetOperationContext context)
    {
        QueryBody left = (QueryBody) visit(context.left);
        QueryBody right = (QueryBody) visit(context.right);

        Optional<Boolean> distinct = Optional.empty();
        if (context.setQuantifier() != null) {
            if (context.setQuantifier().DISTINCT() != null) {
                distinct = Optional.of(true);
            }
            else if (context.setQuantifier().ALL() != null) {
                distinct = Optional.of(false);
            }
        }

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
        return new SingleColumn(
                getLocation(context),
                (Expression) visit(context.expression()),
                visitIfPresent(context.identifier(), Identifier.class));
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
        return new Explain(getLocation(context), context.ANALYZE() != null, context.VERBOSE() != null, (Statement) visit(context.statement()), visit(context.explainOption(), ExplainOption.class));
    }

    @Override
    public Node visitExplainFormat(SqlBaseParser.ExplainFormatContext context)
    {
        switch (context.value.getType()) {
            case SqlBaseLexer.GRAPHVIZ:
                return new ExplainFormat(getLocation(context), ExplainFormat.Type.GRAPHVIZ);
            case SqlBaseLexer.TEXT:
                return new ExplainFormat(getLocation(context), ExplainFormat.Type.TEXT);
            case SqlBaseLexer.JSON:
                return new ExplainFormat(getLocation(context), ExplainFormat.Type.JSON);
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
            case SqlBaseLexer.VALIDATE:
                return new ExplainType(getLocation(context), ExplainType.Type.VALIDATE);
            case SqlBaseLexer.IO:
                return new ExplainType(getLocation(context), ExplainType.Type.IO);
        }

        throw new IllegalArgumentException("Unsupported EXPLAIN type: " + context.value.getText());
    }

    @Override
    public Node visitShowTables(SqlBaseParser.ShowTablesContext context)
    {
        return new ShowTables(
                getLocation(context),
                Optional.ofNullable(context.qualifiedName())
                        .map(this::getQualifiedName),
                getTextIfPresent(context.pattern)
                        .map(AstBuilder::unquote),
                getTextIfPresent(context.escape)
                        .map(AstBuilder::unquote));
    }

    @Override
    public Node visitShowSchemas(SqlBaseParser.ShowSchemasContext context)
    {
        return new ShowSchemas(
                getLocation(context),
                visitIfPresent(context.identifier(), Identifier.class),
                getTextIfPresent(context.pattern)
                        .map(AstBuilder::unquote),
                getTextIfPresent(context.escape)
                        .map(AstBuilder::unquote));
    }

    @Override
    public Node visitShowCatalogs(SqlBaseParser.ShowCatalogsContext context)
    {
        return new ShowCatalogs(getLocation(context),
                getTextIfPresent(context.pattern)
                        .map(AstBuilder::unquote),
                getTextIfPresent(context.escape)
                        .map(AstBuilder::unquote));
    }

    @Override
    public Node visitShowColumns(SqlBaseParser.ShowColumnsContext context)
    {
        return new ShowColumns(getLocation(context), getQualifiedName(context.qualifiedName()));
    }

    @Override
    public Node visitShowStats(SqlBaseParser.ShowStatsContext context)
    {
        return new ShowStats(Optional.of(getLocation(context)), new Table(getQualifiedName(context.qualifiedName())));
    }

    @Override
    public Node visitShowStatsForQuery(SqlBaseParser.ShowStatsForQueryContext context)
    {
        QuerySpecification specification = (QuerySpecification) visitQuerySpecification(context.querySpecification());
        Query query = new Query(Optional.empty(), specification, Optional.empty(), Optional.empty(), Optional.empty());
        return new ShowStats(Optional.of(getLocation(context)), new TableSubquery(query));
    }

    @Override
    public Node visitShowCreateView(SqlBaseParser.ShowCreateViewContext context)
    {
        return new ShowCreate(getLocation(context), ShowCreate.Type.VIEW, getQualifiedName(context.qualifiedName()));
    }

    @Override
    public Node visitShowCreateMaterializedView(SqlBaseParser.ShowCreateMaterializedViewContext context)
    {
        return new ShowCreate(getLocation(context), ShowCreate.Type.MATERIALIZED_VIEW, getQualifiedName(context.qualifiedName()));
    }

    @Override
    public Node visitShowFunctions(SqlBaseParser.ShowFunctionsContext context)
    {
        return new ShowFunctions(
                getLocation(context),
                getTextIfPresent(context.pattern)
                        .map(AstBuilder::unquote),
                getTextIfPresent(context.escape)
                        .map(AstBuilder::unquote));
    }

    @Override
    public Node visitShowSession(SqlBaseParser.ShowSessionContext context)
    {
        return new ShowSession(getLocation(context),
                getTextIfPresent(context.pattern)
                        .map(AstBuilder::unquote),
                getTextIfPresent(context.escape)
                        .map(AstBuilder::unquote));
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
    public Node visitCreateRole(SqlBaseParser.CreateRoleContext context)
    {
        return new CreateRole(
                getLocation(context),
                (Identifier) visit(context.name),
                getGrantorSpecificationIfPresent(context.grantor()));
    }

    @Override
    public Node visitDropRole(SqlBaseParser.DropRoleContext context)
    {
        return new DropRole(
                getLocation(context),
                (Identifier) visit(context.name));
    }

    @Override
    public Node visitGrantRoles(SqlBaseParser.GrantRolesContext context)
    {
        return new GrantRoles(
                getLocation(context),
                ImmutableSet.copyOf(getIdentifiers(context.roles().identifier())),
                ImmutableSet.copyOf(getPrincipalSpecifications(context.principal())),
                context.OPTION() != null,
                getGrantorSpecificationIfPresent(context.grantor()));
    }

    @Override
    public Node visitRevokeRoles(SqlBaseParser.RevokeRolesContext context)
    {
        return new RevokeRoles(
                getLocation(context),
                ImmutableSet.copyOf(getIdentifiers(context.roles().identifier())),
                ImmutableSet.copyOf(getPrincipalSpecifications(context.principal())),
                context.OPTION() != null,
                getGrantorSpecificationIfPresent(context.grantor()));
    }

    @Override
    public Node visitSetRole(SqlBaseParser.SetRoleContext context)
    {
        SetRole.Type type = SetRole.Type.ROLE;
        if (context.ALL() != null) {
            type = SetRole.Type.ALL;
        }
        else if (context.NONE() != null) {
            type = SetRole.Type.NONE;
        }
        return new SetRole(getLocation(context), type, getIdentifierIfPresent(context.role));
    }

    @Override
    public Node visitGrant(SqlBaseParser.GrantContext context)
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
        return new Grant(
                getLocation(context),
                privileges,
                context.TABLE() != null,
                getQualifiedName(context.qualifiedName()),
                getPrincipalSpecification(context.grantee),
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
                getPrincipalSpecification(context.grantee));
    }

    @Override
    public Node visitShowGrants(SqlBaseParser.ShowGrantsContext context)
    {
        Optional<QualifiedName> tableName = Optional.empty();

        if (context.qualifiedName() != null) {
            tableName = Optional.of(getQualifiedName(context.qualifiedName()));
        }

        return new ShowGrants(
                getLocation(context),
                context.TABLE() != null,
                tableName);
    }

    @Override
    public Node visitShowRoles(SqlBaseParser.ShowRolesContext context)
    {
        return new ShowRoles(
                getLocation(context),
                getIdentifierIfPresent(context.identifier()),
                context.CURRENT() != null);
    }

    @Override
    public Node visitShowRoleGrants(SqlBaseParser.ShowRoleGrantsContext context)
    {
        return new ShowRoleGrants(
                getLocation(context),
                getIdentifierIfPresent(context.identifier()));
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
        LogicalBinaryExpression.Operator operator = getLogicalBinaryOperator(context.operator);
        boolean warningForMixedAndOr = false;
        Expression left = (Expression) visit(context.left);
        Expression right = (Expression) visit(context.right);

        if (operator.equals(LogicalBinaryExpression.Operator.OR) &&
                (mixedAndOrOperatorParenthesisCheck(right, context.right, LogicalBinaryExpression.Operator.AND) ||
                 mixedAndOrOperatorParenthesisCheck(left, context.left, LogicalBinaryExpression.Operator.AND))) {
            warningForMixedAndOr = true;
        }

        if (operator.equals(LogicalBinaryExpression.Operator.AND) &&
                (mixedAndOrOperatorParenthesisCheck(right, context.right, LogicalBinaryExpression.Operator.OR) ||
                 mixedAndOrOperatorParenthesisCheck(left, context.left, LogicalBinaryExpression.Operator.OR))) {
            warningForMixedAndOr = true;
        }

        if (warningForMixedAndOr) {
            warningConsumer.accept(new ParsingWarning(
                    "The query contains OR and AND operators without proper parentheses. "
                    + "Make sure the operators are guarded by parentheses in order "
                    + "to fetch logically correct results.",
                    context.getStart().getLine(), context.getStart().getCharPositionInLine()));
        }

        return new LogicalBinaryExpression(
                getLocation(context.operator),
                operator,
                left,
                right);
    }

    // *************** from clause *****************

    @Override
    public Node visitJoinRelation(SqlBaseParser.JoinRelationContext context)
    {
        Relation left = (Relation) visit(context.left);
        Relation right;

        if (context.CROSS() != null) {
            right = (Relation) visit(context.right);
            return new Join(getLocation(context), Join.Type.CROSS, left, right, Optional.empty());
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
                criteria = new JoinUsing(visit(context.joinCriteria().identifier(), Identifier.class));
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

        return new SampledRelation(
                getLocation(context),
                child,
                getSamplingMethod((Token) context.sampleType().getChild(0).getPayload()),
                (Expression) visit(context.percentage));
    }

    @Override
    public Node visitAliasedRelation(SqlBaseParser.AliasedRelationContext context)
    {
        Relation child = (Relation) visit(context.relationPrimary());

        if (context.identifier() == null) {
            return child;
        }

        List<Identifier> aliases = null;
        if (context.columnAliases() != null) {
            aliases = visit(context.columnAliases().identifier(), Identifier.class);
        }

        return new AliasedRelation(getLocation(context), child, (Identifier) visit(context.identifier()), aliases);
    }

    @Override
    public Node visitTableName(SqlBaseParser.TableNameContext context)
    {
        if (context.tableVersionExpression() != null) {
            return new Table(getLocation(context), getQualifiedName(context.qualifiedName()), (TableVersionExpression) visit(context.tableVersionExpression()));
        }

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
    public Node visitLateral(SqlBaseParser.LateralContext context)
    {
        return new Lateral(getLocation(context), (Query) visit(context.query()));
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
                ComparisonExpression.Operator.IS_DISTINCT_FROM,
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
        Expression result = new LikePredicate(
                getLocation(context),
                (Expression) visit(context.value),
                (Expression) visit(context.pattern),
                visitIfPresent(context.escape, Expression.class));

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
        return new ExistsPredicate(getLocation(context), new SubqueryExpression(getLocation(context), (Query) visit(context.query())));
    }

    @Override
    public Node visitQuantifiedComparison(SqlBaseParser.QuantifiedComparisonContext context)
    {
        return new QuantifiedComparisonExpression(
                getLocation(context.comparisonOperator()),
                getComparisonOperator(((TerminalNode) context.comparisonOperator().getChild(0)).getSymbol()),
                getComparisonQuantifier(((TerminalNode) context.comparisonQuantifier().getChild(0)).getSymbol()),
                (Expression) visit(context.value),
                new SubqueryExpression(getLocation(context.query()), (Query) visit(context.query())));
    }

    // ************** value expressions **************

    @Override
    public Node visitTableVersion(SqlBaseParser.TableVersionContext context)
    {
        Expression child = (Expression) visit(context.valueExpression());

        switch (context.tableVersionType.getType()) {
            case SqlBaseLexer.SYSTEM_TIME:
            case SqlBaseLexer.TIMESTAMP:
                return timestampExpression(getLocation(context), getTableVersionOperator((Token) context.tableVersionState().getChild(0).getPayload()), child);
            case SqlBaseLexer.SYSTEM_VERSION:
            case SqlBaseLexer.VERSION:
                return versionExpression(getLocation(context), getTableVersionOperator((Token) context.tableVersionState().getChild(0).getPayload()), child);
            default:
                throw new UnsupportedOperationException("Unsupported Type: " + context.tableVersionType.getText());
        }
    }

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
        return visit(context.string());
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
        CurrentTime.Function function = getDateTimeFunctionType(context.name);

        if (context.precision != null) {
            return new CurrentTime(getLocation(context), function, Integer.parseInt(context.precision.getText()));
        }

        return new CurrentTime(getLocation(context), function);
    }

    @Override
    public Node visitCurrentUser(SqlBaseParser.CurrentUserContext context)
    {
        return new CurrentUser(getLocation(context.CURRENT_USER()));
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
            throw parseError("Invalid EXTRACT field: " + fieldString, context);
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
        return new DereferenceExpression(
                getLocation(context),
                (Expression) visit(context.base),
                (Identifier) visit(context.fieldName));
    }

    @Override
    public Node visitColumnReference(SqlBaseParser.ColumnReferenceContext context)
    {
        return visit(context.identifier());
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
        Optional<Expression> filter = visitIfPresent(context.filter(), Expression.class);
        Optional<Window> window = visitIfPresent(context.over(), Window.class);

        Optional<OrderBy> orderBy = Optional.empty();
        if (context.ORDER() != null) {
            orderBy = Optional.of(new OrderBy(visit(context.sortItem(), SortItem.class)));
        }

        QualifiedName name = getQualifiedName(context.qualifiedName());

        boolean distinct = isDistinct(context.setQuantifier());

        boolean ignoreNulls = context.nullTreatment() != null && context.nullTreatment().IGNORE() != null;

        if (name.toString().equalsIgnoreCase("if")) {
            check(context.expression().size() == 2 || context.expression().size() == 3, "Invalid number of arguments for 'if' function", context);
            check(!window.isPresent(), "OVER clause not valid for 'if' function", context);
            check(!distinct, "DISTINCT not valid for 'if' function", context);
            check(!filter.isPresent(), "FILTER not valid for 'if' function", context);

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
            check(!filter.isPresent(), "FILTER not valid for 'nullif' function", context);

            return new NullIfExpression(
                    getLocation(context),
                    (Expression) visit(context.expression(0)),
                    (Expression) visit(context.expression(1)));
        }

        if (name.toString().equalsIgnoreCase("coalesce")) {
            check(context.expression().size() >= 2, "The 'coalesce' function must have at least two arguments", context);
            check(!window.isPresent(), "OVER clause not valid for 'coalesce' function", context);
            check(!distinct, "DISTINCT not valid for 'coalesce' function", context);
            check(!filter.isPresent(), "FILTER not valid for 'coalesce' function", context);

            return new CoalesceExpression(getLocation(context), visit(context.expression(), Expression.class));
        }

        if (name.toString().equalsIgnoreCase("try")) {
            check(context.expression().size() == 1, "The 'try' function must have exactly one argument", context);
            check(!window.isPresent(), "OVER clause not valid for 'try' function", context);
            check(!distinct, "DISTINCT not valid for 'try' function", context);
            check(!filter.isPresent(), "FILTER not valid for 'try' function", context);

            return new TryExpression(getLocation(context), (Expression) visit(getOnlyElement(context.expression())));
        }

        if (name.toString().equalsIgnoreCase("$internal$bind")) {
            check(context.expression().size() >= 1, "The '$internal$bind' function must have at least one arguments", context);
            check(!window.isPresent(), "OVER clause not valid for '$internal$bind' function", context);
            check(!distinct, "DISTINCT not valid for '$internal$bind' function", context);
            check(!filter.isPresent(), "FILTER not valid for '$internal$bind' function", context);

            int numValues = context.expression().size() - 1;
            List<Expression> arguments = context.expression().stream()
                    .map(this::visit)
                    .map(Expression.class::cast)
                    .collect(toImmutableList());

            return new BindExpression(
                    getLocation(context),
                    arguments.subList(0, numValues),
                    arguments.get(numValues));
        }

        return new FunctionCall(
                getLocation(context),
                getQualifiedName(context.qualifiedName()),
                window,
                filter,
                orderBy,
                distinct,
                ignoreNulls,
                visit(context.expression(), Expression.class));
    }

    @Override
    public Node visitLambda(SqlBaseParser.LambdaContext context)
    {
        List<LambdaArgumentDeclaration> arguments = visit(context.identifier(), Identifier.class).stream()
                .map(LambdaArgumentDeclaration::new)
                .collect(toList());

        Expression body = (Expression) visit(context.expression());

        return new LambdaExpression(getLocation(context), arguments, body);
    }

    @Override
    public Node visitFilter(SqlBaseParser.FilterContext context)
    {
        return visit(context.booleanExpression());
    }

    @Override
    public Node visitOver(SqlBaseParser.OverContext context)
    {
        Optional<OrderBy> orderBy = Optional.empty();
        if (context.ORDER() != null) {
            orderBy = Optional.of(new OrderBy(getLocation(context.ORDER()), visit(context.sortItem(), SortItem.class)));
        }

        return new Window(
                getLocation(context),
                visit(context.partition, Expression.class),
                orderBy,
                visitIfPresent(context.windowFrame(), WindowFrame.class));
    }

    @Override
    public Node visitColumnDefinition(SqlBaseParser.ColumnDefinitionContext context)
    {
        Optional<String> comment = Optional.empty();
        if (context.COMMENT() != null) {
            comment = Optional.of(((StringLiteral) visit(context.string())).getValue());
        }

        List<Property> properties = ImmutableList.of();
        if (context.properties() != null) {
            properties = visit(context.properties().property(), Property.class);
        }

        boolean nullable = context.NOT() == null;

        return new ColumnDefinition(
                getLocation(context),
                (Identifier) visit(context.identifier()),
                getType(context.type()),
                nullable, properties,
                comment);
    }

    @Override
    public Node visitLikeClause(SqlBaseParser.LikeClauseContext context)
    {
        return new LikeClause(
                getLocation(context),
                getQualifiedName(context.qualifiedName()),
                Optional.ofNullable(context.optionType)
                        .map(AstBuilder::getPropertiesOption));
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

    @Override
    public Node visitGroupingOperation(SqlBaseParser.GroupingOperationContext context)
    {
        List<QualifiedName> arguments = context.qualifiedName().stream()
                .map(this::getQualifiedName)
                .collect(toList());

        return new GroupingOperation(Optional.of(getLocation(context)), arguments);
    }

    @Override
    public Node visitUnquotedIdentifier(SqlBaseParser.UnquotedIdentifierContext context)
    {
        return new Identifier(getLocation(context), context.getText(), false);
    }

    @Override
    public Node visitQuotedIdentifier(SqlBaseParser.QuotedIdentifierContext context)
    {
        String token = context.getText();
        String identifier = token.substring(1, token.length() - 1)
                .replace("\"\"", "\"");

        return new Identifier(getLocation(context), identifier, true);
    }

    // ************** literals **************

    @Override
    public Node visitNullLiteral(SqlBaseParser.NullLiteralContext context)
    {
        return new NullLiteral(getLocation(context));
    }

    @Override
    public Node visitBasicStringLiteral(SqlBaseParser.BasicStringLiteralContext context)
    {
        return new StringLiteral(getLocation(context), unquote(context.STRING().getText()));
    }

    @Override
    public Node visitUnicodeStringLiteral(SqlBaseParser.UnicodeStringLiteralContext context)
    {
        return new StringLiteral(getLocation(context), decodeUnicodeLiteral(context));
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
        String value = ((StringLiteral) visit(context.string())).getValue();

        if (context.DOUBLE_PRECISION() != null) {
            // TODO: Temporary hack that should be removed with new planner.
            return new GenericLiteral(getLocation(context), "DOUBLE", value);
        }

        String type = context.identifier().getText();
        if (type.equalsIgnoreCase("time")) {
            return new TimeLiteral(getLocation(context), value);
        }
        if (type.equalsIgnoreCase("timestamp")) {
            return new TimestampLiteral(getLocation(context), value);
        }
        if (type.equalsIgnoreCase("decimal")) {
            return new DecimalLiteral(getLocation(context), value);
        }
        if (type.equalsIgnoreCase("char")) {
            return new CharLiteral(getLocation(context), value);
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
        switch (parsingOptions.getDecimalLiteralTreatment()) {
            case AS_DOUBLE:
                return new DoubleLiteral(getLocation(context), context.getText());
            case AS_DECIMAL:
                return new DecimalLiteral(getLocation(context), context.getText());
            case REJECT:
                throw new ParsingException("Unexpected decimal literal: " + context.getText());
        }
        throw new AssertionError("Unreachable");
    }

    @Override
    public Node visitDoubleLiteral(SqlBaseParser.DoubleLiteralContext context)
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
                ((StringLiteral) visit(context.string())).getValue(),
                Optional.ofNullable(context.sign)
                        .map(AstBuilder::getIntervalSign)
                        .orElse(IntervalLiteral.Sign.POSITIVE),
                getIntervalFieldType((Token) context.from.getChild(0).getPayload()),
                Optional.ofNullable(context.to)
                        .map((x) -> x.getChild(0).getPayload())
                        .map(Token.class::cast)
                        .map(AstBuilder::getIntervalFieldType));
    }

    @Override
    public Node visitParameter(SqlBaseParser.ParameterContext context)
    {
        Parameter parameter = new Parameter(getLocation(context), parameterPosition);
        parameterPosition++;
        return parameter;
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

    private enum UnicodeDecodeState
    {
        EMPTY,
        ESCAPED,
        UNICODE_SEQUENCE
    }

    private static String decodeUnicodeLiteral(SqlBaseParser.UnicodeStringLiteralContext context)
    {
        char escape;
        if (context.UESCAPE() != null) {
            String escapeString = unquote(context.STRING().getText());
            check(!escapeString.isEmpty(), "Empty Unicode escape character", context);
            check(escapeString.length() == 1, "Invalid Unicode escape character: " + escapeString, context);
            escape = escapeString.charAt(0);
            check(isValidUnicodeEscape(escape), "Invalid Unicode escape character: " + escapeString, context);
        }
        else {
            escape = '\\';
        }

        String rawContent = unquote(context.UNICODE_STRING().getText().substring(2));
        StringBuilder unicodeStringBuilder = new StringBuilder();
        StringBuilder escapedCharacterBuilder = new StringBuilder();
        int charactersNeeded = 0;
        UnicodeDecodeState state = UnicodeDecodeState.EMPTY;
        for (int i = 0; i < rawContent.length(); i++) {
            char ch = rawContent.charAt(i);
            switch (state) {
                case EMPTY:
                    if (ch == escape) {
                        state = UnicodeDecodeState.ESCAPED;
                    }
                    else {
                        unicodeStringBuilder.append(ch);
                    }
                    break;
                case ESCAPED:
                    if (ch == escape) {
                        unicodeStringBuilder.append(escape);
                        state = UnicodeDecodeState.EMPTY;
                    }
                    else if (ch == '+') {
                        state = UnicodeDecodeState.UNICODE_SEQUENCE;
                        charactersNeeded = 6;
                    }
                    else if (isHexDigit(ch)) {
                        state = UnicodeDecodeState.UNICODE_SEQUENCE;
                        charactersNeeded = 4;
                        escapedCharacterBuilder.append(ch);
                    }
                    else {
                        throw parseError("Invalid hexadecimal digit: " + ch, context);
                    }
                    break;
                case UNICODE_SEQUENCE:
                    check(isHexDigit(ch), "Incomplete escape sequence: " + escapedCharacterBuilder.toString(), context);
                    escapedCharacterBuilder.append(ch);
                    if (charactersNeeded == escapedCharacterBuilder.length()) {
                        String currentEscapedCode = escapedCharacterBuilder.toString();
                        escapedCharacterBuilder.setLength(0);
                        int codePoint = Integer.parseInt(currentEscapedCode, 16);
                        check(Character.isValidCodePoint(codePoint), "Invalid escaped character: " + currentEscapedCode, context);
                        if (Character.isSupplementaryCodePoint(codePoint)) {
                            unicodeStringBuilder.appendCodePoint(codePoint);
                        }
                        else {
                            char currentCodePoint = (char) codePoint;
                            check(!Character.isSurrogate(currentCodePoint), format("Invalid escaped character: %s. Escaped character is a surrogate. Use '\\+123456' instead.", currentEscapedCode), context);
                            unicodeStringBuilder.append(currentCodePoint);
                        }
                        state = UnicodeDecodeState.EMPTY;
                        charactersNeeded = -1;
                    }
                    else {
                        check(charactersNeeded > escapedCharacterBuilder.length(), "Unexpected escape sequence length: " + escapedCharacterBuilder.length(), context);
                    }
                    break;
                default:
                    throw new UnsupportedOperationException();
            }
        }

        check(state == UnicodeDecodeState.EMPTY, "Incomplete escape sequence: " + escapedCharacterBuilder.toString(), context);
        return unicodeStringBuilder.toString();
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

    private static LikeClause.PropertiesOption getPropertiesOption(Token token)
    {
        switch (token.getType()) {
            case SqlBaseLexer.INCLUDING:
                return LikeClause.PropertiesOption.INCLUDING;
            case SqlBaseLexer.EXCLUDING:
                return LikeClause.PropertiesOption.EXCLUDING;
        }
        throw new IllegalArgumentException("Unsupported LIKE option type: " + token.getText());
    }

    private QualifiedName getQualifiedName(SqlBaseParser.QualifiedNameContext context)
    {
        List<String> parts = visit(context.identifier(), Identifier.class).stream()
                .map(Identifier::getValue) // TODO: preserve quotedness
                .collect(Collectors.toList());

        return QualifiedName.of(parts);
    }

    private static boolean isDistinct(SqlBaseParser.SetQuantifierContext setQuantifier)
    {
        return setQuantifier != null && setQuantifier.DISTINCT() != null;
    }

    private static boolean isHexDigit(char c)
    {
        return ((c >= '0') && (c <= '9')) ||
                ((c >= 'A') && (c <= 'F')) ||
                ((c >= 'a') && (c <= 'f'));
    }

    private static boolean isValidUnicodeEscape(char c)
    {
        return c < 0x7F && c > 0x20 && !isHexDigit(c) && c != '"' && c != '+' && c != '\'';
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

    public static String constraintTypeToString(ConstraintType constraintType)
    {
        switch (constraintType) {
            case UNIQUE:
                return "UNIQUE";
            case PRIMARY_KEY:
                return "PRIMARY KEY";
            default:
                return "";
        }
    }

    private static ConstraintType getConstraintType(Token token)
    {
        switch (token.getType()) {
            case SqlBaseLexer.UNIQUE:
                return UNIQUE;
            case SqlBaseLexer.PRIMARY:
                return PRIMARY_KEY;
        }

        throw new IllegalArgumentException("Unsupported constraint type: " + token.getText());
    }

    private static TableVersionOperator getTableVersionOperator(Token token)
    {
        switch (token.getType()) {
            case SqlBaseLexer.AS:
                return EQUAL;
            case SqlBaseLexer.BEFORE:
                return LESS_THAN;
        }

        throw new IllegalArgumentException("Unsupported table version operator: " + token.getText());
    }

    private static ArithmeticBinaryExpression.Operator getArithmeticBinaryOperator(Token operator)
    {
        switch (operator.getType()) {
            case SqlBaseLexer.PLUS:
                return ArithmeticBinaryExpression.Operator.ADD;
            case SqlBaseLexer.MINUS:
                return ArithmeticBinaryExpression.Operator.SUBTRACT;
            case SqlBaseLexer.ASTERISK:
                return ArithmeticBinaryExpression.Operator.MULTIPLY;
            case SqlBaseLexer.SLASH:
                return ArithmeticBinaryExpression.Operator.DIVIDE;
            case SqlBaseLexer.PERCENT:
                return ArithmeticBinaryExpression.Operator.MODULUS;
        }

        throw new UnsupportedOperationException("Unsupported operator: " + operator.getText());
    }

    private Optional<Identifier> getIdentifierIfPresent(ParserRuleContext context)
    {
        return Optional.ofNullable(context).map(c -> (Identifier) visit(c));
    }

    private static ComparisonExpression.Operator getComparisonOperator(Token symbol)
    {
        switch (symbol.getType()) {
            case SqlBaseLexer.EQ:
                return ComparisonExpression.Operator.EQUAL;
            case SqlBaseLexer.NEQ:
                return ComparisonExpression.Operator.NOT_EQUAL;
            case SqlBaseLexer.LT:
                return ComparisonExpression.Operator.LESS_THAN;
            case SqlBaseLexer.LTE:
                return ComparisonExpression.Operator.LESS_THAN_OR_EQUAL;
            case SqlBaseLexer.GT:
                return ComparisonExpression.Operator.GREATER_THAN;
            case SqlBaseLexer.GTE:
                return ComparisonExpression.Operator.GREATER_THAN_OR_EQUAL;
        }

        throw new IllegalArgumentException("Unsupported operator: " + symbol.getText());
    }

    private static CurrentTime.Function getDateTimeFunctionType(Token token)
    {
        switch (token.getType()) {
            case SqlBaseLexer.CURRENT_DATE:
                return CurrentTime.Function.DATE;
            case SqlBaseLexer.CURRENT_TIME:
                return CurrentTime.Function.TIME;
            case SqlBaseLexer.CURRENT_TIMESTAMP:
                return CurrentTime.Function.TIMESTAMP;
            case SqlBaseLexer.LOCALTIME:
                return CurrentTime.Function.LOCALTIME;
            case SqlBaseLexer.LOCALTIMESTAMP:
                return CurrentTime.Function.LOCALTIMESTAMP;
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
            case SqlBaseLexer.GROUPS:
                return WindowFrame.Type.GROUPS;
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
        }

        throw new IllegalArgumentException("Unsupported sampling method: " + token.getText());
    }

    private static LogicalBinaryExpression.Operator getLogicalBinaryOperator(Token token)
    {
        switch (token.getType()) {
            case SqlBaseLexer.AND:
                return LogicalBinaryExpression.Operator.AND;
            case SqlBaseLexer.OR:
                return LogicalBinaryExpression.Operator.OR;
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

    private static QuantifiedComparisonExpression.Quantifier getComparisonQuantifier(Token symbol)
    {
        switch (symbol.getType()) {
            case SqlBaseLexer.ALL:
                return QuantifiedComparisonExpression.Quantifier.ALL;
            case SqlBaseLexer.ANY:
                return QuantifiedComparisonExpression.Quantifier.ANY;
            case SqlBaseLexer.SOME:
                return QuantifiedComparisonExpression.Quantifier.SOME;
        }

        throw new IllegalArgumentException("Unsupported quantifier: " + symbol.getText());
    }

    private List<String> getTypes(SqlBaseParser.TypesContext types)
    {
        return types.type().stream()
                .map(this::getType)
                .collect(toImmutableList());
    }

    private String getType(SqlBaseParser.TypeContext type)
    {
        if (type.baseType() != null) {
            String signature = type.baseType().getText();
            if (type.baseType().DOUBLE_PRECISION() != null) {
                // TODO: Temporary hack that should be removed with new planner.
                signature = "DOUBLE";
            }
            if (!type.typeParameter().isEmpty()) {
                String typeParameterSignature = type
                        .typeParameter()
                        .stream()
                        .map(this::typeParameterToString)
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
                builder.append(visit(type.identifier(i)))
                        .append(" ")
                        .append(getType(type.type(i)));
            }
            builder.append(")");
            return "ROW" + builder.toString();
        }

        if (type.INTERVAL() != null) {
            return "INTERVAL " + getIntervalFieldType((Token) type.from.getChild(0).getPayload()) +
                    " TO " + getIntervalFieldType((Token) type.to.getChild(0).getPayload());
        }

        throw new IllegalArgumentException("Unsupported type specification: " + type.getText());
    }

    private SqlParameterDeclaration getParameterDeclarations(SqlBaseParser.SqlParameterDeclarationContext context)
    {
        return new SqlParameterDeclaration((Identifier) visit(context.identifier()), getType(context.type()));
    }

    private RoutineCharacteristics getRoutineCharacteristics(SqlBaseParser.RoutineCharacteristicsContext context)
    {
        Language language = null;
        Determinism determinism = null;
        NullCallClause nullCallClause = null;

        for (SqlBaseParser.RoutineCharacteristicContext characteristic : context.routineCharacteristic()) {
            if (characteristic.language() != null) {
                if (language != null) {
                    throw new ParsingException(format("Duplicate language clause: %s", characteristic.language().getText()), getLocation(characteristic.language()));
                }
                if (characteristic.language().SQL() != null) {
                    language = Language.SQL;
                }
                else {
                    language = new Language(((Identifier) visit(characteristic.language().identifier())).getValue());
                }
            }
            else if (characteristic.determinism() != null) {
                if (determinism != null) {
                    throw new ParsingException(format("Duplicate determinism characteristics: %s", characteristic.determinism().getText()), getLocation(characteristic.determinism()));
                }
                determinism = characteristic.determinism().NOT() == null ? DETERMINISTIC : NOT_DETERMINISTIC;
            }
            else if (characteristic.nullCallClause() != null) {
                if (nullCallClause != null) {
                    throw new ParsingException(format("Duplicate null-call clause: %s", characteristic.nullCallClause().getText()), getLocation(characteristic.nullCallClause()));
                }
                nullCallClause = characteristic.nullCallClause().CALLED() != null ? CALLED_ON_NULL_INPUT : RETURNS_NULL_ON_NULL_INPUT;
            }
            else {
                throw new IllegalArgumentException(format("Unsupported RoutineCharacteristic: %s", characteristic.getText()));
            }
        }

        return new RoutineCharacteristics(
                Optional.ofNullable(language),
                Optional.ofNullable(determinism),
                Optional.ofNullable(nullCallClause));
    }

    private AlterRoutineCharacteristics getAlterRoutineCharacteristics(SqlBaseParser.AlterRoutineCharacteristicsContext context)
    {
        if (context.alterRoutineCharacteristic().isEmpty()) {
            throw new ParsingException("No alter routine characteristics specified");
        }

        NullCallClause nullCallClause = null;

        for (SqlBaseParser.AlterRoutineCharacteristicContext characteristic : context.alterRoutineCharacteristic()) {
            if (characteristic.nullCallClause() != null) {
                if (nullCallClause != null) {
                    throw new ParsingException(format("Duplicate null-call clause: %s", characteristic.nullCallClause().getText()), getLocation(characteristic.nullCallClause()));
                }
                nullCallClause = characteristic.nullCallClause().CALLED() != null ? CALLED_ON_NULL_INPUT : RETURNS_NULL_ON_NULL_INPUT;
            }
        }

        return new AlterRoutineCharacteristics(Optional.ofNullable(nullCallClause));
    }

    private String typeParameterToString(SqlBaseParser.TypeParameterContext typeParameter)
    {
        if (typeParameter.INTEGER_VALUE() != null) {
            return typeParameter.INTEGER_VALUE().toString();
        }
        if (typeParameter.type() != null) {
            return getType(typeParameter.type());
        }
        throw new IllegalArgumentException("Unsupported typeParameter: " + typeParameter.getText());
    }

    private List<Identifier> getIdentifiers(List<SqlBaseParser.IdentifierContext> identifiers)
    {
        return identifiers.stream().map(context -> (Identifier) visit(context)).collect(toList());
    }

    private List<PrincipalSpecification> getPrincipalSpecifications(List<SqlBaseParser.PrincipalContext> principals)
    {
        return principals.stream().map(this::getPrincipalSpecification).collect(toList());
    }

    private Optional<GrantorSpecification> getGrantorSpecificationIfPresent(SqlBaseParser.GrantorContext context)
    {
        return Optional.ofNullable(context).map(this::getGrantorSpecification);
    }

    private GrantorSpecification getGrantorSpecification(SqlBaseParser.GrantorContext context)
    {
        if (context instanceof SqlBaseParser.SpecifiedPrincipalContext) {
            return new GrantorSpecification(GrantorSpecification.Type.PRINCIPAL, Optional.of(getPrincipalSpecification(((SqlBaseParser.SpecifiedPrincipalContext) context).principal())));
        }
        else if (context instanceof SqlBaseParser.CurrentUserGrantorContext) {
            return new GrantorSpecification(GrantorSpecification.Type.CURRENT_USER, Optional.empty());
        }
        else if (context instanceof SqlBaseParser.CurrentRoleGrantorContext) {
            return new GrantorSpecification(GrantorSpecification.Type.CURRENT_ROLE, Optional.empty());
        }
        else {
            throw new IllegalArgumentException("Unsupported grantor: " + context);
        }
    }

    private PrincipalSpecification getPrincipalSpecification(SqlBaseParser.PrincipalContext context)
    {
        if (context instanceof SqlBaseParser.UnspecifiedPrincipalContext) {
            return new PrincipalSpecification(PrincipalSpecification.Type.UNSPECIFIED, (Identifier) visit(((SqlBaseParser.UnspecifiedPrincipalContext) context).identifier()));
        }
        else if (context instanceof SqlBaseParser.UserPrincipalContext) {
            return new PrincipalSpecification(PrincipalSpecification.Type.USER, (Identifier) visit(((SqlBaseParser.UserPrincipalContext) context).identifier()));
        }
        else if (context instanceof SqlBaseParser.RolePrincipalContext) {
            return new PrincipalSpecification(PrincipalSpecification.Type.ROLE, (Identifier) visit(((SqlBaseParser.RolePrincipalContext) context).identifier()));
        }
        else {
            throw new IllegalArgumentException("Unsupported principal: " + context);
        }
    }

    private boolean mixedAndOrOperatorParenthesisCheck(Expression expression, SqlBaseParser.BooleanExpressionContext node, LogicalBinaryExpression.Operator operator)
    {
        if (expression instanceof LogicalBinaryExpression) {
            if (((LogicalBinaryExpression) expression).getOperator().equals(operator)) {
                if (node.children.get(0) instanceof SqlBaseParser.ValueExpressionDefaultContext) {
                    return !(((SqlBaseParser.PredicatedContext) node).valueExpression().getChild(0) instanceof
                        SqlBaseParser.ParenthesizedExpressionContext);
                }
                else {
                    return true;
                }
            }
        }
        return false;
    }

    private static void check(boolean condition, String message, ParserRuleContext context)
    {
        if (!condition) {
            throw parseError(message, context);
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

    private static ParsingException parseError(String message, ParserRuleContext context)
    {
        return new ParsingException(message, null, context.getStart().getLine(), context.getStart().getCharPositionInLine());
    }
}
