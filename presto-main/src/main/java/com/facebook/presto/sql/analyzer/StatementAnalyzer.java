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
package com.facebook.presto.sql.analyzer;

import com.facebook.presto.Session;
import com.facebook.presto.SystemSessionProperties;
import com.facebook.presto.common.CatalogSchemaName;
import com.facebook.presto.common.QualifiedObjectName;
import com.facebook.presto.common.function.OperatorType;
import com.facebook.presto.common.type.ArrayType;
import com.facebook.presto.common.type.DoubleType;
import com.facebook.presto.common.type.MapType;
import com.facebook.presto.common.type.RealType;
import com.facebook.presto.common.type.RowType;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.metadata.OperatorNotFoundException;
import com.facebook.presto.metadata.TableMetadata;
import com.facebook.presto.metadata.ViewDefinition;
import com.facebook.presto.security.AccessControl;
import com.facebook.presto.security.AllowAllAccessControl;
import com.facebook.presto.security.ViewAccessControl;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.ConnectorId;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.PrestoWarning;
import com.facebook.presto.spi.TableHandle;
import com.facebook.presto.spi.WarningCollector;
import com.facebook.presto.spi.function.FunctionKind;
import com.facebook.presto.spi.function.Signature;
import com.facebook.presto.spi.function.SqlFunction;
import com.facebook.presto.spi.security.AccessDeniedException;
import com.facebook.presto.spi.security.Identity;
import com.facebook.presto.sql.parser.ParsingException;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.planner.ExpressionInterpreter;
import com.facebook.presto.sql.planner.TypeProvider;
import com.facebook.presto.sql.planner.VariablesExtractor;
import com.facebook.presto.sql.tree.AddColumn;
import com.facebook.presto.sql.tree.AliasedRelation;
import com.facebook.presto.sql.tree.AllColumns;
import com.facebook.presto.sql.tree.AlterFunction;
import com.facebook.presto.sql.tree.Analyze;
import com.facebook.presto.sql.tree.Call;
import com.facebook.presto.sql.tree.Commit;
import com.facebook.presto.sql.tree.CreateFunction;
import com.facebook.presto.sql.tree.CreateSchema;
import com.facebook.presto.sql.tree.CreateTable;
import com.facebook.presto.sql.tree.CreateTableAsSelect;
import com.facebook.presto.sql.tree.CreateView;
import com.facebook.presto.sql.tree.Cube;
import com.facebook.presto.sql.tree.Deallocate;
import com.facebook.presto.sql.tree.DefaultTraversalVisitor;
import com.facebook.presto.sql.tree.Delete;
import com.facebook.presto.sql.tree.DereferenceExpression;
import com.facebook.presto.sql.tree.DropColumn;
import com.facebook.presto.sql.tree.DropFunction;
import com.facebook.presto.sql.tree.DropSchema;
import com.facebook.presto.sql.tree.DropTable;
import com.facebook.presto.sql.tree.DropView;
import com.facebook.presto.sql.tree.Except;
import com.facebook.presto.sql.tree.Execute;
import com.facebook.presto.sql.tree.Explain;
import com.facebook.presto.sql.tree.ExplainType;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.ExpressionRewriter;
import com.facebook.presto.sql.tree.ExpressionTreeRewriter;
import com.facebook.presto.sql.tree.FieldReference;
import com.facebook.presto.sql.tree.FrameBound;
import com.facebook.presto.sql.tree.FunctionCall;
import com.facebook.presto.sql.tree.Grant;
import com.facebook.presto.sql.tree.GroupBy;
import com.facebook.presto.sql.tree.GroupingElement;
import com.facebook.presto.sql.tree.GroupingOperation;
import com.facebook.presto.sql.tree.GroupingSets;
import com.facebook.presto.sql.tree.Identifier;
import com.facebook.presto.sql.tree.Insert;
import com.facebook.presto.sql.tree.Intersect;
import com.facebook.presto.sql.tree.Join;
import com.facebook.presto.sql.tree.JoinCriteria;
import com.facebook.presto.sql.tree.JoinOn;
import com.facebook.presto.sql.tree.JoinUsing;
import com.facebook.presto.sql.tree.Lateral;
import com.facebook.presto.sql.tree.Literal;
import com.facebook.presto.sql.tree.LongLiteral;
import com.facebook.presto.sql.tree.NaturalJoin;
import com.facebook.presto.sql.tree.Node;
import com.facebook.presto.sql.tree.NodeRef;
import com.facebook.presto.sql.tree.OrderBy;
import com.facebook.presto.sql.tree.Prepare;
import com.facebook.presto.sql.tree.Property;
import com.facebook.presto.sql.tree.QualifiedName;
import com.facebook.presto.sql.tree.Query;
import com.facebook.presto.sql.tree.QueryBody;
import com.facebook.presto.sql.tree.QuerySpecification;
import com.facebook.presto.sql.tree.Relation;
import com.facebook.presto.sql.tree.RenameColumn;
import com.facebook.presto.sql.tree.RenameSchema;
import com.facebook.presto.sql.tree.RenameTable;
import com.facebook.presto.sql.tree.ResetSession;
import com.facebook.presto.sql.tree.Return;
import com.facebook.presto.sql.tree.Revoke;
import com.facebook.presto.sql.tree.Rollback;
import com.facebook.presto.sql.tree.Rollup;
import com.facebook.presto.sql.tree.Row;
import com.facebook.presto.sql.tree.SampledRelation;
import com.facebook.presto.sql.tree.Select;
import com.facebook.presto.sql.tree.SelectItem;
import com.facebook.presto.sql.tree.SetOperation;
import com.facebook.presto.sql.tree.SetSession;
import com.facebook.presto.sql.tree.SimpleGroupBy;
import com.facebook.presto.sql.tree.SingleColumn;
import com.facebook.presto.sql.tree.SortItem;
import com.facebook.presto.sql.tree.SqlParameterDeclaration;
import com.facebook.presto.sql.tree.StartTransaction;
import com.facebook.presto.sql.tree.Statement;
import com.facebook.presto.sql.tree.Table;
import com.facebook.presto.sql.tree.TableSubquery;
import com.facebook.presto.sql.tree.Union;
import com.facebook.presto.sql.tree.Unnest;
import com.facebook.presto.sql.tree.Use;
import com.facebook.presto.sql.tree.Values;
import com.facebook.presto.sql.tree.Window;
import com.facebook.presto.sql.tree.WindowFrame;
import com.facebook.presto.sql.tree.With;
import com.facebook.presto.sql.tree.WithQuery;
import com.facebook.presto.sql.util.AstUtils;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Multimap;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import static com.facebook.presto.SystemSessionProperties.getMaxGroupingSets;
import static com.facebook.presto.SystemSessionProperties.isAllowWindowOrderByLiterals;
import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.BooleanType.BOOLEAN;
import static com.facebook.presto.common.type.TypeSignature.parseTypeSignature;
import static com.facebook.presto.common.type.UnknownType.UNKNOWN;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static com.facebook.presto.metadata.MetadataUtil.createQualifiedObjectName;
import static com.facebook.presto.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static com.facebook.presto.spi.StandardErrorCode.NOT_FOUND;
import static com.facebook.presto.spi.StandardWarningCode.PERFORMANCE_WARNING;
import static com.facebook.presto.spi.StandardWarningCode.REDUNDANT_ORDER_BY;
import static com.facebook.presto.spi.function.FunctionKind.AGGREGATE;
import static com.facebook.presto.spi.function.FunctionKind.WINDOW;
import static com.facebook.presto.sql.NodeUtils.getSortItemsFromOrderBy;
import static com.facebook.presto.sql.NodeUtils.mapFromProperties;
import static com.facebook.presto.sql.ParsingUtil.createParsingOptions;
import static com.facebook.presto.sql.analyzer.AggregationAnalyzer.verifyOrderByAggregations;
import static com.facebook.presto.sql.analyzer.AggregationAnalyzer.verifySourceAggregations;
import static com.facebook.presto.sql.analyzer.Analyzer.verifyNoAggregateWindowOrGroupingFunctions;
import static com.facebook.presto.sql.analyzer.Analyzer.verifyNoExternalFunctions;
import static com.facebook.presto.sql.analyzer.ExpressionAnalyzer.createConstantAnalyzer;
import static com.facebook.presto.sql.analyzer.ExpressionAnalyzer.getExpressionTypes;
import static com.facebook.presto.sql.analyzer.ExpressionTreeUtils.extractAggregateFunctions;
import static com.facebook.presto.sql.analyzer.ExpressionTreeUtils.extractExpressions;
import static com.facebook.presto.sql.analyzer.ExpressionTreeUtils.extractWindowFunctions;
import static com.facebook.presto.sql.analyzer.ScopeReferenceExtractor.hasReferencesToScope;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.AMBIGUOUS_ATTRIBUTE;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.COLUMN_NAME_NOT_SPECIFIED;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.COLUMN_TYPE_UNKNOWN;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.DUPLICATE_COLUMN_NAME;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.DUPLICATE_PARAMETER_NAME;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.DUPLICATE_PROPERTY;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.DUPLICATE_RELATION;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.INVALID_FUNCTION_NAME;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.INVALID_ORDINAL;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.INVALID_PROCEDURE_ARGUMENTS;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.INVALID_WINDOW_FRAME;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.MISMATCHED_COLUMN_ALIASES;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.MISMATCHED_SET_COLUMN_TYPES;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.MISSING_ATTRIBUTE;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.MISSING_CATALOG;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.MISSING_COLUMN;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.MISSING_SCHEMA;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.MISSING_TABLE;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.MUST_BE_WINDOW_FUNCTION;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.NESTED_WINDOW;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.NONDETERMINISTIC_ORDER_BY_EXPRESSION_WITH_SELECT_DISTINCT;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.NON_NUMERIC_SAMPLE_PERCENTAGE;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.NOT_SUPPORTED;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.ORDER_BY_MUST_BE_IN_SELECT;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.TABLE_ALREADY_EXISTS;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.TOO_MANY_GROUPING_SETS;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.TYPE_MISMATCH;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.VIEW_ANALYSIS_ERROR;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.VIEW_IS_RECURSIVE;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.VIEW_IS_STALE;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.VIEW_PARSE_ERROR;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.WILDCARD_WITHOUT_FROM;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.WINDOW_FUNCTION_ORDERBY_LITERAL;
import static com.facebook.presto.sql.analyzer.TypeSignatureProvider.fromTypes;
import static com.facebook.presto.sql.planner.ExpressionDeterminismEvaluator.isDeterministic;
import static com.facebook.presto.sql.planner.ExpressionInterpreter.expressionOptimizer;
import static com.facebook.presto.sql.tree.ExplainType.Type.DISTRIBUTED;
import static com.facebook.presto.sql.tree.FrameBound.Type.CURRENT_ROW;
import static com.facebook.presto.sql.tree.FrameBound.Type.FOLLOWING;
import static com.facebook.presto.sql.tree.FrameBound.Type.PRECEDING;
import static com.facebook.presto.sql.tree.FrameBound.Type.UNBOUNDED_FOLLOWING;
import static com.facebook.presto.sql.tree.FrameBound.Type.UNBOUNDED_PRECEDING;
import static com.facebook.presto.sql.tree.WindowFrame.Type.RANGE;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Throwables.throwIfInstanceOf;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static com.google.common.collect.Iterables.getLast;
import static java.lang.Math.toIntExact;
import static java.lang.String.format;
import static java.util.Collections.emptyList;
import static java.util.Locale.ENGLISH;
import static java.util.Map.Entry;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.counting;
import static java.util.stream.Collectors.groupingBy;

class StatementAnalyzer
{
    private static final int UNION_DISTINCT_FIELDS_WARNING_THRESHOLD = 3;
    private final Analysis analysis;
    private final Metadata metadata;
    private final Session session;
    private final SqlParser sqlParser;
    private final AccessControl accessControl;
    private final WarningCollector warningCollector;

    public StatementAnalyzer(
            Analysis analysis,
            Metadata metadata,
            SqlParser sqlParser,
            AccessControl accessControl,
            Session session,
            WarningCollector warningCollector)
    {
        this.analysis = requireNonNull(analysis, "analysis is null");
        this.metadata = requireNonNull(metadata, "metadata is null");
        this.sqlParser = requireNonNull(sqlParser, "sqlParser is null");
        this.accessControl = requireNonNull(accessControl, "accessControl is null");
        this.session = requireNonNull(session, "session is null");
        this.warningCollector = requireNonNull(warningCollector, "warningCollector is null");
    }

    public Scope analyze(Node node, Scope outerQueryScope)
    {
        return analyze(node, Optional.of(outerQueryScope));
    }

    public Scope analyze(Node node, Optional<Scope> outerQueryScope)
    {
        return new Visitor(outerQueryScope, warningCollector).process(node, Optional.empty());
    }

    /**
     * Visitor context represents local query scope (if exists). The invariant is
     * that the local query scopes hierarchy should always have outer query scope
     * (if provided) as ancestor.
     */
    private class Visitor
            extends DefaultTraversalVisitor<Scope, Optional<Scope>>
    {
        private final Optional<Scope> outerQueryScope;
        private final WarningCollector warningCollector;

        private Visitor(Optional<Scope> outerQueryScope, WarningCollector warningCollector)
        {
            this.outerQueryScope = requireNonNull(outerQueryScope, "outerQueryScope is null");
            this.warningCollector = requireNonNull(warningCollector, "warningCollector is null");
        }

        public Scope process(Node node, Optional<Scope> scope)
        {
            Scope returnScope = super.process(node, scope);
            checkState(returnScope.getOuterQueryParent().equals(outerQueryScope), "result scope should have outer query scope equal with parameter outer query scope");
            if (scope.isPresent()) {
                checkState(hasScopeAsLocalParent(returnScope, scope.get()), "return scope should have context scope as one of ancestors");
            }
            return returnScope;
        }

        private Scope process(Node node, Scope scope)
        {
            return process(node, Optional.of(scope));
        }

        @Override
        protected Scope visitUse(Use node, Optional<Scope> scope)
        {
            throw new SemanticException(NOT_SUPPORTED, node, "USE statement is not supported");
        }

        @Override
        protected Scope visitInsert(Insert insert, Optional<Scope> scope)
        {
            QualifiedObjectName targetTable = createQualifiedObjectName(session, insert, insert.getTarget());
            if (metadata.getView(session, targetTable).isPresent()) {
                throw new SemanticException(NOT_SUPPORTED, insert, "Inserting into views is not supported");
            }

            // analyze the query that creates the data
            Scope queryScope = process(insert.getQuery(), scope);

            analysis.setUpdateType("INSERT");

            // verify the insert destination columns match the query
            Optional<TableHandle> targetTableHandle = metadata.getTableHandle(session, targetTable);
            if (!targetTableHandle.isPresent()) {
                throw new SemanticException(MISSING_TABLE, insert, "Table '%s' does not exist", targetTable);
            }
            accessControl.checkCanInsertIntoTable(session.getRequiredTransactionId(), session.getIdentity(), session.getAccessControlContext(), targetTable);

            TableMetadata tableMetadata = metadata.getTableMetadata(session, targetTableHandle.get());
            List<String> tableColumns = tableMetadata.getColumns().stream()
                    .filter(column -> !column.isHidden())
                    .map(ColumnMetadata::getName)
                    .collect(toImmutableList());

            List<String> insertColumns;
            if (insert.getColumns().isPresent()) {
                insertColumns = insert.getColumns().get().stream()
                        .map(Identifier::getValue)
                        .map(column -> column.toLowerCase(ENGLISH))
                        .collect(toImmutableList());

                Set<String> columnNames = new HashSet<>();
                for (String insertColumn : insertColumns) {
                    if (!tableColumns.contains(insertColumn)) {
                        throw new SemanticException(MISSING_COLUMN, insert, "Insert column name does not exist in target table: %s", insertColumn);
                    }
                    if (!columnNames.add(insertColumn)) {
                        throw new SemanticException(DUPLICATE_COLUMN_NAME, insert, "Insert column name is specified more than once: %s", insertColumn);
                    }
                }
            }
            else {
                insertColumns = tableColumns;
            }

            List<ColumnMetadata> expectedColumns = insertColumns.stream()
                    .map(insertColumn -> tableMetadata.getColumn(insertColumn))
                    .collect(toImmutableList());

            checkTypesMatchForInsert(insert, queryScope, expectedColumns);

            Map<String, ColumnHandle> columnHandles = metadata.getColumnHandles(session, targetTableHandle.get());
            analysis.setInsert(new Analysis.Insert(
                    targetTableHandle.get(),
                    insertColumns.stream().map(columnHandles::get).collect(toImmutableList())));

            return createAndAssignScope(insert, scope, Field.newUnqualified("rows", BIGINT));
        }

        private void checkTypesMatchForInsert(Insert insert, Scope queryScope, List<ColumnMetadata> expectedColumns)
        {
            List<Type> queryColumnTypes = queryScope.getRelationType().getVisibleFields().stream()
                    .map(Field::getType)
                    .collect(toImmutableList());

            String errorMessage = "";
            if (expectedColumns.size() != queryColumnTypes.size()) {
                errorMessage = format("Insert query has %d expression(s) but expected %d target column(s). ",
                        queryColumnTypes.size(), expectedColumns.size());
            }

            for (int i = 0; i < Math.max(expectedColumns.size(), queryColumnTypes.size()); i++) {
                Node node = insert;
                QueryBody queryBody = insert.getQuery().getQueryBody();
                if (queryBody instanceof Values) {
                    List<Expression> rows = ((Values) queryBody).getRows();
                    checkState(!rows.isEmpty(), "Missing column values");
                    node = rows.get(0);
                    if (node instanceof Row) {
                        int columnIndex = Math.min(i, queryColumnTypes.size() - 1);
                        node = ((Row) rows.get(0)).getItems().get(columnIndex);
                    }
                }
                if (i == expectedColumns.size()) {
                    throw new SemanticException(MISMATCHED_SET_COLUMN_TYPES,
                            node,
                            errorMessage + "Mismatch at column %d",
                            i + 1);
                }
                if (i == queryColumnTypes.size()) {
                    throw new SemanticException(MISMATCHED_SET_COLUMN_TYPES,
                            node,
                            errorMessage + "Mismatch at column %d: '%s'",
                            i + 1,
                            expectedColumns.get(i).getName());
                }
                if (!metadata.getFunctionAndTypeManager().canCoerce(
                        queryColumnTypes.get(i),
                        expectedColumns.get(i).getType())) {
                    throw new SemanticException(MISMATCHED_SET_COLUMN_TYPES,
                            node,
                            errorMessage + "Mismatch at column %d: '%s' is of type %s but expression is of type %s",
                            i + 1,
                            expectedColumns.get(i).getName(),
                            expectedColumns.get(i).getType(),
                            queryColumnTypes.get(i));
                }
            }
        }

        @Override
        protected Scope visitDelete(Delete node, Optional<Scope> scope)
        {
            Table table = node.getTable();
            QualifiedObjectName tableName = createQualifiedObjectName(session, table, table.getName());
            if (metadata.getView(session, tableName).isPresent()) {
                throw new SemanticException(NOT_SUPPORTED, node, "Deleting from views is not supported");
            }

            // Analyzer checks for select permissions but DELETE has a separate permission, so disable access checks
            // TODO: we shouldn't need to create a new analyzer. The access control should be carried in the context object
            StatementAnalyzer analyzer = new StatementAnalyzer(
                    analysis,
                    metadata,
                    sqlParser,
                    new AllowAllAccessControl(),
                    session,
                    warningCollector);

            Scope tableScope = analyzer.analyze(table, scope);
            node.getWhere().ifPresent(where -> analyzeWhere(node, tableScope, where));

            analysis.setUpdateType("DELETE");

            accessControl.checkCanDeleteFromTable(session.getRequiredTransactionId(), session.getIdentity(), session.getAccessControlContext(), tableName);

            return createAndAssignScope(node, scope, Field.newUnqualified("rows", BIGINT));
        }

        @Override
        protected Scope visitAnalyze(Analyze node, Optional<Scope> scope)
        {
            analysis.setUpdateType("ANALYZE");
            QualifiedObjectName tableName = createQualifiedObjectName(session, node, node.getTableName());

            // verify the target table exists and it's not a view
            if (metadata.getView(session, tableName).isPresent()) {
                throw new SemanticException(NOT_SUPPORTED, node, "Analyzing views is not supported");
            }

            validateProperties(node.getProperties(), scope);
            ConnectorId connectorId = metadata.getCatalogHandle(session, tableName.getCatalogName())
                    .orElseThrow(() -> new PrestoException(NOT_FOUND, "Catalog not found: " + tableName.getCatalogName()));

            Map<String, Object> analyzeProperties = metadata.getAnalyzePropertyManager().getProperties(
                    connectorId,
                    connectorId.getCatalogName(),
                    mapFromProperties(node.getProperties()),
                    session,
                    metadata,
                    analysis.getParameters());
            TableHandle tableHandle = metadata.getTableHandleForStatisticsCollection(session, tableName, analyzeProperties)
                    .orElseThrow(() -> (new SemanticException(MISSING_TABLE, node, "Table '%s' does not exist", tableName)));

            // user must have read and insert permission in order to analyze stats of a table
            analysis.addTableColumnReferences(
                    accessControl,
                    session.getIdentity(),
                    ImmutableMultimap.<QualifiedObjectName, String>builder()
                            .putAll(tableName, metadata.getColumnHandles(session, tableHandle).keySet())
                            .build());
            try {
                accessControl.checkCanInsertIntoTable(session.getRequiredTransactionId(), session.getIdentity(), session.getAccessControlContext(), tableName);
            }
            catch (AccessDeniedException exception) {
                throw new AccessDeniedException(format("Cannot ANALYZE (missing insert privilege) table %s", tableName));
            }

            analysis.setAnalyzeTarget(tableHandle);
            return createAndAssignScope(node, scope, Field.newUnqualified("rows", BIGINT));
        }

        @Override
        protected Scope visitCreateTableAsSelect(CreateTableAsSelect node, Optional<Scope> scope)
        {
            analysis.setUpdateType("CREATE TABLE");

            // turn this into a query that has a new table writer node on top.
            QualifiedObjectName targetTable = createQualifiedObjectName(session, node, node.getName());
            analysis.setCreateTableDestination(targetTable);

            Optional<TableHandle> targetTableHandle = metadata.getTableHandle(session, targetTable);
            if (targetTableHandle.isPresent()) {
                if (node.isNotExists()) {
                    analysis.setCreateTableAsSelectNoOp(true);
                    return createAndAssignScope(node, scope, Field.newUnqualified("rows", BIGINT));
                }
                throw new SemanticException(TABLE_ALREADY_EXISTS, node, "Destination table '%s' already exists", targetTable);
            }

            validateProperties(node.getProperties(), scope);
            analysis.setCreateTableProperties(mapFromProperties(node.getProperties()));

            node.getColumnAliases().ifPresent(analysis::setCreateTableColumnAliases);
            analysis.setCreateTableComment(node.getComment());

            accessControl.checkCanCreateTable(session.getRequiredTransactionId(), session.getIdentity(), session.getAccessControlContext(), targetTable);

            analysis.setCreateTableAsSelectWithData(node.isWithData());

            // analyze the query that creates the table
            Scope queryScope = process(node.getQuery(), scope);

            if (node.getColumnAliases().isPresent()) {
                validateColumnAliases(node.getColumnAliases().get(), queryScope.getRelationType().getVisibleFieldCount());

                // analzie only column types in subquery if column alias exists
                for (Field field : queryScope.getRelationType().getVisibleFields()) {
                    if (field.getType().equals(UNKNOWN)) {
                        throw new SemanticException(COLUMN_TYPE_UNKNOWN, node, "Column type is unknown at position %s", queryScope.getRelationType().indexOf(field) + 1);
                    }
                }
            }
            else {
                validateColumns(node, queryScope.getRelationType());
            }

            return createAndAssignScope(node, scope, Field.newUnqualified("rows", BIGINT));
        }

        @Override
        protected Scope visitCreateView(CreateView node, Optional<Scope> scope)
        {
            analysis.setUpdateType("CREATE VIEW");

            QualifiedObjectName viewName = createQualifiedObjectName(session, node, node.getName());

            // analyze the query that creates the view
            StatementAnalyzer analyzer = new StatementAnalyzer(analysis, metadata, sqlParser, accessControl, session, warningCollector);

            Scope queryScope = analyzer.analyze(node.getQuery(), scope);

            accessControl.checkCanCreateView(session.getRequiredTransactionId(), session.getIdentity(), session.getAccessControlContext(), viewName);

            validateColumns(node, queryScope.getRelationType());

            return createAndAssignScope(node, scope);
        }

        @Override
        protected Scope visitCreateFunction(CreateFunction node, Optional<Scope> scope)
        {
            analysis.setUpdateType("CREATE FUNCTION");

            // Check function name
            checkFunctionName(node, node.getFunctionName(), node.isTemporary());

            // Check no replace with temporary functions
            if (node.isTemporary() && node.isReplace()) {
                throw new SemanticException(NOT_SUPPORTED, node, "REPLACE is not supported for temporary functions");
            }

            // Check parameter
            List<String> duplicateParameters = node.getParameters().stream()
                    .map(SqlParameterDeclaration::getName)
                    .map(Identifier::getValue)
                    .collect(groupingBy(Function.identity(), counting()))
                    .entrySet()
                    .stream()
                    .filter(entry -> entry.getValue() > 1)
                    .map(Entry::getKey)
                    .collect(toImmutableList());
            if (!duplicateParameters.isEmpty()) {
                throw new SemanticException(DUPLICATE_PARAMETER_NAME, node, "Duplicate function parameter name: %s", Joiner.on(", ").join(duplicateParameters));
            }

            // Check return type
            Type returnType = metadata.getType(parseTypeSignature(node.getReturnType()));
            List<Field> fields = node.getParameters().stream()
                    .map(parameter -> Field.newUnqualified(parameter.getName().getValue(), metadata.getType(parseTypeSignature(parameter.getType()))))
                    .collect(toImmutableList());
            Scope functionScope = Scope.builder()
                    .withRelationType(RelationId.anonymous(), new RelationType(fields))
                    .build();
            if (node.getBody() instanceof Return) {
                Expression returnExpression = ((Return) node.getBody()).getExpression();
                Type bodyType = analyzeExpression(returnExpression, functionScope).getExpressionTypes().get(NodeRef.of(returnExpression));
                if (!metadata.getFunctionAndTypeManager().canCoerce(bodyType, returnType)) {
                    throw new SemanticException(TYPE_MISMATCH, node, "Function implementation type '%s' does not match declared return type '%s'", bodyType, returnType);
                }

                verifyNoAggregateWindowOrGroupingFunctions(analysis.getFunctionHandles(), metadata.getFunctionAndTypeManager(), returnExpression, "CREATE FUNCTION body");
                verifyNoExternalFunctions(analysis.getFunctionHandles(), metadata.getFunctionAndTypeManager(), returnExpression, "CREATE FUNCTION body");

                // TODO: Check body contains no SQL invoked functions
            }

            return createAndAssignScope(node, scope);
        }

        @Override
        protected Scope visitAlterFunction(AlterFunction node, Optional<Scope> scope)
        {
            checkFunctionName(node, node.getFunctionName(), false);
            return createAndAssignScope(node, scope);
        }

        @Override
        protected Scope visitDropFunction(DropFunction node, Optional<Scope> scope)
        {
            checkFunctionName(node, node.getFunctionName(), node.isTemporary());
            return createAndAssignScope(node, scope);
        }

        @Override
        protected Scope visitSetSession(SetSession node, Optional<Scope> scope)
        {
            return createAndAssignScope(node, scope);
        }

        @Override
        protected Scope visitResetSession(ResetSession node, Optional<Scope> scope)
        {
            return createAndAssignScope(node, scope);
        }

        @Override
        protected Scope visitAddColumn(AddColumn node, Optional<Scope> scope)
        {
            return createAndAssignScope(node, scope);
        }

        @Override
        protected Scope visitCreateSchema(CreateSchema node, Optional<Scope> scope)
        {
            validateProperties(node.getProperties(), scope);
            return createAndAssignScope(node, scope);
        }

        @Override
        protected Scope visitDropSchema(DropSchema node, Optional<Scope> scope)
        {
            return createAndAssignScope(node, scope);
        }

        @Override
        protected Scope visitRenameSchema(RenameSchema node, Optional<Scope> scope)
        {
            return createAndAssignScope(node, scope);
        }

        @Override
        protected Scope visitCreateTable(CreateTable node, Optional<Scope> scope)
        {
            validateProperties(node.getProperties(), scope);
            return createAndAssignScope(node, scope);
        }

        @Override
        protected Scope visitProperty(Property node, Optional<Scope> scope)
        {
            // Property value expressions must be constant
            createConstantAnalyzer(metadata, session, analysis.getParameters(), warningCollector, analysis.isDescribe())
                    .analyze(node.getValue(), createScope(scope));
            return createAndAssignScope(node, scope);
        }

        @Override
        protected Scope visitDropTable(DropTable node, Optional<Scope> scope)
        {
            return createAndAssignScope(node, scope);
        }

        @Override
        protected Scope visitRenameTable(RenameTable node, Optional<Scope> scope)
        {
            return createAndAssignScope(node, scope);
        }

        @Override
        protected Scope visitRenameColumn(RenameColumn node, Optional<Scope> scope)
        {
            return createAndAssignScope(node, scope);
        }

        @Override
        protected Scope visitDropColumn(DropColumn node, Optional<Scope> scope)
        {
            return createAndAssignScope(node, scope);
        }

        @Override
        protected Scope visitDropView(DropView node, Optional<Scope> scope)
        {
            return createAndAssignScope(node, scope);
        }

        @Override
        protected Scope visitStartTransaction(StartTransaction node, Optional<Scope> scope)
        {
            return createAndAssignScope(node, scope);
        }

        @Override
        protected Scope visitCommit(Commit node, Optional<Scope> scope)
        {
            return createAndAssignScope(node, scope);
        }

        @Override
        protected Scope visitRollback(Rollback node, Optional<Scope> scope)
        {
            return createAndAssignScope(node, scope);
        }

        @Override
        protected Scope visitPrepare(Prepare node, Optional<Scope> scope)
        {
            return createAndAssignScope(node, scope);
        }

        @Override
        protected Scope visitDeallocate(Deallocate node, Optional<Scope> scope)
        {
            return createAndAssignScope(node, scope);
        }

        @Override
        protected Scope visitExecute(Execute node, Optional<Scope> scope)
        {
            return createAndAssignScope(node, scope);
        }

        @Override
        protected Scope visitGrant(Grant node, Optional<Scope> scope)
        {
            return createAndAssignScope(node, scope);
        }

        @Override
        protected Scope visitRevoke(Revoke node, Optional<Scope> scope)
        {
            return createAndAssignScope(node, scope);
        }

        @Override
        protected Scope visitCall(Call node, Optional<Scope> scope)
        {
            return createAndAssignScope(node, scope);
        }

        private void validateProperties(List<Property> properties, Optional<Scope> scope)
        {
            Set<String> propertyNames = new HashSet<>();
            for (Property property : properties) {
                if (!propertyNames.add(property.getName().getValue())) {
                    throw new SemanticException(DUPLICATE_PROPERTY, property, "Duplicate property: %s", property.getName().getValue());
                }
            }
            for (Property property : properties) {
                process(property, scope);
            }
        }

        private void validateColumns(Statement node, RelationType descriptor)
        {
            // verify that all column names are specified and unique
            // TODO: collect errors and return them all at once
            Set<String> names = new HashSet<>();
            for (Field field : descriptor.getVisibleFields()) {
                Optional<String> fieldName = field.getName();
                if (!fieldName.isPresent()) {
                    throw new SemanticException(COLUMN_NAME_NOT_SPECIFIED, node, "Column name not specified at position %s", descriptor.indexOf(field) + 1);
                }
                if (!names.add(fieldName.get())) {
                    throw new SemanticException(DUPLICATE_COLUMN_NAME, node, "Column name '%s' specified more than once", fieldName.get());
                }
                if (field.getType().equals(UNKNOWN)) {
                    throw new SemanticException(COLUMN_TYPE_UNKNOWN, node, "Column type is unknown: %s", fieldName.get());
                }
            }
        }

        private void validateColumnAliases(List<Identifier> columnAliases, int sourceColumnSize)
        {
            if (columnAliases.size() != sourceColumnSize) {
                throw new SemanticException(
                        MISMATCHED_COLUMN_ALIASES,
                        columnAliases.get(0),
                        "Column alias list has %s entries but subquery has %s columns",
                        columnAliases.size(),
                        sourceColumnSize);
            }
            Set<String> names = new HashSet<>();
            for (Identifier identifier : columnAliases) {
                if (names.contains(identifier.getValue().toLowerCase(ENGLISH))) {
                    throw new SemanticException(DUPLICATE_COLUMN_NAME, identifier, "Column name '%s' specified more than once", identifier.getValue());
                }
                names.add(identifier.getValue().toLowerCase(ENGLISH));
            }
        }

        @Override
        protected Scope visitExplain(Explain node, Optional<Scope> scope)
                throws SemanticException
        {
            checkState(node.isAnalyze(), "Non analyze explain should be rewritten to Query");
            if (node.getOptions().stream().anyMatch(option -> !option.equals(new ExplainType(DISTRIBUTED)))) {
                throw new SemanticException(NOT_SUPPORTED, node, "EXPLAIN ANALYZE only supports TYPE DISTRIBUTED option");
            }
            process(node.getStatement(), scope);
            analysis.setUpdateType(null);
            return createAndAssignScope(node, scope, Field.newUnqualified("Query Plan", VARCHAR));
        }

        @Override
        protected Scope visitQuery(Query node, Optional<Scope> scope)
        {
            Scope withScope = analyzeWith(node, scope);
            Scope queryBodyScope = process(node.getQueryBody(), withScope);
            List<Expression> orderByExpressions = emptyList();
            if (node.getOrderBy().isPresent()) {
                orderByExpressions = analyzeOrderBy(node, getSortItemsFromOrderBy(node.getOrderBy()), queryBodyScope);
                if (queryBodyScope.getOuterQueryParent().isPresent() && !node.getLimit().isPresent()) {
                    // not the root scope and ORDER BY is ineffective
                    analysis.markRedundantOrderBy(node.getOrderBy().get());
                    warningCollector.add(new PrestoWarning(REDUNDANT_ORDER_BY, "ORDER BY in subquery may have no effect"));
                }
            }
            analysis.setOrderByExpressions(node, orderByExpressions);

            // Input fields == Output fields
            analysis.setOutputExpressions(node, descriptorToFields(queryBodyScope));

            Scope queryScope = Scope.builder()
                    .withParent(withScope)
                    .withRelationType(RelationId.of(node), queryBodyScope.getRelationType())
                    .build();

            analysis.setScope(node, queryScope);
            return queryScope;
        }

        @Override
        protected Scope visitUnnest(Unnest node, Optional<Scope> scope)
        {
            ImmutableList.Builder<Field> outputFields = ImmutableList.builder();
            for (Expression expression : node.getExpressions()) {
                ExpressionAnalysis expressionAnalysis = analyzeExpression(expression, createScope(scope));
                Type expressionType = expressionAnalysis.getType(expression);
                if (expressionType instanceof ArrayType) {
                    Type elementType = ((ArrayType) expressionType).getElementType();
                    if (!SystemSessionProperties.isLegacyUnnest(session) && elementType instanceof RowType) {
                        ((RowType) elementType).getFields().stream()
                                .map(field -> Field.newUnqualified(field.getName(), field.getType()))
                                .forEach(outputFields::add);
                    }
                    else {
                        outputFields.add(Field.newUnqualified(Optional.empty(), elementType));
                    }
                }
                else if (expressionType instanceof MapType) {
                    outputFields.add(Field.newUnqualified(Optional.empty(), ((MapType) expressionType).getKeyType()));
                    outputFields.add(Field.newUnqualified(Optional.empty(), ((MapType) expressionType).getValueType()));
                }
                else {
                    throw new PrestoException(INVALID_FUNCTION_ARGUMENT, "Cannot unnest type: " + expressionType);
                }
            }
            if (node.isWithOrdinality()) {
                outputFields.add(Field.newUnqualified(Optional.empty(), BIGINT));
            }
            return createAndAssignScope(node, scope, outputFields.build());
        }

        @Override
        protected Scope visitLateral(Lateral node, Optional<Scope> scope)
        {
            StatementAnalyzer analyzer = new StatementAnalyzer(analysis, metadata, sqlParser, accessControl, session, warningCollector);
            Scope queryScope = analyzer.analyze(node.getQuery(), scope);
            return createAndAssignScope(node, scope, queryScope.getRelationType());
        }

        @Override
        protected Scope visitTable(Table table, Optional<Scope> scope)
        {
            if (!table.getName().getPrefix().isPresent()) {
                // is this a reference to a WITH query?
                String name = table.getName().getSuffix();

                Optional<WithQuery> withQuery = createScope(scope).getNamedQuery(name);
                if (withQuery.isPresent()) {
                    Query query = withQuery.get().getQuery();
                    analysis.registerNamedQuery(table, query);

                    // re-alias the fields with the name assigned to the query in the WITH declaration
                    RelationType queryDescriptor = analysis.getOutputDescriptor(query);

                    List<Field> fields;
                    Optional<List<Identifier>> columnNames = withQuery.get().getColumnNames();
                    if (columnNames.isPresent()) {
                        // if columns are explicitly aliased -> WITH cte(alias1, alias2 ...)
                        ImmutableList.Builder<Field> fieldBuilder = ImmutableList.builder();

                        Iterator<Field> visibleFieldsIterator = queryDescriptor.getVisibleFields().iterator();
                        for (Identifier columnName : columnNames.get()) {
                            Field inputField = visibleFieldsIterator.next();
                            fieldBuilder.add(Field.newQualified(
                                    QualifiedName.of(name),
                                    Optional.of(columnName.getValue()),
                                    inputField.getType(),
                                    false,
                                    inputField.getOriginTable(),
                                    inputField.getOriginColumnName(),
                                    inputField.isAliased()));
                        }

                        fields = fieldBuilder.build();
                    }
                    else {
                        fields = queryDescriptor.getAllFields().stream()
                                .map(field -> Field.newQualified(
                                        QualifiedName.of(name),
                                        field.getName(),
                                        field.getType(),
                                        field.isHidden(),
                                        field.getOriginTable(),
                                        field.getOriginColumnName(),
                                        field.isAliased()))
                                .collect(toImmutableList());
                    }

                    return createAndAssignScope(table, scope, fields);
                }
            }

            QualifiedObjectName name = createQualifiedObjectName(session, table, table.getName());
            if (name.getObjectName().isEmpty()) {
                throw new SemanticException(MISSING_TABLE, table, "Table name is empty");
            }
            if (name.getSchemaName().isEmpty()) {
                throw new SemanticException(MISSING_SCHEMA, table, "Schema name is empty");
            }
            analysis.addEmptyColumnReferencesForTable(accessControl, session.getIdentity(), name);

            Optional<ViewDefinition> optionalView = metadata.getView(session, name);
            if (optionalView.isPresent()) {
                Statement statement = analysis.getStatement();
                if (statement instanceof CreateView) {
                    CreateView viewStatement = (CreateView) statement;
                    QualifiedObjectName viewNameFromStatement = createQualifiedObjectName(session, viewStatement, viewStatement.getName());
                    if (viewStatement.isReplace() && viewNameFromStatement.equals(name)) {
                        throw new SemanticException(VIEW_IS_RECURSIVE, table, "Statement would create a recursive view");
                    }
                }
                if (analysis.hasTableInView(table)) {
                    throw new SemanticException(VIEW_IS_RECURSIVE, table, "View is recursive");
                }
                ViewDefinition view = optionalView.get();

                Query query = parseView(view.getOriginalSql(), name, table);

                analysis.registerNamedQuery(table, query);

                analysis.registerTableForView(table);
                RelationType descriptor = analyzeView(query, name, view.getCatalog(), view.getSchema(), view.getOwner(), table);
                analysis.unregisterTableForView();

                if (isViewStale(view.getColumns(), descriptor.getVisibleFields())) {
                    throw new SemanticException(VIEW_IS_STALE, table, "View '%s' is stale; it must be re-created", name);
                }

                // Derive the type of the view from the stored definition, not from the analysis of the underlying query.
                // This is needed in case the underlying table(s) changed and the query in the view now produces types that
                // are implicitly coercible to the declared view types.
                List<Field> outputFields = view.getColumns().stream()
                        .map(column -> Field.newQualified(
                                table.getName(),
                                Optional.of(column.getName()),
                                column.getType(),
                                false,
                                Optional.of(name),
                                Optional.of(column.getName()),
                                false))
                        .collect(toImmutableList());

                analysis.addRelationCoercion(table, outputFields.stream().map(Field::getType).toArray(Type[]::new));

                return createAndAssignScope(table, scope, outputFields);
            }

            Optional<TableHandle> tableHandle = metadata.getTableHandle(session, name);
            if (!tableHandle.isPresent()) {
                if (!metadata.getCatalogHandle(session, name.getCatalogName()).isPresent()) {
                    throw new SemanticException(MISSING_CATALOG, table, "Catalog %s does not exist", name.getCatalogName());
                }
                if (!metadata.schemaExists(session, new CatalogSchemaName(name.getCatalogName(), name.getSchemaName()))) {
                    throw new SemanticException(MISSING_SCHEMA, table, "Schema %s does not exist", name.getSchemaName());
                }
                throw new SemanticException(MISSING_TABLE, table, "Table %s does not exist", name);
            }
            TableMetadata tableMetadata = metadata.getTableMetadata(session, tableHandle.get());
            Map<String, ColumnHandle> columnHandles = metadata.getColumnHandles(session, tableHandle.get());

            // TODO: discover columns lazily based on where they are needed (to support connectors that can't enumerate all tables)
            ImmutableList.Builder<Field> fields = ImmutableList.builder();
            for (ColumnMetadata column : tableMetadata.getColumns()) {
                Field field = Field.newQualified(
                        table.getName(),
                        Optional.of(column.getName()),
                        column.getType(),
                        column.isHidden(),
                        Optional.of(name),
                        Optional.of(column.getName()),
                        false);
                fields.add(field);
                ColumnHandle columnHandle = columnHandles.get(column.getName());
                checkArgument(columnHandle != null, "Unknown field %s", field);
                analysis.setColumn(field, columnHandle);
            }

            analysis.registerTable(table, tableHandle.get());

            return createAndAssignScope(table, scope, fields.build());
        }

        @Override
        protected Scope visitAliasedRelation(AliasedRelation relation, Optional<Scope> scope)
        {
            Scope relationScope = process(relation.getRelation(), scope);

            // todo this check should be inside of TupleDescriptor.withAlias, but the exception needs the node object
            RelationType relationType = relationScope.getRelationType();
            if (relation.getColumnNames() != null) {
                int totalColumns = relationType.getVisibleFieldCount();
                if (totalColumns != relation.getColumnNames().size()) {
                    throw new SemanticException(MISMATCHED_COLUMN_ALIASES, relation, "Column alias list has %s entries but '%s' has %s columns available", relation.getColumnNames().size(), relation.getAlias(), totalColumns);
                }
            }

            List<String> aliases = null;
            if (relation.getColumnNames() != null) {
                aliases = relation.getColumnNames().stream()
                        .map(Identifier::getValue)
                        .collect(Collectors.toList());
            }

            RelationType descriptor = relationType.withAlias(relation.getAlias().getValue(), aliases);

            return createAndAssignScope(relation, scope, descriptor);
        }

        @Override
        protected Scope visitSampledRelation(SampledRelation relation, Optional<Scope> scope)
        {
            if (!VariablesExtractor.extractNames(relation.getSamplePercentage(), analysis.getColumnReferences()).isEmpty()) {
                throw new SemanticException(NON_NUMERIC_SAMPLE_PERCENTAGE, relation.getSamplePercentage(), "Sample percentage cannot contain column references");
            }

            Map<NodeRef<Expression>, Type> expressionTypes = getExpressionTypes(
                    session,
                    metadata,
                    sqlParser,
                    TypeProvider.empty(),
                    relation.getSamplePercentage(),
                    analysis.getParameters(),
                    warningCollector,
                    analysis.isDescribe());
            ExpressionInterpreter samplePercentageEval = expressionOptimizer(relation.getSamplePercentage(), metadata, session, expressionTypes);

            Object samplePercentageObject = samplePercentageEval.optimize(symbol -> {
                throw new SemanticException(NON_NUMERIC_SAMPLE_PERCENTAGE, relation.getSamplePercentage(), "Sample percentage cannot contain column references");
            });

            if (!(samplePercentageObject instanceof Number)) {
                throw new SemanticException(NON_NUMERIC_SAMPLE_PERCENTAGE, relation.getSamplePercentage(), "Sample percentage should evaluate to a numeric expression");
            }

            double samplePercentageValue = ((Number) samplePercentageObject).doubleValue();

            if (samplePercentageValue < 0.0) {
                throw new SemanticException(SemanticErrorCode.SAMPLE_PERCENTAGE_OUT_OF_RANGE, relation.getSamplePercentage(), "Sample percentage must be greater than or equal to 0");
            }
            if ((samplePercentageValue > 100.0)) {
                throw new SemanticException(SemanticErrorCode.SAMPLE_PERCENTAGE_OUT_OF_RANGE, relation.getSamplePercentage(), "Sample percentage must be less than or equal to 100");
            }

            analysis.setSampleRatio(relation, samplePercentageValue / 100);
            Scope relationScope = process(relation.getRelation(), scope);
            return createAndAssignScope(relation, scope, relationScope.getRelationType());
        }

        @Override
        protected Scope visitTableSubquery(TableSubquery node, Optional<Scope> scope)
        {
            StatementAnalyzer analyzer = new StatementAnalyzer(analysis, metadata, sqlParser, accessControl, session, warningCollector);
            Scope queryScope = analyzer.analyze(node.getQuery(), scope);
            return createAndAssignScope(node, scope, queryScope.getRelationType());
        }

        @Override
        protected Scope visitQuerySpecification(QuerySpecification node, Optional<Scope> scope)
        {
            // TODO: extract candidate names from SELECT, WHERE, HAVING, GROUP BY and ORDER BY expressions
            // to pass down to analyzeFrom

            Scope sourceScope = analyzeFrom(node, scope);

            node.getWhere().ifPresent(where -> analyzeWhere(node, sourceScope, where));

            List<Expression> outputExpressions = analyzeSelect(node, sourceScope);
            List<Expression> groupByExpressions = analyzeGroupBy(node, sourceScope, outputExpressions);
            analyzeHaving(node, sourceScope);

            Scope outputScope = computeAndAssignOutputScope(node, scope, sourceScope);

            List<Expression> orderByExpressions = emptyList();
            Optional<Scope> orderByScope = Optional.empty();
            if (node.getOrderBy().isPresent()) {
                if (node.getSelect().isDistinct()) {
                    verifySelectDistinct(node, outputExpressions);
                }

                OrderBy orderBy = node.getOrderBy().get();
                orderByScope = Optional.of(computeAndAssignOrderByScope(orderBy, sourceScope, outputScope));

                orderByExpressions = analyzeOrderBy(node, orderBy.getSortItems(), orderByScope.get());

                if (sourceScope.getOuterQueryParent().isPresent() && !node.getLimit().isPresent()) {
                    // not the root scope and ORDER BY is ineffective
                    analysis.markRedundantOrderBy(orderBy);
                    warningCollector.add(new PrestoWarning(REDUNDANT_ORDER_BY, "ORDER BY in subquery may have no effect"));
                }
            }
            analysis.setOrderByExpressions(node, orderByExpressions);

            List<Expression> sourceExpressions = new ArrayList<>(outputExpressions);
            node.getHaving().ifPresent(sourceExpressions::add);

            analyzeGroupingOperations(node, sourceExpressions, orderByExpressions);
            List<FunctionCall> aggregates = analyzeAggregations(node, sourceExpressions, orderByExpressions);

            if (!aggregates.isEmpty() && groupByExpressions.isEmpty()) {
                // Have Aggregation functions but no explicit GROUP BY clause
                analysis.setGroupByExpressions(node, ImmutableList.of());
            }

            verifyAggregations(node, sourceScope, orderByScope, groupByExpressions, sourceExpressions, orderByExpressions);

            analyzeWindowFunctions(node, outputExpressions, orderByExpressions);

            if (analysis.isAggregation(node) && node.getOrderBy().isPresent()) {
                // Create a different scope for ORDER BY expressions when aggregation is present.
                // This is because planner requires scope in order to resolve names against fields.
                // Original ORDER BY scope "sees" FROM query fields. However, during planning
                // and when aggregation is present, ORDER BY expressions should only be resolvable against
                // output scope, group by expressions and aggregation expressions.
                List<GroupingOperation> orderByGroupingOperations = extractExpressions(orderByExpressions, GroupingOperation.class);
                List<FunctionCall> orderByAggregations = extractAggregateFunctions(analysis.getFunctionHandles(), orderByExpressions, metadata.getFunctionAndTypeManager());
                computeAndAssignOrderByScopeWithAggregation(node.getOrderBy().get(), sourceScope, outputScope, orderByAggregations, groupByExpressions, orderByGroupingOperations);
            }

            return outputScope;
        }

        @Override
        protected Scope visitSetOperation(SetOperation node, Optional<Scope> scope)
        {
            checkState(node.getRelations().size() >= 2);
            List<Scope> relationScopes = node.getRelations().stream()
                    .map(relation -> {
                        Scope relationScope = process(relation, scope);
                        return createAndAssignScope(relation, scope, relationScope.getRelationType().withOnlyVisibleFields());
                    })
                    .collect(toImmutableList());

            Type[] outputFieldTypes = relationScopes.get(0).getRelationType().getVisibleFields().stream()
                    .map(Field::getType)
                    .toArray(Type[]::new);
            int outputFieldSize = outputFieldTypes.length;
            if (isExpensiveUnionDistinct(node, outputFieldTypes)) {
                warningCollector.add(new PrestoWarning(
                        PERFORMANCE_WARNING,
                        format("UNION DISTINCT query should consider avoiding double/real/complex types and reducing the number of visible fields (%d) to %d",
                                outputFieldSize,
                                UNION_DISTINCT_FIELDS_WARNING_THRESHOLD)));
            }
            for (Scope relationScope : relationScopes) {
                RelationType relationType = relationScope.getRelationType();
                int descFieldSize = relationType.getVisibleFields().size();
                String setOperationName = node.getClass().getSimpleName().toUpperCase(ENGLISH);
                if (outputFieldSize != descFieldSize) {
                    throw new SemanticException(
                            MISMATCHED_SET_COLUMN_TYPES,
                            node,
                            "%s query has different number of fields: %d, %d",
                            setOperationName,
                            outputFieldSize,
                            descFieldSize);
                }
                for (int i = 0; i < descFieldSize; i++) {
                    Type descFieldType = relationType.getFieldByIndex(i).getType();
                    Optional<Type> commonSuperType = metadata.getFunctionAndTypeManager().getCommonSuperType(outputFieldTypes[i], descFieldType);
                    if (!commonSuperType.isPresent()) {
                        throw new SemanticException(
                                TYPE_MISMATCH,
                                node,
                                "column %d in %s query has incompatible types: %s, %s",
                                i + 1,
                                setOperationName,
                                outputFieldTypes[i].getDisplayName(),
                                descFieldType.getDisplayName());
                    }
                    outputFieldTypes[i] = commonSuperType.get();
                }
            }

            Field[] outputDescriptorFields = new Field[outputFieldTypes.length];
            RelationType firstDescriptor = relationScopes.get(0).getRelationType().withOnlyVisibleFields();
            for (int i = 0; i < outputFieldTypes.length; i++) {
                Field oldField = firstDescriptor.getFieldByIndex(i);
                outputDescriptorFields[i] = new Field(
                        oldField.getRelationAlias(),
                        oldField.getName(),
                        outputFieldTypes[i],
                        oldField.isHidden(),
                        oldField.getOriginTable(),
                        oldField.getOriginColumnName(),
                        oldField.isAliased());
            }

            for (int i = 0; i < node.getRelations().size(); i++) {
                Relation relation = node.getRelations().get(i);
                Scope relationScope = relationScopes.get(i);
                RelationType relationType = relationScope.getRelationType();
                for (int j = 0; j < relationType.getVisibleFields().size(); j++) {
                    Type outputFieldType = outputFieldTypes[j];
                    Type descFieldType = relationType.getFieldByIndex(j).getType();
                    if (!outputFieldType.equals(descFieldType)) {
                        analysis.addRelationCoercion(relation, outputFieldTypes);
                        break;
                    }
                }
            }
            return createAndAssignScope(node, scope, outputDescriptorFields);
        }

        private boolean isExpensiveUnionDistinct(SetOperation setOperation, Type[] outputTypes)
        {
            return setOperation instanceof Union &&
                    setOperation.isDistinct().orElse(false) &&
                    outputTypes.length > UNION_DISTINCT_FIELDS_WARNING_THRESHOLD &&
                    Arrays.stream(outputTypes)
                            .anyMatch(
                                    type -> type instanceof RealType ||
                                            type instanceof DoubleType ||
                                            type instanceof MapType ||
                                            type instanceof ArrayType ||
                                            type instanceof RowType);
        }

        @Override
        protected Scope visitUnion(Union node, Optional<Scope> scope)
        {
            if (!node.isDistinct().isPresent()) {
                warningCollector.add(new PrestoWarning(
                        PERFORMANCE_WARNING,
                        "UNION specified without ALL or DISTINCT keyword is equivalent to UNION DISTINCT, which is computationally expensive. " +
                                "Consider using UNION ALL when possible, or specifically add the keyword DISTINCT if absolutely necessary"));
            }
            return visitSetOperation(node, scope);
        }

        @Override
        protected Scope visitIntersect(Intersect node, Optional<Scope> scope)
        {
            if (!node.isDistinct().orElse(true)) {
                throw new SemanticException(NOT_SUPPORTED, node, "INTERSECT ALL not yet implemented");
            }

            return visitSetOperation(node, scope);
        }

        @Override
        protected Scope visitExcept(Except node, Optional<Scope> scope)
        {
            if (!node.isDistinct().orElse(true)) {
                throw new SemanticException(NOT_SUPPORTED, node, "EXCEPT ALL not yet implemented");
            }

            return visitSetOperation(node, scope);
        }

        @Override
        protected Scope visitJoin(Join node, Optional<Scope> scope)
        {
            JoinCriteria criteria = node.getCriteria().orElse(null);
            if (criteria instanceof NaturalJoin) {
                throw new SemanticException(NOT_SUPPORTED, node, "Natural join not supported");
            }

            Scope left = process(node.getLeft(), scope);
            Scope right = process(node.getRight(), isLateralRelation(node.getRight()) ? Optional.of(left) : scope);

            if (criteria instanceof JoinUsing) {
                return analyzeJoinUsing(node, ((JoinUsing) criteria).getColumns(), scope, left, right);
            }

            Scope output = createAndAssignScope(node, scope, left.getRelationType().joinWith(right.getRelationType()));

            if (node.getType() == Join.Type.CROSS || node.getType() == Join.Type.IMPLICIT) {
                return output;
            }
            else if (criteria instanceof JoinOn) {
                Expression expression = ((JoinOn) criteria).getExpression();

                // need to register coercions in case when join criteria requires coercion (e.g. join on char(1) = char(2))
                ExpressionAnalysis expressionAnalysis = analyzeExpression(expression, output);
                Type clauseType = expressionAnalysis.getType(expression);
                if (!clauseType.equals(BOOLEAN)) {
                    if (!clauseType.equals(UNKNOWN)) {
                        throw new SemanticException(TYPE_MISMATCH, expression, "JOIN ON clause must evaluate to a boolean: actual type %s", clauseType);
                    }
                    // coerce null to boolean
                    analysis.addCoercion(expression, BOOLEAN, false);
                }

                verifyNoAggregateWindowOrGroupingFunctions(analysis.getFunctionHandles(), metadata.getFunctionAndTypeManager(), expression, "JOIN clause");

                analysis.recordSubqueries(node, expressionAnalysis);
                analysis.setJoinCriteria(node, expression);
            }
            else {
                throw new UnsupportedOperationException("unsupported join criteria: " + criteria.getClass().getName());
            }

            return output;
        }

        private Scope analyzeJoinUsing(Join node, List<Identifier> columns, Optional<Scope> scope, Scope left, Scope right)
        {
            List<Field> joinFields = new ArrayList<>();

            List<Integer> leftJoinFields = new ArrayList<>();
            List<Integer> rightJoinFields = new ArrayList<>();

            Set<Identifier> seen = new HashSet<>();
            for (Identifier column : columns) {
                if (!seen.add(column)) {
                    throw new SemanticException(DUPLICATE_COLUMN_NAME, column, "Column '%s' appears multiple times in USING clause", column.getValue());
                }

                Optional<ResolvedField> leftField = left.tryResolveField(column);
                Optional<ResolvedField> rightField = right.tryResolveField(column);

                if (!leftField.isPresent()) {
                    throw new SemanticException(MISSING_ATTRIBUTE, column, "Column '%s' is missing from left side of join", column.getValue());
                }
                if (!rightField.isPresent()) {
                    throw new SemanticException(MISSING_ATTRIBUTE, column, "Column '%s' is missing from right side of join", column.getValue());
                }

                // ensure a comparison operator exists for the given types (applying coercions if necessary)
                try {
                    metadata.getFunctionAndTypeManager().resolveOperator(OperatorType.EQUAL, fromTypes(
                            leftField.get().getType(), rightField.get().getType()));
                }
                catch (OperatorNotFoundException e) {
                    throw new SemanticException(TYPE_MISMATCH, column, "%s", e.getMessage());
                }

                Optional<Type> type = metadata.getFunctionAndTypeManager().getCommonSuperType(leftField.get().getType(), rightField.get().getType());
                analysis.addTypes(ImmutableMap.of(NodeRef.of(column), type.get()));

                joinFields.add(Field.newUnqualified(column.getValue(), type.get()));

                leftJoinFields.add(leftField.get().getRelationFieldIndex());
                rightJoinFields.add(rightField.get().getRelationFieldIndex());

                analysis.addColumnReference(NodeRef.of(column), FieldId.from(leftField.get()));
                analysis.addColumnReference(NodeRef.of(column), FieldId.from(rightField.get()));
                if (leftField.get().getField().getOriginTable().isPresent() && leftField.get().getField().getOriginColumnName().isPresent()) {
                    analysis.addTableColumnReferences(
                            accessControl,
                            session.getIdentity(),
                            ImmutableMultimap.of(leftField.get().getField().getOriginTable().get(), leftField.get().getField().getOriginColumnName().get()));
                }
                if (rightField.get().getField().getOriginTable().isPresent() && rightField.get().getField().getOriginColumnName().isPresent()) {
                    analysis.addTableColumnReferences(
                            accessControl,
                            session.getIdentity(),
                            ImmutableMultimap.of(rightField.get().getField().getOriginTable().get(), rightField.get().getField().getOriginColumnName().get()));
                }
            }

            ImmutableList.Builder<Field> outputs = ImmutableList.builder();
            outputs.addAll(joinFields);

            ImmutableList.Builder<Integer> leftFields = ImmutableList.builder();
            for (int i = 0; i < left.getRelationType().getAllFieldCount(); i++) {
                if (!leftJoinFields.contains(i)) {
                    outputs.add(left.getRelationType().getFieldByIndex(i));
                    leftFields.add(i);
                }
            }

            ImmutableList.Builder<Integer> rightFields = ImmutableList.builder();
            for (int i = 0; i < right.getRelationType().getAllFieldCount(); i++) {
                if (!rightJoinFields.contains(i)) {
                    outputs.add(right.getRelationType().getFieldByIndex(i));
                    rightFields.add(i);
                }
            }

            analysis.setJoinUsing(node, new Analysis.JoinUsingAnalysis(leftJoinFields, rightJoinFields, leftFields.build(), rightFields.build()));

            return createAndAssignScope(node, scope, new RelationType(outputs.build()));
        }

        private boolean isLateralRelation(Relation node)
        {
            if (node instanceof AliasedRelation) {
                return isLateralRelation(((AliasedRelation) node).getRelation());
            }
            return node instanceof Unnest || node instanceof Lateral;
        }

        @Override
        protected Scope visitValues(Values node, Optional<Scope> scope)
        {
            checkState(node.getRows().size() >= 1);

            List<List<Type>> rowTypes = node.getRows().stream()
                    .map(row -> analyzeExpression(row, createScope(scope)).getType(row))
                    .map(type -> {
                        if (type instanceof RowType) {
                            return type.getTypeParameters();
                        }
                        return ImmutableList.of(type);
                    })
                    .collect(toImmutableList());

            // determine common super type of the rows
            List<Type> fieldTypes = new ArrayList<>(rowTypes.iterator().next());
            for (List<Type> rowType : rowTypes) {
                // check field count consistency for rows
                if (rowType.size() != fieldTypes.size()) {
                    throw new SemanticException(MISMATCHED_SET_COLUMN_TYPES,
                            node,
                            "Values rows have mismatched types: %s vs %s",
                            rowTypes.get(0),
                            rowType);
                }

                for (int i = 0; i < rowType.size(); i++) {
                    Type fieldType = rowType.get(i);
                    Type superType = fieldTypes.get(i);

                    Optional<Type> commonSuperType = metadata.getFunctionAndTypeManager().getCommonSuperType(fieldType, superType);
                    if (!commonSuperType.isPresent()) {
                        throw new SemanticException(MISMATCHED_SET_COLUMN_TYPES,
                                node,
                                "Values rows have mismatched types: %s vs %s",
                                rowTypes.get(0),
                                rowType);
                    }
                    fieldTypes.set(i, commonSuperType.get());
                }
            }

            // add coercions for the rows
            for (Expression row : node.getRows()) {
                if (row instanceof Row) {
                    List<Expression> items = ((Row) row).getItems();
                    for (int i = 0; i < items.size(); i++) {
                        Type expectedType = fieldTypes.get(i);
                        Expression item = items.get(i);
                        Type actualType = analysis.getType(item);
                        if (!actualType.equals(expectedType)) {
                            analysis.addCoercion(item, expectedType, metadata.getFunctionAndTypeManager().isTypeOnlyCoercion(actualType, expectedType));
                        }
                    }
                }
                else {
                    Type actualType = analysis.getType(row);
                    Type expectedType = fieldTypes.get(0);
                    if (!actualType.equals(expectedType)) {
                        analysis.addCoercion(row, expectedType, metadata.getFunctionAndTypeManager().isTypeOnlyCoercion(actualType, expectedType));
                    }
                }
            }

            List<Field> fields = fieldTypes.stream()
                    .map(valueType -> Field.newUnqualified(Optional.empty(), valueType))
                    .collect(toImmutableList());

            return createAndAssignScope(node, scope, fields);
        }

        private void analyzeWindowFunctions(QuerySpecification node, List<Expression> outputExpressions, List<Expression> orderByExpressions)
        {
            analysis.setWindowFunctions(node, analyzeWindowFunctions(node, outputExpressions));
            if (node.getOrderBy().isPresent()) {
                analysis.setOrderByWindowFunctions(node.getOrderBy().get(), analyzeWindowFunctions(node, orderByExpressions));
            }
        }

        private List<FunctionCall> analyzeWindowFunctions(QuerySpecification node, List<Expression> expressions)
        {
            for (Expression expression : expressions) {
                new WindowFunctionValidator(metadata.getFunctionAndTypeManager()).process(expression, analysis);
            }

            List<FunctionCall> windowFunctions = extractWindowFunctions(expressions);

            for (FunctionCall windowFunction : windowFunctions) {
                // filter with window function is not supported yet
                if (windowFunction.getFilter().isPresent()) {
                    throw new SemanticException(NOT_SUPPORTED, node, "FILTER is not yet supported for window functions");
                }

                if (windowFunction.getOrderBy().isPresent()) {
                    throw new SemanticException(NOT_SUPPORTED, windowFunction, "Window function with ORDER BY is not supported");
                }

                Window window = windowFunction.getWindow().get();
                if (window.getOrderBy().filter(
                        orderBy -> orderBy.getSortItems()
                                .stream()
                                .anyMatch(item -> item.getSortKey() instanceof Literal))
                        .isPresent()) {
                    if (isAllowWindowOrderByLiterals(session)) {
                        warningCollector.add(
                                new PrestoWarning(
                                        PERFORMANCE_WARNING,
                                        String.format(
                                                "ORDER BY literals/constants with window function: '%s' is unnecessary and expensive. If you intend to ORDER BY using ordinals, please use the actual expression instead of the ordinal",
                                                windowFunction)));
                    }
                    else {
                        throw new SemanticException(
                                WINDOW_FUNCTION_ORDERBY_LITERAL,
                                node,
                                "ORDER BY literals/constants with window function: '%s' is unnecessary and expensive. If you intend to ORDER BY using ordinals, please use the actual expression instead of the ordinal",
                                windowFunction);
                    }
                }

                ImmutableList.Builder<Node> toExtract = ImmutableList.builder();
                toExtract.addAll(windowFunction.getArguments());
                toExtract.addAll(window.getPartitionBy());
                window.getOrderBy().ifPresent(orderBy -> toExtract.addAll(orderBy.getSortItems()));
                window.getFrame().ifPresent(toExtract::add);

                List<FunctionCall> nestedWindowFunctions = extractWindowFunctions(toExtract.build());

                if (!nestedWindowFunctions.isEmpty()) {
                    throw new SemanticException(NESTED_WINDOW, node, "Cannot nest window functions inside window function '%s': %s",
                            windowFunction,
                            windowFunctions);
                }

                if (windowFunction.isDistinct()) {
                    throw new SemanticException(NOT_SUPPORTED, node, "DISTINCT in window function parameters not yet supported: %s", windowFunction);
                }

                if (window.getFrame().isPresent()) {
                    analyzeWindowFrame(window.getFrame().get());
                }

                FunctionKind kind = metadata.getFunctionAndTypeManager().getFunctionMetadata(analysis.getFunctionHandle(windowFunction)).getFunctionKind();
                if (kind != AGGREGATE && kind != WINDOW) {
                    throw new SemanticException(MUST_BE_WINDOW_FUNCTION, node, "Not a window function: %s", windowFunction.getName());
                }
            }

            return windowFunctions;
        }

        private void analyzeWindowFrame(WindowFrame frame)
        {
            FrameBound.Type startType = frame.getStart().getType();
            FrameBound.Type endType = frame.getEnd().orElse(new FrameBound(CURRENT_ROW)).getType();

            if (startType == UNBOUNDED_FOLLOWING) {
                throw new SemanticException(INVALID_WINDOW_FRAME, frame, "Window frame start cannot be UNBOUNDED FOLLOWING");
            }
            if (endType == UNBOUNDED_PRECEDING) {
                throw new SemanticException(INVALID_WINDOW_FRAME, frame, "Window frame end cannot be UNBOUNDED PRECEDING");
            }
            if ((startType == CURRENT_ROW) && (endType == PRECEDING)) {
                throw new SemanticException(INVALID_WINDOW_FRAME, frame, "Window frame starting from CURRENT ROW cannot end with PRECEDING");
            }
            if ((startType == FOLLOWING) && (endType == PRECEDING)) {
                throw new SemanticException(INVALID_WINDOW_FRAME, frame, "Window frame starting from FOLLOWING cannot end with PRECEDING");
            }
            if ((startType == FOLLOWING) && (endType == CURRENT_ROW)) {
                throw new SemanticException(INVALID_WINDOW_FRAME, frame, "Window frame starting from FOLLOWING cannot end with CURRENT ROW");
            }
            if ((frame.getType() == RANGE) && ((startType == PRECEDING) || (endType == PRECEDING))) {
                throw new SemanticException(INVALID_WINDOW_FRAME, frame, "Window frame RANGE PRECEDING is only supported with UNBOUNDED");
            }
            if ((frame.getType() == RANGE) && ((startType == FOLLOWING) || (endType == FOLLOWING))) {
                throw new SemanticException(INVALID_WINDOW_FRAME, frame, "Window frame RANGE FOLLOWING is only supported with UNBOUNDED");
            }
        }

        private void analyzeHaving(QuerySpecification node, Scope scope)
        {
            if (node.getHaving().isPresent()) {
                Expression predicate = node.getHaving().get();

                ExpressionAnalysis expressionAnalysis = analyzeExpression(predicate, scope);

                expressionAnalysis.getWindowFunctions().stream()
                        .findFirst()
                        .ifPresent(function -> {
                            throw new SemanticException(NESTED_WINDOW, function.getNode(), "HAVING clause cannot contain window functions");
                        });

                analysis.recordSubqueries(node, expressionAnalysis);

                Type predicateType = expressionAnalysis.getType(predicate);
                if (!predicateType.equals(BOOLEAN) && !predicateType.equals(UNKNOWN)) {
                    throw new SemanticException(TYPE_MISMATCH, predicate, "HAVING clause must evaluate to a boolean: actual type %s", predicateType);
                }

                analysis.setHaving(node, predicate);
            }
        }

        private Multimap<QualifiedName, Expression> extractNamedOutputExpressions(Select node)
        {
            // Compute aliased output terms so we can resolve order by expressions against them first
            ImmutableMultimap.Builder<QualifiedName, Expression> assignments = ImmutableMultimap.builder();
            for (SelectItem item : node.getSelectItems()) {
                if (item instanceof SingleColumn) {
                    SingleColumn column = (SingleColumn) item;
                    Optional<Identifier> alias = column.getAlias();
                    if (alias.isPresent()) {
                        assignments.put(QualifiedName.of(alias.get().getValue()), column.getExpression()); // TODO: need to know if alias was quoted
                    }
                    else if (column.getExpression() instanceof Identifier) {
                        assignments.put(QualifiedName.of(((Identifier) column.getExpression()).getValue()), column.getExpression());
                    }
                }
            }

            return assignments.build();
        }

        private void checkFunctionName(Statement node, QualifiedName functionName, boolean isTemporary)
        {
            if (isTemporary) {
                if (functionName.getParts().size() != 1) {
                    throw new SemanticException(INVALID_FUNCTION_NAME, node, "Temporary functions cannot be qualified.");
                }

                List<String> builtInFunctionNames = metadata.getFunctionAndTypeManager().listBuiltInFunctions().stream()
                        .map(SqlFunction::getSignature)
                        .map(Signature::getName)
                        .map(QualifiedObjectName::getObjectName)
                        .collect(Collectors.toList());
                if (builtInFunctionNames.contains(functionName.toString())) {
                    throw new SemanticException(INVALID_FUNCTION_NAME, node, format("Function %s is already registered as a built-in function.", functionName));
                }
            }
            else {
                if (functionName.getParts().size() != 3) {
                    throw new SemanticException(INVALID_FUNCTION_NAME, node, format("Function name should be in the form of catalog.schema.function_name, found: %s", functionName));
                }
            }
        }

        private class OrderByExpressionRewriter
                extends ExpressionRewriter<Void>
        {
            private final Multimap<QualifiedName, Expression> assignments;

            public OrderByExpressionRewriter(Multimap<QualifiedName, Expression> assignments)
            {
                this.assignments = assignments;
            }

            @Override
            public Expression rewriteIdentifier(Identifier reference, Void context, ExpressionTreeRewriter<Void> treeRewriter)
            {
                // if this is a simple name reference, try to resolve against output columns
                QualifiedName name = QualifiedName.of(reference.getValue());
                Set<Expression> expressions = assignments.get(name)
                        .stream()
                        .collect(Collectors.toSet());

                if (expressions.size() > 1) {
                    throw new SemanticException(AMBIGUOUS_ATTRIBUTE, reference, "'%s' in ORDER BY is ambiguous", name);
                }

                if (expressions.size() == 1) {
                    return Iterables.getOnlyElement(expressions);
                }

                // otherwise, couldn't resolve name against output aliases, so fall through...
                return reference;
            }
        }

        private void checkGroupingSetsCount(GroupBy node)
        {
            // If groupBy is distinct then crossProduct will be overestimated if there are duplicate grouping sets.
            int crossProduct = 1;
            for (GroupingElement element : node.getGroupingElements()) {
                try {
                    int product;
                    if (element instanceof SimpleGroupBy) {
                        product = 1;
                    }
                    else if (element instanceof Cube) {
                        int exponent = element.getExpressions().size();
                        if (exponent > 30) {
                            throw new ArithmeticException();
                        }
                        product = 1 << exponent;
                    }
                    else if (element instanceof Rollup) {
                        product = element.getExpressions().size() + 1;
                    }
                    else if (element instanceof GroupingSets) {
                        product = ((GroupingSets) element).getSets().size();
                    }
                    else {
                        throw new UnsupportedOperationException("Unsupported grouping element type: " + element.getClass().getName());
                    }
                    crossProduct = Math.multiplyExact(crossProduct, product);
                }
                catch (ArithmeticException e) {
                    throw new SemanticException(TOO_MANY_GROUPING_SETS, node,
                            "GROUP BY has more than %s grouping sets but can contain at most %s", Integer.MAX_VALUE, getMaxGroupingSets(session));
                }
                if (crossProduct > getMaxGroupingSets(session)) {
                    throw new SemanticException(TOO_MANY_GROUPING_SETS, node,
                            "GROUP BY has %s grouping sets but can contain at most %s", crossProduct, getMaxGroupingSets(session));
                }
            }
        }

        private List<Expression> analyzeGroupBy(QuerySpecification node, Scope scope, List<Expression> outputExpressions)
        {
            if (node.getGroupBy().isPresent()) {
                ImmutableList.Builder<Set<FieldId>> cubes = ImmutableList.builder();
                ImmutableList.Builder<List<FieldId>> rollups = ImmutableList.builder();
                ImmutableList.Builder<List<Set<FieldId>>> sets = ImmutableList.builder();
                ImmutableList.Builder<Expression> complexExpressions = ImmutableList.builder();
                ImmutableList.Builder<Expression> groupingExpressions = ImmutableList.builder();

                checkGroupingSetsCount(node.getGroupBy().get());
                for (GroupingElement groupingElement : node.getGroupBy().get().getGroupingElements()) {
                    if (groupingElement instanceof SimpleGroupBy) {
                        for (Expression column : groupingElement.getExpressions()) {
                            // simple GROUP BY expressions allow ordinals or arbitrary expressions
                            if (column instanceof LongLiteral) {
                                long ordinal = ((LongLiteral) column).getValue();
                                if (ordinal < 1 || ordinal > outputExpressions.size()) {
                                    throw new SemanticException(INVALID_ORDINAL, column, "GROUP BY position %s is not in select list", ordinal);
                                }

                                column = outputExpressions.get(toIntExact(ordinal - 1));
                            }
                            else {
                                analyzeExpression(column, scope);
                            }

                            if (analysis.getColumnReferenceFields().containsKey(NodeRef.of(column))) {
                                sets.add(ImmutableList.of(ImmutableSet.copyOf(analysis.getColumnReferenceFields().get(NodeRef.of(column)))));
                            }
                            else {
                                verifyNoAggregateWindowOrGroupingFunctions(analysis.getFunctionHandles(), metadata.getFunctionAndTypeManager(), column, "GROUP BY clause");
                                analysis.recordSubqueries(node, analyzeExpression(column, scope));
                                complexExpressions.add(column);
                            }

                            groupingExpressions.add(column);
                        }
                    }
                    else {
                        for (Expression column : groupingElement.getExpressions()) {
                            analyzeExpression(column, scope);
                            if (!analysis.getColumnReferences().contains(NodeRef.of(column))) {
                                throw new SemanticException(SemanticErrorCode.MUST_BE_COLUMN_REFERENCE, column, "GROUP BY expression must be a column reference: %s", column);
                            }

                            groupingExpressions.add(column);
                        }

                        if (groupingElement instanceof Cube) {
                            Set<FieldId> cube = groupingElement.getExpressions().stream()
                                    .map(NodeRef::of)
                                    .map(analysis.getColumnReferenceFields()::get)
                                    .flatMap(Collection::stream)
                                    .collect(toImmutableSet());

                            cubes.add(cube);
                        }
                        else if (groupingElement instanceof Rollup) {
                            List<FieldId> rollup = groupingElement.getExpressions().stream()
                                    .map(NodeRef::of)
                                    .map(analysis.getColumnReferenceFields()::get)
                                    .flatMap(Collection::stream)
                                    .collect(toImmutableList());

                            rollups.add(rollup);
                        }
                        else if (groupingElement instanceof GroupingSets) {
                            List<Set<FieldId>> groupingSets = ((GroupingSets) groupingElement).getSets().stream()
                                    .map(set -> set.stream()
                                            .map(NodeRef::of)
                                            .map(analysis.getColumnReferenceFields()::get)
                                            .flatMap(Collection::stream)
                                            .collect(toImmutableSet()))
                                    .collect(toImmutableList());

                            sets.add(groupingSets);
                        }
                    }
                }

                List<Expression> expressions = groupingExpressions.build();
                for (Expression expression : expressions) {
                    Type type = analysis.getType(expression);
                    if (!type.isComparable()) {
                        throw new SemanticException(TYPE_MISMATCH, node, "%s is not comparable, and therefore cannot be used in GROUP BY", type);
                    }
                }

                analysis.setGroupByExpressions(node, expressions);
                analysis.setGroupingSets(node, new Analysis.GroupingSetAnalysis(cubes.build(), rollups.build(), sets.build(), complexExpressions.build()));

                return expressions;
            }

            return ImmutableList.of();
        }

        private Scope computeAndAssignOutputScope(QuerySpecification node, Optional<Scope> scope, Scope sourceScope)
        {
            ImmutableList.Builder<Field> outputFields = ImmutableList.builder();

            for (SelectItem item : node.getSelect().getSelectItems()) {
                if (item instanceof AllColumns) {
                    // expand * and T.*
                    Optional<QualifiedName> starPrefix = ((AllColumns) item).getPrefix();

                    for (Field field : sourceScope.getRelationType().resolveFieldsWithPrefix(starPrefix)) {
                        outputFields.add(Field.newUnqualified(field.getName(), field.getType(), field.getOriginTable(), field.getOriginColumnName(), false));
                    }
                }
                else if (item instanceof SingleColumn) {
                    SingleColumn column = (SingleColumn) item;

                    Expression expression = column.getExpression();
                    Optional<Identifier> field = column.getAlias();

                    Optional<QualifiedObjectName> originTable = Optional.empty();
                    Optional<String> originColumn = Optional.empty();
                    QualifiedName name = null;

                    if (expression instanceof Identifier) {
                        name = QualifiedName.of(((Identifier) expression).getValue());
                    }
                    else if (expression instanceof DereferenceExpression) {
                        name = DereferenceExpression.getQualifiedName((DereferenceExpression) expression);
                    }

                    if (name != null) {
                        List<Field> matchingFields = sourceScope.getRelationType().resolveFields(name);
                        if (!matchingFields.isEmpty()) {
                            originTable = matchingFields.get(0).getOriginTable();
                            originColumn = matchingFields.get(0).getOriginColumnName();
                        }
                    }

                    if (!field.isPresent()) {
                        if (name != null) {
                            field = Optional.of(new Identifier(getLast(name.getOriginalParts())));
                        }
                    }

                    outputFields.add(Field.newUnqualified(field.map(Identifier::getValue), analysis.getType(expression), originTable, originColumn, column.getAlias().isPresent())); // TODO don't use analysis as a side-channel. Use outputExpressions to look up the type
                }
                else {
                    throw new IllegalArgumentException("Unsupported SelectItem type: " + item.getClass().getName());
                }
            }

            return createAndAssignScope(node, scope, outputFields.build());
        }

        private Scope computeAndAssignOrderByScope(OrderBy node, Scope sourceScope, Scope outputScope)
        {
            // ORDER BY should "see" both output and FROM fields during initial analysis and non-aggregation query planning
            Scope orderByScope = Scope.builder()
                    .withParent(sourceScope)
                    .withRelationType(outputScope.getRelationId(), outputScope.getRelationType())
                    .build();
            analysis.setScope(node, orderByScope);
            return orderByScope;
        }

        private Scope computeAndAssignOrderByScopeWithAggregation(OrderBy node, Scope sourceScope, Scope outputScope, List<FunctionCall> aggregations, List<Expression> groupByExpressions, List<GroupingOperation> groupingOperations)
        {
            // This scope is only used for planning. When aggregation is present then
            // only output fields, groups and aggregation expressions should be visible from ORDER BY expression
            ImmutableList.Builder<Expression> orderByAggregationExpressionsBuilder = ImmutableList.<Expression>builder()
                    .addAll(groupByExpressions)
                    .addAll(aggregations)
                    .addAll(groupingOperations);

            // Don't add aggregate complex expressions that contains references to output column because the names would clash in TranslationMap during planning.
            List<Expression> orderByExpressionsReferencingOutputScope = AstUtils.preOrder(node)
                    .filter(Expression.class::isInstance)
                    .map(Expression.class::cast)
                    .filter(expression -> hasReferencesToScope(expression, analysis, outputScope))
                    .collect(toImmutableList());
            List<Expression> orderByAggregationExpressions = orderByAggregationExpressionsBuilder.build().stream()
                    .filter(expression -> !orderByExpressionsReferencingOutputScope.contains(expression) || analysis.isColumnReference(expression))
                    .collect(toImmutableList());

            // generate placeholder fields
            Set<Field> seen = new HashSet<>();
            List<Field> orderByAggregationSourceFields = orderByAggregationExpressions.stream()
                    .map(expression -> {
                        // generate qualified placeholder field for GROUP BY expressions that are column references
                        Optional<Field> sourceField = sourceScope.tryResolveField(expression)
                                .filter(resolvedField -> seen.add(resolvedField.getField()))
                                .map(ResolvedField::getField);
                        return sourceField
                                .orElse(Field.newUnqualified(Optional.empty(), analysis.getType(expression)));
                    })
                    .collect(toImmutableList());

            Scope orderByAggregationScope = Scope.builder()
                    .withRelationType(RelationId.anonymous(), new RelationType(orderByAggregationSourceFields))
                    .build();

            Scope orderByScope = Scope.builder()
                    .withParent(orderByAggregationScope)
                    .withRelationType(outputScope.getRelationId(), outputScope.getRelationType())
                    .build();
            analysis.setScope(node, orderByScope);
            analysis.setOrderByAggregates(node, orderByAggregationExpressions);
            return orderByScope;
        }

        private List<Expression> analyzeSelect(QuerySpecification node, Scope scope)
        {
            ImmutableList.Builder<Expression> outputExpressionBuilder = ImmutableList.builder();

            for (SelectItem item : node.getSelect().getSelectItems()) {
                if (item instanceof AllColumns) {
                    // expand * and T.*
                    Optional<QualifiedName> starPrefix = ((AllColumns) item).getPrefix();

                    RelationType relationType = scope.getRelationType();
                    List<Field> fields = relationType.resolveFieldsWithPrefix(starPrefix);
                    if (fields.isEmpty()) {
                        if (starPrefix.isPresent()) {
                            throw new SemanticException(MISSING_TABLE, item, "Table '%s' not found", starPrefix.get());
                        }
                        if (!node.getFrom().isPresent()) {
                            throw new SemanticException(WILDCARD_WITHOUT_FROM, item, "SELECT * not allowed in queries without FROM clause");
                        }
                        throw new SemanticException(COLUMN_NAME_NOT_SPECIFIED, item, "SELECT * not allowed from relation that has no columns");
                    }

                    for (Field field : fields) {
                        int fieldIndex = relationType.indexOf(field);
                        FieldReference expression = new FieldReference(fieldIndex);
                        outputExpressionBuilder.add(expression);
                        ExpressionAnalysis expressionAnalysis = analyzeExpression(expression, scope);

                        Type type = expressionAnalysis.getType(expression);
                        if (node.getSelect().isDistinct() && !type.isComparable()) {
                            throw new SemanticException(TYPE_MISMATCH, node.getSelect(), "DISTINCT can only be applied to comparable types (actual: %s)", type);
                        }
                    }
                }
                else if (item instanceof SingleColumn) {
                    SingleColumn column = (SingleColumn) item;
                    ExpressionAnalysis expressionAnalysis = analyzeExpression(column.getExpression(), scope);
                    analysis.recordSubqueries(node, expressionAnalysis);
                    outputExpressionBuilder.add(column.getExpression());

                    Type type = expressionAnalysis.getType(column.getExpression());
                    if (node.getSelect().isDistinct() && !type.isComparable()) {
                        throw new SemanticException(TYPE_MISMATCH, node.getSelect(), "DISTINCT can only be applied to comparable types (actual: %s): %s", type, column.getExpression());
                    }
                }
                else {
                    throw new IllegalArgumentException("Unsupported SelectItem type: " + item.getClass().getName());
                }
            }

            ImmutableList<Expression> result = outputExpressionBuilder.build();
            analysis.setOutputExpressions(node, result);

            return result;
        }

        public void analyzeWhere(Node node, Scope scope, Expression predicate)
        {
            ExpressionAnalysis expressionAnalysis = analyzeExpression(predicate, scope);

            verifyNoAggregateWindowOrGroupingFunctions(analysis.getFunctionHandles(), metadata.getFunctionAndTypeManager(), predicate, "WHERE clause");

            analysis.recordSubqueries(node, expressionAnalysis);

            Type predicateType = expressionAnalysis.getType(predicate);
            if (!predicateType.equals(BOOLEAN)) {
                if (!predicateType.equals(UNKNOWN)) {
                    throw new SemanticException(TYPE_MISMATCH, predicate, "WHERE clause must evaluate to a boolean: actual type %s", predicateType);
                }
                // coerce null to boolean
                analysis.addCoercion(predicate, BOOLEAN, false);
            }

            analysis.setWhere(node, predicate);
        }

        private Scope analyzeFrom(QuerySpecification node, Optional<Scope> scope)
        {
            if (node.getFrom().isPresent()) {
                return process(node.getFrom().get(), scope);
            }

            return createScope(scope);
        }

        private void analyzeGroupingOperations(QuerySpecification node, List<Expression> outputExpressions, List<Expression> orderByExpressions)
        {
            List<GroupingOperation> groupingOperations = extractExpressions(Iterables.concat(outputExpressions, orderByExpressions), GroupingOperation.class);
            boolean isGroupingOperationPresent = !groupingOperations.isEmpty();

            if (isGroupingOperationPresent && !node.getGroupBy().isPresent()) {
                throw new SemanticException(
                        INVALID_PROCEDURE_ARGUMENTS,
                        node,
                        "A GROUPING() operation can only be used with a corresponding GROUPING SET/CUBE/ROLLUP/GROUP BY clause");
            }

            analysis.setGroupingOperations(node, groupingOperations);
        }

        private List<FunctionCall> analyzeAggregations(
                QuerySpecification node,
                List<Expression> outputExpressions,
                List<Expression> orderByExpressions)
        {
            List<FunctionCall> aggregates = extractAggregateFunctions(analysis.getFunctionHandles(), Iterables.concat(outputExpressions, orderByExpressions), metadata.getFunctionAndTypeManager());
            analysis.setAggregates(node, aggregates);
            return aggregates;
        }

        private void verifyAggregations(
                QuerySpecification node,
                Scope sourceScope,
                Optional<Scope> orderByScope,
                List<Expression> groupByExpressions,
                List<Expression> outputExpressions,
                List<Expression> orderByExpressions)
        {
            checkState(orderByExpressions.isEmpty() || orderByScope.isPresent(), "non-empty orderByExpressions list without orderByScope provided");

            if (analysis.isAggregation(node)) {
                // ensure SELECT, ORDER BY and HAVING are constant with respect to group
                // e.g, these are all valid expressions:
                //     SELECT f(a) GROUP BY a
                //     SELECT f(a + 1) GROUP BY a + 1
                //     SELECT a + sum(b) GROUP BY a
                List<Expression> distinctGroupingColumns = groupByExpressions.stream()
                        .distinct()
                        .collect(toImmutableList());

                for (Expression expression : outputExpressions) {
                    verifySourceAggregations(distinctGroupingColumns, sourceScope, expression, metadata, analysis, warningCollector);
                }

                for (Expression expression : orderByExpressions) {
                    verifyOrderByAggregations(distinctGroupingColumns, sourceScope, orderByScope.get(), expression, metadata, analysis, warningCollector);
                }
            }
        }

        private RelationType analyzeView(Query query, QualifiedObjectName name, Optional<String> catalog, Optional<String> schema, Optional<String> owner, Table node)
        {
            try {
                // run view as view owner if set; otherwise, run as session user
                Identity identity;
                AccessControl viewAccessControl;
                if (owner.isPresent() && !owner.get().equals(session.getIdentity().getUser())) {
                    identity = new Identity(owner.get(), Optional.empty());
                    viewAccessControl = new ViewAccessControl(accessControl);
                }
                else {
                    identity = session.getIdentity();
                    viewAccessControl = accessControl;
                }

                Session viewSession = Session.builder(metadata.getSessionPropertyManager())
                        .setQueryId(session.getQueryId())
                        .setTransactionId(session.getTransactionId().orElse(null))
                        .setIdentity(identity)
                        .setSource(session.getSource().orElse(null))
                        .setCatalog(catalog.orElse(null))
                        .setSchema(schema.orElse(null))
                        .setTimeZoneKey(session.getTimeZoneKey())
                        .setLocale(session.getLocale())
                        .setRemoteUserAddress(session.getRemoteUserAddress().orElse(null))
                        .setUserAgent(session.getUserAgent().orElse(null))
                        .setClientInfo(session.getClientInfo().orElse(null))
                        .setStartTime(session.getStartTime())
                        .build();

                StatementAnalyzer analyzer = new StatementAnalyzer(analysis, metadata, sqlParser, viewAccessControl, viewSession, warningCollector);
                Scope queryScope = analyzer.analyze(query, Scope.create());
                return queryScope.getRelationType().withAlias(name.getObjectName(), null);
            }
            catch (RuntimeException e) {
                throwIfInstanceOf(e, PrestoException.class);
                throw new SemanticException(VIEW_ANALYSIS_ERROR, node, "Failed analyzing stored view '%s': %s", name, e.getMessage());
            }
        }

        private Query parseView(String view, QualifiedObjectName name, Node node)
        {
            try {
                return (Query) sqlParser.createStatement(view, createParsingOptions(session, warningCollector));
            }
            catch (ParsingException e) {
                throw new SemanticException(VIEW_PARSE_ERROR, node, "Failed parsing stored view '%s': %s", name, e.getMessage());
            }
        }

        private boolean isViewStale(List<ViewDefinition.ViewColumn> columns, Collection<Field> fields)
        {
            if (columns.size() != fields.size()) {
                return true;
            }

            List<Field> fieldList = ImmutableList.copyOf(fields);
            for (int i = 0; i < columns.size(); i++) {
                ViewDefinition.ViewColumn column = columns.get(i);
                Field field = fieldList.get(i);
                if (!column.getName().equalsIgnoreCase(field.getName().orElse(null)) ||
                        !metadata.getFunctionAndTypeManager().canCoerce(field.getType(), column.getType())) {
                    return true;
                }
            }

            return false;
        }

        private ExpressionAnalysis analyzeExpression(Expression expression, Scope scope)
        {
            return ExpressionAnalyzer.analyzeExpression(
                    session,
                    metadata,
                    accessControl,
                    sqlParser,
                    scope,
                    analysis,
                    expression,
                    warningCollector);
        }

        private List<Expression> descriptorToFields(Scope scope)
        {
            ImmutableList.Builder<Expression> builder = ImmutableList.builder();
            for (int fieldIndex = 0; fieldIndex < scope.getRelationType().getAllFieldCount(); fieldIndex++) {
                FieldReference expression = new FieldReference(fieldIndex);
                builder.add(expression);
                analyzeExpression(expression, scope);
            }
            return builder.build();
        }

        private Scope analyzeWith(Query node, Optional<Scope> scope)
        {
            // analyze WITH clause
            if (!node.getWith().isPresent()) {
                return createScope(scope);
            }
            With with = node.getWith().get();
            if (with.isRecursive()) {
                throw new SemanticException(NOT_SUPPORTED, with, "Recursive WITH queries are not supported");
            }

            Scope.Builder withScopeBuilder = scopeBuilder(scope);
            for (WithQuery withQuery : with.getQueries()) {
                Query query = withQuery.getQuery();
                process(query, withScopeBuilder.build());

                String name = withQuery.getName().getValue().toLowerCase(ENGLISH);
                if (withScopeBuilder.containsNamedQuery(name)) {
                    throw new SemanticException(DUPLICATE_RELATION, withQuery, "WITH query name '%s' specified more than once", name);
                }

                // check if all or none of the columns are explicitly alias
                if (withQuery.getColumnNames().isPresent()) {
                    List<Identifier> columnNames = withQuery.getColumnNames().get();
                    RelationType queryDescriptor = analysis.getOutputDescriptor(query);
                    if (columnNames.size() != queryDescriptor.getVisibleFieldCount()) {
                        throw new SemanticException(MISMATCHED_COLUMN_ALIASES, withQuery, "WITH column alias list has %s entries but WITH query(%s) has %s columns", columnNames.size(), name, queryDescriptor.getVisibleFieldCount());
                    }
                }

                withScopeBuilder.withNamedQuery(name, withQuery);
            }

            Scope withScope = withScopeBuilder.build();
            analysis.setScope(with, withScope);
            return withScope;
        }

        private void verifySelectDistinct(QuerySpecification node, List<Expression> outputExpressions)
        {
            for (SortItem item : node.getOrderBy().get().getSortItems()) {
                Expression expression = item.getSortKey();

                if (expression instanceof LongLiteral) {
                    continue;
                }

                Expression rewrittenOrderByExpression = ExpressionTreeRewriter.rewriteWith(new OrderByExpressionRewriter(extractNamedOutputExpressions(node.getSelect())), expression);
                int index = outputExpressions.indexOf(rewrittenOrderByExpression);
                if (index == -1) {
                    throw new SemanticException(ORDER_BY_MUST_BE_IN_SELECT, node.getSelect(), "For SELECT DISTINCT, ORDER BY expressions must appear in select list");
                }

                if (!isDeterministic(expression)) {
                    throw new SemanticException(NONDETERMINISTIC_ORDER_BY_EXPRESSION_WITH_SELECT_DISTINCT, expression, "Non deterministic ORDER BY expression is not supported with SELECT DISTINCT");
                }
            }
        }

        private List<Expression> analyzeOrderBy(Node node, List<SortItem> sortItems, Scope orderByScope)
        {
            ImmutableList.Builder<Expression> orderByFieldsBuilder = ImmutableList.builder();

            for (SortItem item : sortItems) {
                Expression expression = item.getSortKey();

                if (expression instanceof LongLiteral) {
                    // this is an ordinal in the output tuple

                    long ordinal = ((LongLiteral) expression).getValue();
                    if (ordinal < 1 || ordinal > orderByScope.getRelationType().getVisibleFieldCount()) {
                        throw new SemanticException(INVALID_ORDINAL, expression, "ORDER BY position %s is not in select list", ordinal);
                    }

                    expression = new FieldReference(toIntExact(ordinal - 1));
                }

                ExpressionAnalysis expressionAnalysis = analyzeExpression(expression, orderByScope);
                analysis.recordSubqueries(node, expressionAnalysis);

                Type type = analysis.getType(expression);
                if (!type.isOrderable()) {
                    throw new SemanticException(TYPE_MISMATCH, node, "Type %s is not orderable, and therefore cannot be used in ORDER BY: %s", type, expression);
                }

                orderByFieldsBuilder.add(expression);
            }

            List<Expression> orderByFields = orderByFieldsBuilder.build();
            return orderByFields;
        }

        private Scope createAndAssignScope(Node node, Optional<Scope> parentScope)
        {
            return createAndAssignScope(node, parentScope, emptyList());
        }

        private Scope createAndAssignScope(Node node, Optional<Scope> parentScope, Field... fields)
        {
            return createAndAssignScope(node, parentScope, new RelationType(fields));
        }

        private Scope createAndAssignScope(Node node, Optional<Scope> parentScope, List<Field> fields)
        {
            return createAndAssignScope(node, parentScope, new RelationType(fields));
        }

        private Scope createAndAssignScope(Node node, Optional<Scope> parentScope, RelationType relationType)
        {
            Scope scope = scopeBuilder(parentScope)
                    .withRelationType(RelationId.of(node), relationType)
                    .build();

            analysis.setScope(node, scope);
            return scope;
        }

        private Scope createScope(Optional<Scope> parentScope)
        {
            return scopeBuilder(parentScope).build();
        }

        private Scope.Builder scopeBuilder(Optional<Scope> parentScope)
        {
            Scope.Builder scopeBuilder = Scope.builder();

            if (parentScope.isPresent()) {
                // parent scope represents local query scope hierarchy. Local query scope
                // hierarchy should have outer query scope as ancestor already.
                scopeBuilder.withParent(parentScope.get());
            }
            else if (outerQueryScope.isPresent()) {
                scopeBuilder.withOuterQueryParent(outerQueryScope.get());
            }

            return scopeBuilder;
        }
    }

    private static boolean hasScopeAsLocalParent(Scope root, Scope parent)
    {
        Scope scope = root;
        while (scope.getLocalParent().isPresent()) {
            scope = scope.getLocalParent().get();
            if (scope.equals(parent)) {
                return true;
            }
        }

        return false;
    }
}
