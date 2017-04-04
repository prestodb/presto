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
import com.facebook.presto.metadata.FunctionKind;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.metadata.QualifiedObjectName;
import com.facebook.presto.metadata.TableHandle;
import com.facebook.presto.metadata.TableMetadata;
import com.facebook.presto.metadata.ViewDefinition;
import com.facebook.presto.security.AccessControl;
import com.facebook.presto.security.AllowAllAccessControl;
import com.facebook.presto.security.ViewAccessControl;
import com.facebook.presto.spi.CatalogSchemaName;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.security.Identity;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeSignature;
import com.facebook.presto.sql.ExpressionUtils;
import com.facebook.presto.sql.parser.ParsingException;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.planner.DependencyExtractor;
import com.facebook.presto.sql.planner.ExpressionInterpreter;
import com.facebook.presto.sql.tree.AddColumn;
import com.facebook.presto.sql.tree.AliasedRelation;
import com.facebook.presto.sql.tree.AllColumns;
import com.facebook.presto.sql.tree.Call;
import com.facebook.presto.sql.tree.Commit;
import com.facebook.presto.sql.tree.ComparisonExpression;
import com.facebook.presto.sql.tree.CreateSchema;
import com.facebook.presto.sql.tree.CreateTable;
import com.facebook.presto.sql.tree.CreateTableAsSelect;
import com.facebook.presto.sql.tree.CreateView;
import com.facebook.presto.sql.tree.Deallocate;
import com.facebook.presto.sql.tree.DefaultTraversalVisitor;
import com.facebook.presto.sql.tree.Delete;
import com.facebook.presto.sql.tree.DereferenceExpression;
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
import com.facebook.presto.sql.tree.GroupingElement;
import com.facebook.presto.sql.tree.Identifier;
import com.facebook.presto.sql.tree.Insert;
import com.facebook.presto.sql.tree.Intersect;
import com.facebook.presto.sql.tree.Join;
import com.facebook.presto.sql.tree.JoinCriteria;
import com.facebook.presto.sql.tree.JoinOn;
import com.facebook.presto.sql.tree.JoinUsing;
import com.facebook.presto.sql.tree.LongLiteral;
import com.facebook.presto.sql.tree.NaturalJoin;
import com.facebook.presto.sql.tree.Node;
import com.facebook.presto.sql.tree.OrderBy;
import com.facebook.presto.sql.tree.Prepare;
import com.facebook.presto.sql.tree.QualifiedName;
import com.facebook.presto.sql.tree.Query;
import com.facebook.presto.sql.tree.QuerySpecification;
import com.facebook.presto.sql.tree.Relation;
import com.facebook.presto.sql.tree.RenameColumn;
import com.facebook.presto.sql.tree.RenameSchema;
import com.facebook.presto.sql.tree.RenameTable;
import com.facebook.presto.sql.tree.ResetSession;
import com.facebook.presto.sql.tree.Revoke;
import com.facebook.presto.sql.tree.Rollback;
import com.facebook.presto.sql.tree.Row;
import com.facebook.presto.sql.tree.SampledRelation;
import com.facebook.presto.sql.tree.Select;
import com.facebook.presto.sql.tree.SelectItem;
import com.facebook.presto.sql.tree.SetOperation;
import com.facebook.presto.sql.tree.SetSession;
import com.facebook.presto.sql.tree.SingleColumn;
import com.facebook.presto.sql.tree.SortItem;
import com.facebook.presto.sql.tree.StartTransaction;
import com.facebook.presto.sql.tree.Statement;
import com.facebook.presto.sql.tree.Table;
import com.facebook.presto.sql.tree.TableSubquery;
import com.facebook.presto.sql.tree.Unnest;
import com.facebook.presto.sql.tree.Use;
import com.facebook.presto.sql.tree.Values;
import com.facebook.presto.sql.tree.Window;
import com.facebook.presto.sql.tree.WindowFrame;
import com.facebook.presto.sql.tree.With;
import com.facebook.presto.sql.tree.WithQuery;
import com.facebook.presto.sql.util.AstUtils;
import com.facebook.presto.type.ArrayType;
import com.facebook.presto.type.MapType;
import com.facebook.presto.type.RowType;
import com.facebook.presto.util.maps.IdentityLinkedHashMap;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static com.facebook.presto.SystemSessionProperties.LEGACY_ORDER_BY;
import static com.facebook.presto.metadata.FunctionKind.AGGREGATE;
import static com.facebook.presto.metadata.FunctionKind.WINDOW;
import static com.facebook.presto.metadata.MetadataUtil.createQualifiedObjectName;
import static com.facebook.presto.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.facebook.presto.sql.NodeUtils.getSortItemsFromOrderBy;
import static com.facebook.presto.sql.analyzer.AggregationAnalyzer.verifyOrderByAggregations;
import static com.facebook.presto.sql.analyzer.AggregationAnalyzer.verifySourceAggregations;
import static com.facebook.presto.sql.analyzer.ExpressionAnalyzer.createConstantAnalyzer;
import static com.facebook.presto.sql.analyzer.ExpressionAnalyzer.getExpressionTypes;
import static com.facebook.presto.sql.analyzer.ScopeReferenceExtractor.hasReferencesToScope;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.AMBIGUOUS_ATTRIBUTE;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.COLUMN_NAME_NOT_SPECIFIED;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.COLUMN_TYPE_UNKNOWN;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.DUPLICATE_COLUMN_NAME;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.DUPLICATE_RELATION;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.INVALID_ORDINAL;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.INVALID_WINDOW_FRAME;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.MISMATCHED_COLUMN_ALIASES;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.MISMATCHED_SET_COLUMN_TYPES;
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
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.TYPE_MISMATCH;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.VIEW_ANALYSIS_ERROR;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.VIEW_IS_RECURSIVE;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.VIEW_IS_STALE;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.VIEW_PARSE_ERROR;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.WILDCARD_WITHOUT_FROM;
import static com.facebook.presto.sql.analyzer.TypeSignatureProvider.fromTypeSignatures;
import static com.facebook.presto.sql.planner.DeterminismEvaluator.isDeterministic;
import static com.facebook.presto.sql.planner.ExpressionInterpreter.expressionOptimizer;
import static com.facebook.presto.sql.tree.ComparisonExpressionType.EQUAL;
import static com.facebook.presto.sql.tree.ExplainType.Type.DISTRIBUTED;
import static com.facebook.presto.sql.tree.FrameBound.Type.CURRENT_ROW;
import static com.facebook.presto.sql.tree.FrameBound.Type.FOLLOWING;
import static com.facebook.presto.sql.tree.FrameBound.Type.PRECEDING;
import static com.facebook.presto.sql.tree.FrameBound.Type.UNBOUNDED_FOLLOWING;
import static com.facebook.presto.sql.tree.FrameBound.Type.UNBOUNDED_PRECEDING;
import static com.facebook.presto.sql.tree.WindowFrame.Type.RANGE;
import static com.facebook.presto.type.UnknownType.UNKNOWN;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.Iterables.getLast;
import static com.google.common.collect.Iterables.transform;
import static java.lang.Math.toIntExact;
import static java.util.Collections.emptyList;
import static java.util.Objects.requireNonNull;

class StatementAnalyzer
{
    private final Analysis analysis;
    private final Metadata metadata;
    private final Session session;
    private final SqlParser sqlParser;
    private final AccessControl accessControl;

    public StatementAnalyzer(
            Analysis analysis,
            Metadata metadata,
            SqlParser sqlParser,
            AccessControl accessControl,
            Session session)
    {
        this.analysis = requireNonNull(analysis, "analysis is null");
        this.metadata = requireNonNull(metadata, "metadata is null");
        this.sqlParser = requireNonNull(sqlParser, "sqlParser is null");
        this.accessControl = requireNonNull(accessControl, "accessControl is null");
        this.session = requireNonNull(session, "session is null");
    }

    public Scope analyze(Node node, Scope outerQueryScope)
    {
        return analyze(node, Optional.of(outerQueryScope));
    }

    public Scope analyze(Node node, Optional<Scope> outerQueryScope)
    {
        return new Visitor(outerQueryScope).process(node, Optional.empty());
    }

    private void analyzeWhere(Node node, Scope outerQueryScope, Expression predicate)
    {
        Visitor visitor = new Visitor(Optional.of(outerQueryScope));
        visitor.analyzeWhere(node, outerQueryScope, predicate);
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

        private Visitor(Optional<Scope> outerQueryScope)
        {
            this.outerQueryScope = requireNonNull(outerQueryScope, "outerQueryScope is null");
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
            accessControl.checkCanInsertIntoTable(session.getRequiredTransactionId(), session.getIdentity(), targetTable);

            TableMetadata tableMetadata = metadata.getTableMetadata(session, targetTableHandle.get());
            List<String> tableColumns = tableMetadata.getColumns().stream()
                    .filter(column -> !column.isHidden())
                    .map(ColumnMetadata::getName)
                    .collect(toImmutableList());

            List<String> insertColumns;
            if (insert.getColumns().isPresent()) {
                insertColumns = insert.getColumns().get().stream()
                        .map(String::toLowerCase)
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

            Map<String, ColumnHandle> columnHandles = metadata.getColumnHandles(session, targetTableHandle.get());
            analysis.setInsert(new Analysis.Insert(
                    targetTableHandle.get(),
                    insertColumns.stream().map(columnHandles::get).collect(toImmutableList())));

            Iterable<Type> tableTypes = insertColumns.stream()
                    .map(insertColumn -> tableMetadata.getColumn(insertColumn).getType())
                    .collect(toImmutableList());

            Iterable<Type> queryTypes = transform(queryScope.getRelationType().getVisibleFields(), Field::getType);

            if (!typesMatchForInsert(tableTypes, queryTypes)) {
                throw new SemanticException(MISMATCHED_SET_COLUMN_TYPES, insert, "Insert query has mismatched column types: " +
                        "Table: [" + Joiner.on(", ").join(tableTypes) + "], " +
                        "Query: [" + Joiner.on(", ").join(queryTypes) + "]");
            }

            return createAndAssignScope(insert, scope, Field.newUnqualified("rows", BIGINT));
        }

        private boolean typesMatchForInsert(Iterable<Type> tableTypes, Iterable<Type> queryTypes)
        {
            if (Iterables.size(tableTypes) != Iterables.size(queryTypes)) {
                return false;
            }

            Iterator<Type> tableTypesIterator = tableTypes.iterator();
            Iterator<Type> queryTypesIterator = queryTypes.iterator();
            while (tableTypesIterator.hasNext()) {
                Type tableType = tableTypesIterator.next();
                Type queryType = queryTypesIterator.next();

                if (!metadata.getTypeManager().canCoerce(queryType, tableType)) {
                    return false;
                }
            }

            return true;
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
                    session);

            Scope tableScope = analyzer.analyze(table, scope);
            node.getWhere().ifPresent(where -> analyzer.analyzeWhere(node, tableScope, where));

            analysis.setUpdateType("DELETE");

            accessControl.checkCanDeleteFromTable(session.getRequiredTransactionId(), session.getIdentity(), tableName);

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

            for (Expression expression : node.getProperties().values()) {
                // analyze table property value expressions which must be constant
                createConstantAnalyzer(metadata, session, analysis.getParameters(), analysis.isDescribe())
                        .analyze(expression, createScope(scope));
            }
            analysis.setCreateTableProperties(node.getProperties());
            analysis.setCreateTableComment(node.getComment());

            accessControl.checkCanCreateTable(session.getRequiredTransactionId(), session.getIdentity(), targetTable);

            analysis.setCreateTableAsSelectWithData(node.isWithData());

            // analyze the query that creates the table
            Scope queryScope = process(node.getQuery(), scope);

            validateColumns(node, queryScope.getRelationType());

            return createAndAssignScope(node, scope, Field.newUnqualified("rows", BIGINT));
        }

        @Override
        protected Scope visitCreateView(CreateView node, Optional<Scope> scope)
        {
            analysis.setUpdateType("CREATE VIEW");

            QualifiedObjectName viewName = createQualifiedObjectName(session, node, node.getName());

            // analyze the query that creates the view
            StatementAnalyzer analyzer = new StatementAnalyzer(
                    analysis,
                    metadata,
                    sqlParser,
                    new ViewAccessControl(accessControl),
                    session);

            Scope queryScope = analyzer.analyze(node.getQuery(), scope);

            accessControl.checkCanCreateView(session.getRequiredTransactionId(), session.getIdentity(), viewName);

            validateColumns(node, queryScope.getRelationType());

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
            if (node.getOrderBy().isPresent()) {
                analyzeOrderBy(node, queryBodyScope);
            }
            else {
                analysis.setOrderByExpressions(node, emptyList());
            }

            // Input fields == Output fields
            analysis.setOutputExpressions(node, descriptorToFields(queryBodyScope));

            Scope queryScope = Scope.builder()
                    .withParent(withScope)
                    .withRelationType(queryBodyScope.getRelationType())
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
                    outputFields.add(Field.newUnqualified(Optional.empty(), ((ArrayType) expressionType).getElementType()));
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
                    Optional<List<String>> columnNames = withQuery.get().getColumnNames();
                    if (columnNames.isPresent()) {
                        // if columns are explicitly aliased -> WITH cte(alias1, alias2 ...)
                        ImmutableList.Builder<Field> fieldBuilder = ImmutableList.builder();

                        int field = 0;
                        for (String columnName : columnNames.get()) {
                            Field inputField = queryDescriptor.getFieldByIndex(field);
                            fieldBuilder.add(Field.newQualified(
                                    QualifiedName.of(name),
                                    Optional.of(columnName),
                                    inputField.getType(),
                                    false,
                                    inputField.getOriginTable(),
                                    inputField.isAliased()));

                            field++;
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
                                        field.isAliased()))
                                .collect(toImmutableList());
                    }

                    return createAndAssignScope(table, scope, fields);
                }
            }

            QualifiedObjectName name = createQualifiedObjectName(session, table, table.getName());

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

                accessControl.checkCanSelectFromView(session.getRequiredTransactionId(), session.getIdentity(), name);

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
                                QualifiedName.of(name.getObjectName()),
                                Optional.of(column.getName()),
                                column.getType(),
                                false,
                                Optional.of(name),
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
            accessControl.checkCanSelectFromTable(session.getRequiredTransactionId(), session.getIdentity(), name);
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

            RelationType descriptor = relationType.withAlias(relation.getAlias(), relation.getColumnNames());
            return createAndAssignScope(relation, scope, descriptor);
        }

        @Override
        protected Scope visitSampledRelation(SampledRelation relation, Optional<Scope> scope)
        {
            if (!DependencyExtractor.extractNames(relation.getSamplePercentage(), analysis.getColumnReferences()).isEmpty()) {
                throw new SemanticException(NON_NUMERIC_SAMPLE_PERCENTAGE, relation.getSamplePercentage(), "Sample percentage cannot contain column references");
            }

            IdentityLinkedHashMap<Expression, Type> expressionTypes = getExpressionTypes(
                    session,
                    metadata,
                    sqlParser,
                    ImmutableMap.of(),
                    relation.getSamplePercentage(),
                    analysis.getParameters(),
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
            StatementAnalyzer analyzer = new StatementAnalyzer(analysis, metadata, sqlParser, accessControl, session);
            Scope queryScope = analyzer.analyze(node.getQuery(), scope);
            return createAndAssignScope(node, scope, queryScope.getRelationType());
        }

        @Override
        protected Scope visitQuerySpecification(QuerySpecification node, Optional<Scope> scope)
        {
            if (SystemSessionProperties.isLegacyOrderByEnabled(session)) {
                return legacyVisitQuerySpecification(node, scope);
            }

            // TODO: extract candidate names from SELECT, WHERE, HAVING, GROUP BY and ORDER BY expressions
            // to pass down to analyzeFrom

            Scope sourceScope = analyzeFrom(node, scope);

            node.getWhere().ifPresent(where -> analyzeWhere(node, sourceScope, where));

            List<Expression> outputExpressions = analyzeSelect(node, sourceScope);
            List<List<Expression>> groupByExpressions = analyzeGroupBy(node, sourceScope, outputExpressions);
            analyzeHaving(node, sourceScope);

            Scope outputScope = computeAndAssignOutputScope(node, scope, sourceScope);

            List<Expression> orderByExpressions = emptyList();
            Optional<Scope> orderByScope = Optional.empty();
            if (node.getOrderBy().isPresent()) {
                orderByScope = Optional.of(computeAndAssignOrderByScope(node.getOrderBy().get(), sourceScope, outputScope));
                orderByExpressions = analyzeOrderBy(node, orderByScope.get(), outputExpressions);
            }
            else {
                analysis.setOrderByExpressions(node, emptyList());
            }

            List<Expression> sourceExpressions = new ArrayList<>();
            sourceExpressions.addAll(outputExpressions);
            node.getHaving().ifPresent(sourceExpressions::add);

            List<FunctionCall> aggregations = analyzeAggregations(node, sourceScope, orderByScope, groupByExpressions, analysis.getColumnReferences(), sourceExpressions, orderByExpressions);
            analyzeWindowFunctions(node, outputExpressions, orderByExpressions);

            if (!groupByExpressions.isEmpty() && node.getOrderBy().isPresent()) {
                // Create a different scope for ORDER BY expressions when aggregation is present.
                // This is because planner requires scope in order to resolve names against fields.
                // Original ORDER BY scope "sees" FROM query fields. However, during planning
                // and when aggregation is present, ORDER BY expressions should only be resolvable against
                // output scope, group by expressions and aggregation expressions.
                computeAndAssignOrderByScopeWithAggregation(node.getOrderBy().get(), outputScope, aggregations, groupByExpressions);
            }

            return outputScope;
        }

        private Scope legacyVisitQuerySpecification(QuerySpecification node, Optional<Scope> scope)
        {
            // TODO: extract candidate names from SELECT, WHERE, HAVING, GROUP BY and ORDER BY expressions
            // to pass down to analyzeFrom

            Scope sourceScope = analyzeFrom(node, scope);

            node.getWhere().ifPresent(where -> analyzeWhere(node, sourceScope, where));

            List<Expression> outputExpressions = analyzeSelect(node, sourceScope);
            List<List<Expression>> groupByExpressions = analyzeGroupBy(node, sourceScope, outputExpressions);

            Scope outputScope = computeAndAssignOutputScope(node, scope, sourceScope);

            List<Expression> orderByExpressions = emptyList();
            if (node.getOrderBy().isPresent()) {
                Scope orderByScope = computeAndAssignOrderByScope(node.getOrderBy().get(), sourceScope, outputScope);
                orderByExpressions = legacyAnalyzeOrderBy(node, sourceScope, orderByScope, outputExpressions);
            }

            analysis.setOrderByExpressions(node, orderByExpressions);

            analyzeHaving(node, sourceScope);

            List<Expression> expressions = new ArrayList<>();
            expressions.addAll(outputExpressions);
            expressions.addAll(orderByExpressions);
            node.getHaving().ifPresent(expressions::add);

            analyzeAggregations(node, sourceScope, Optional.empty(), groupByExpressions, analysis.getColumnReferences(), expressions, emptyList());
            analysis.setWindowFunctions(node, analyzeWindowFunctions(node, expressions));

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
            for (Scope relationScope : relationScopes) {
                int outputFieldSize = outputFieldTypes.length;
                RelationType relationType = relationScope.getRelationType();
                int descFieldSize = relationType.getVisibleFields().size();
                String setOperationName = node.getClass().getSimpleName();
                if (outputFieldSize != descFieldSize) {
                    throw new SemanticException(MISMATCHED_SET_COLUMN_TYPES,
                            node,
                            "%s query has different number of fields: %d, %d",
                            setOperationName, outputFieldSize, descFieldSize);
                }
                for (int i = 0; i < descFieldSize; i++) {
                    Type descFieldType = relationType.getFieldByIndex(i).getType();
                    Optional<Type> commonSuperType = metadata.getTypeManager().getCommonSuperType(outputFieldTypes[i], descFieldType);
                    if (!commonSuperType.isPresent()) {
                        throw new SemanticException(TYPE_MISMATCH,
                                node,
                                "column %d in %s query has incompatible types: %s, %s",
                                i, outputFieldTypes[i].getDisplayName(), setOperationName, descFieldType.getDisplayName());
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

        @Override
        protected Scope visitIntersect(Intersect node, Optional<Scope> scope)
        {
            if (!node.isDistinct()) {
                throw new SemanticException(NOT_SUPPORTED, node, "INTERSECT ALL not yet implemented");
            }

            return visitSetOperation(node, scope);
        }

        @Override
        protected Scope visitExcept(Except node, Optional<Scope> scope)
        {
            if (!node.isDistinct()) {
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
            Scope right = process(node.getRight(), isUnnestRelation(node.getRight()) ? Optional.of(left) : scope);

            Scope output = createAndAssignScope(node, scope, left.getRelationType().joinWith(right.getRelationType()));

            if (node.getType() == Join.Type.CROSS || node.getType() == Join.Type.IMPLICIT) {
                return output;
            }

            if (criteria instanceof JoinUsing) {
                // TODO: implement proper "using" semantics with respect to output columns
                List<String> columns = ((JoinUsing) criteria).getColumns();

                List<Expression> expressions = new ArrayList<>();
                for (String column : columns) {
                    Expression leftExpression = new Identifier(column);
                    Expression rightExpression = new Identifier(column);

                    ExpressionAnalysis leftExpressionAnalysis = analyzeExpression(leftExpression, left);
                    ExpressionAnalysis rightExpressionAnalysis = analyzeExpression(rightExpression, right);
                    checkState(leftExpressionAnalysis.getSubqueryInPredicates().isEmpty(), "INVARIANT");
                    checkState(rightExpressionAnalysis.getSubqueryInPredicates().isEmpty(), "INVARIANT");
                    checkState(leftExpressionAnalysis.getScalarSubqueries().isEmpty(), "INVARIANT");
                    checkState(rightExpressionAnalysis.getScalarSubqueries().isEmpty(), "INVARIANT");

                    addCoercionForJoinCriteria(node, leftExpression, rightExpression);
                    expressions.add(new ComparisonExpression(EQUAL, leftExpression, rightExpression));
                }

                analysis.setJoinCriteria(node, ExpressionUtils.and(expressions));
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

                Analyzer.verifyNoAggregatesOrWindowFunctions(metadata.getFunctionRegistry(), expression, "JOIN clause");

                analysis.recordSubqueries(node, expressionAnalysis);
                analysis.setJoinCriteria(node, expression);
            }
            else {
                throw new UnsupportedOperationException("unsupported join criteria: " + criteria.getClass().getName());
            }

            return output;
        }

        private boolean isUnnestRelation(Relation node)
        {
            if (node instanceof AliasedRelation) {
                return isUnnestRelation(((AliasedRelation) node).getRelation());
            }
            return node instanceof Unnest;
        }

        private void addCoercionForJoinCriteria(Join node, Expression leftExpression, Expression rightExpression)
        {
            Type leftType = analysis.getTypeWithCoercions(leftExpression);
            Type rightType = analysis.getTypeWithCoercions(rightExpression);
            Optional<Type> superType = metadata.getTypeManager().getCommonSuperType(leftType, rightType);
            if (!superType.isPresent()) {
                throw new SemanticException(TYPE_MISMATCH, node, "Join criteria has incompatible types: %s, %s", leftType.getDisplayName(), rightType.getDisplayName());
            }
            if (!leftType.equals(superType.get())) {
                analysis.addCoercion(leftExpression, superType.get(), metadata.getTypeManager().isTypeOnlyCoercion(leftType, rightType));
            }
            if (!rightType.equals(superType.get())) {
                analysis.addCoercion(rightExpression, superType.get(), metadata.getTypeManager().isTypeOnlyCoercion(rightType, leftType));
            }
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

                    Optional<Type> commonSuperType = metadata.getTypeManager().getCommonSuperType(fieldType, superType);
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
                            analysis.addCoercion(item, expectedType, metadata.getTypeManager().isTypeOnlyCoercion(actualType, expectedType));
                        }
                    }
                }
                else {
                    Type actualType = analysis.getType(row);
                    Type expectedType = fieldTypes.get(0);
                    if (!actualType.equals(expectedType)) {
                        analysis.addCoercion(row, expectedType, metadata.getTypeManager().isTypeOnlyCoercion(actualType, expectedType));
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
            WindowFunctionExtractor extractor = new WindowFunctionExtractor();

            for (Expression expression : expressions) {
                extractor.process(expression, null);
                new WindowFunctionValidator().process(expression, analysis);
            }

            List<FunctionCall> windowFunctions = extractor.getWindowFunctions();

            for (FunctionCall windowFunction : windowFunctions) {
                // filter with window function is not supported yet
                if (windowFunction.getFilter().isPresent()) {
                    throw new SemanticException(NOT_SUPPORTED, node, "FILTER is not yet supported for window functions");
                }

                Window window = windowFunction.getWindow().get();

                WindowFunctionExtractor nestedExtractor = new WindowFunctionExtractor();
                for (Expression argument : windowFunction.getArguments()) {
                    nestedExtractor.process(argument, null);
                }

                for (Expression expression : window.getPartitionBy()) {
                    nestedExtractor.process(expression, null);
                }

                if (window.getOrderBy().isPresent()) {
                    nestedExtractor.process(window.getOrderBy().get(), null);
                }

                if (window.getFrame().isPresent()) {
                    nestedExtractor.process(window.getFrame().get(), null);
                }

                if (!nestedExtractor.getWindowFunctions().isEmpty()) {
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

                List<TypeSignature> argumentTypes = Lists.transform(windowFunction.getArguments(), expression -> analysis.getType(expression).getTypeSignature());

                FunctionKind kind = metadata.getFunctionRegistry().resolveFunction(windowFunction.getName(), fromTypeSignatures(argumentTypes)).getKind();
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
                analysis.recordSubqueries(node, expressionAnalysis);

                Type predicateType = expressionAnalysis.getType(predicate);
                if (!predicateType.equals(BOOLEAN) && !predicateType.equals(UNKNOWN)) {
                    throw new SemanticException(TYPE_MISMATCH, predicate, "HAVING clause must evaluate to a boolean: actual type %s", predicateType);
                }

                analysis.setHaving(node, predicate);
            }
        }

        /**
         * Preserve the old column resolution behavior for ORDER BY while we transition workloads to new semantics
         * TODO: remove this
         */
        private List<Expression> legacyAnalyzeOrderBy(QuerySpecification node, Scope sourceScope, Scope orderByScope, List<Expression> outputExpressions)
        {
            List<SortItem> items = getSortItemsFromOrderBy(node.getOrderBy());

            ImmutableList.Builder<Expression> orderByExpressionsBuilder = ImmutableList.builder();

            if (!items.isEmpty()) {
                // Compute aliased output terms so we can resolve order by expressions against them first
                ImmutableMultimap.Builder<QualifiedName, Expression> byAliasBuilder = ImmutableMultimap.builder();
                for (SelectItem item : node.getSelect().getSelectItems()) {
                    if (item instanceof SingleColumn) {
                        Optional<String> alias = ((SingleColumn) item).getAlias();
                        if (alias.isPresent()) {
                            byAliasBuilder.put(QualifiedName.of(alias.get()), ((SingleColumn) item).getExpression()); // TODO: need to know if alias was quoted
                        }
                    }
                }
                Multimap<QualifiedName, Expression> byAlias = byAliasBuilder.build();

                for (SortItem item : items) {
                    Expression expression = item.getSortKey();

                    Expression orderByExpression = null;
                    if (expression instanceof Identifier) {
                        // if this is a simple name reference, try to resolve against output columns

                        QualifiedName name = QualifiedName.of(((Identifier) expression).getName());
                        Collection<Expression> expressions = byAlias.get(name);
                        if (expressions.size() > 1) {
                            throw new SemanticException(AMBIGUOUS_ATTRIBUTE, expression, "'%s' in ORDER BY is ambiguous", name.getSuffix());
                        }
                        if (expressions.size() == 1) {
                            orderByExpression = Iterables.getOnlyElement(expressions);
                        }

                        // otherwise, couldn't resolve name against output aliases, so fall through...
                    }
                    else if (expression instanceof LongLiteral) {
                        // this is an ordinal in the output tuple

                        long ordinal = ((LongLiteral) expression).getValue();
                        if (ordinal < 1 || ordinal > outputExpressions.size()) {
                            throw new SemanticException(INVALID_ORDINAL, expression, "ORDER BY position %s is not in select list", ordinal);
                        }

                        int field = toIntExact(ordinal - 1);
                        Type type = orderByScope.getRelationType().getFieldByIndex(field).getType();
                        if (!type.isOrderable()) {
                            throw new SemanticException(TYPE_MISMATCH, node, "The type of expression in position %s is not orderable (actual: %s), and therefore cannot be used in ORDER BY", ordinal, type);
                        }

                        orderByExpression = outputExpressions.get(field);
                    }

                    // otherwise, just use the expression as is
                    if (orderByExpression == null) {
                        orderByExpression = expression;
                    }

                    ExpressionAnalysis expressionAnalysis = analyzeExpression(orderByExpression, sourceScope);
                    analysis.recordSubqueries(node, expressionAnalysis);

                    Type type = expressionAnalysis.getType(orderByExpression);
                    if (!type.isOrderable()) {
                        throw new SemanticException(TYPE_MISMATCH, node, "Type %s is not orderable, and therefore cannot be used in ORDER BY: %s", type, expression);
                    }

                    orderByExpressionsBuilder.add(orderByExpression);
                }
            }

            List<Expression> orderByExpressions = orderByExpressionsBuilder.build();

            if (node.getSelect().isDistinct() && !outputExpressions.containsAll(orderByExpressions)) {
                throw new SemanticException(ORDER_BY_MUST_BE_IN_SELECT, node.getSelect(), "For SELECT DISTINCT, ORDER BY expressions must appear in select list");
            }
            return orderByExpressions;
        }

        private Multimap<QualifiedName, Expression> extractNamedOutputExpressions(Select node)
        {
            // Compute aliased output terms so we can resolve order by expressions against them first
            ImmutableMultimap.Builder<QualifiedName, Expression> assignments = ImmutableMultimap.builder();
            for (SelectItem item : node.getSelectItems()) {
                if (item instanceof SingleColumn) {
                    SingleColumn column = (SingleColumn) item;
                    Optional<String> alias = column.getAlias();
                    if (alias.isPresent()) {
                        assignments.put(QualifiedName.of(alias.get()), column.getExpression()); // TODO: need to know if alias was quoted
                    }
                    else if (column.getExpression() instanceof Identifier) {
                        assignments.put(QualifiedName.of(((Identifier) column.getExpression()).getName()), column.getExpression());
                    }
                }
            }

            return assignments.build();
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
                QualifiedName name = QualifiedName.of(reference.getName());
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

        private List<List<Expression>> analyzeGroupBy(QuerySpecification node, Scope scope, List<Expression> outputExpressions)
        {
            List<Set<Expression>> computedGroupingSets = ImmutableList.of(); // empty list = no aggregations

            if (node.getGroupBy().isPresent()) {
                List<List<Set<Expression>>> enumeratedGroupingSets = node.getGroupBy().get().getGroupingElements().stream()
                        .map(GroupingElement::enumerateGroupingSets)
                        .collect(toImmutableList());

                // compute cross product of enumerated grouping sets, if there are any
                computedGroupingSets = computeGroupingSetsCrossProduct(enumeratedGroupingSets, node.getGroupBy().get().isDistinct());
                checkState(!computedGroupingSets.isEmpty(), "computed grouping sets cannot be empty");
            }
            else if (hasAggregates(node)) {
                // if there are aggregates, but no group by, create a grand total grouping set (global aggregation)
                computedGroupingSets = ImmutableList.of(ImmutableSet.of());
            }

            List<List<Expression>> analyzedGroupingSets = computedGroupingSets.stream()
                    .map(groupingSet -> analyzeGroupingColumns(groupingSet, node, scope, outputExpressions))
                    .collect(toImmutableList());

            analysis.setGroupingSets(node, analyzedGroupingSets);
            return analyzedGroupingSets;
        }

        private List<Set<Expression>> computeGroupingSetsCrossProduct(List<List<Set<Expression>>> enumeratedGroupingSets, boolean isDistinct)
        {
            checkState(!enumeratedGroupingSets.isEmpty(), "enumeratedGroupingSets cannot be empty");

            List<Set<Expression>> groupingSetsCrossProduct = new ArrayList<>();
            enumeratedGroupingSets.get(0)
                    .stream()
                    .map(ImmutableSet::copyOf)
                    .forEach(groupingSetsCrossProduct::add);

            for (int i = 1; i < enumeratedGroupingSets.size(); i++) {
                List<Set<Expression>> groupingSets = enumeratedGroupingSets.get(i);
                List<Set<Expression>> oldGroupingSetsCrossProduct = ImmutableList.copyOf(groupingSetsCrossProduct);
                groupingSetsCrossProduct.clear();
                for (Set<Expression> existingSet : oldGroupingSetsCrossProduct) {
                    for (Set<Expression> groupingSet : groupingSets) {
                        Set<Expression> concatenatedSet = ImmutableSet.<Expression>builder()
                                .addAll(existingSet)
                                .addAll(groupingSet)
                                .build();
                        groupingSetsCrossProduct.add(concatenatedSet);
                    }
                }
            }

            if (isDistinct) {
                return ImmutableList.copyOf(ImmutableSet.copyOf(groupingSetsCrossProduct));
            }

            return groupingSetsCrossProduct;
        }

        private List<Expression> analyzeGroupingColumns(Set<Expression> groupingColumns, QuerySpecification node, Scope scope, List<Expression> outputExpressions)
        {
            ImmutableList.Builder<Expression> groupingColumnsBuilder = ImmutableList.builder();
            for (Expression groupingColumn : groupingColumns) {
                // first, see if this is an ordinal
                Expression groupByExpression;

                if (groupingColumn instanceof LongLiteral) {
                    long ordinal = ((LongLiteral) groupingColumn).getValue();
                    if (ordinal < 1 || ordinal > outputExpressions.size()) {
                        throw new SemanticException(INVALID_ORDINAL, groupingColumn, "GROUP BY position %s is not in select list", ordinal);
                    }

                    groupByExpression = outputExpressions.get(toIntExact(ordinal - 1));
                }
                else {
                    ExpressionAnalysis expressionAnalysis = analyzeExpression(groupingColumn, scope);
                    analysis.recordSubqueries(node, expressionAnalysis);
                    groupByExpression = groupingColumn;
                }

                Analyzer.verifyNoAggregatesOrWindowFunctions(metadata.getFunctionRegistry(), groupByExpression, "GROUP BY clause");
                Type type = analysis.getType(groupByExpression);
                if (!type.isComparable()) {
                    throw new SemanticException(TYPE_MISMATCH, node, "%s is not comparable, and therefore cannot be used in GROUP BY", type);
                }

                groupingColumnsBuilder.add(groupByExpression);
            }
            return groupingColumnsBuilder.build();
        }

        private Scope computeAndAssignOutputScope(QuerySpecification node, Optional<Scope> scope, Scope sourceScope)
        {
            ImmutableList.Builder<Field> outputFields = ImmutableList.builder();

            for (SelectItem item : node.getSelect().getSelectItems()) {
                if (item instanceof AllColumns) {
                    // expand * and T.*
                    Optional<QualifiedName> starPrefix = ((AllColumns) item).getPrefix();

                    for (Field field : sourceScope.getRelationType().resolveFieldsWithPrefix(starPrefix)) {
                        outputFields.add(Field.newUnqualified(field.getName(), field.getType(), field.getOriginTable(), false));
                    }
                }
                else if (item instanceof SingleColumn) {
                    SingleColumn column = (SingleColumn) item;

                    Expression expression = column.getExpression();
                    Optional<String> fieldName = column.getAlias();

                    Optional<QualifiedObjectName> originTable = Optional.empty();
                    QualifiedName name = null;

                    if (expression instanceof Identifier) {
                        name = QualifiedName.of(((Identifier) expression).getName());
                    }
                    else if (expression instanceof DereferenceExpression) {
                        name = DereferenceExpression.getQualifiedName((DereferenceExpression) expression);
                    }

                    if (name != null) {
                        List<Field> matchingFields = sourceScope.getRelationType().resolveFields(name);
                        if (!matchingFields.isEmpty()) {
                            originTable = matchingFields.get(0).getOriginTable();
                        }
                    }

                    if (!fieldName.isPresent()) {
                        if (name != null) {
                            fieldName = Optional.of(getLast(name.getOriginalParts()));
                        }
                    }

                    outputFields.add(Field.newUnqualified(fieldName, analysis.getType(expression), originTable, column.getAlias().isPresent())); // TODO don't use analysis as a side-channel. Use outputExpressions to look up the type
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
                    .withRelationType(outputScope.getRelationType())
                    .build();
            analysis.setScope(node, orderByScope);
            return orderByScope;
        }

        private Scope computeAndAssignOrderByScopeWithAggregation(OrderBy node, Scope outputScope, List<FunctionCall> aggregations, List<List<Expression>> groupByExpressions)
        {
            // This scope is only used for planning. When aggregation is present then
            // only output fields, groups and aggregation expressions should be visible from ORDER BY expression
            ImmutableList.Builder<Expression> orderByAggregationExpressionsBuilder = ImmutableList.builder();
            groupByExpressions.stream()
                    .flatMap(List::stream)
                    .forEach(orderByAggregationExpressionsBuilder::add);
            orderByAggregationExpressionsBuilder.addAll(aggregations);

            // Don't add aggregate expression that contains references to output column because the names would clash in TranslationMap during planning.
            List<Expression> orderByExpressionsReferencingOutputScope = AstUtils.preOrder(node)
                    .filter(Expression.class::isInstance)
                    .map(Expression.class::cast)
                    .filter(expression -> hasReferencesToScope(expression, analysis, outputScope))
                    .collect(toImmutableList());
            List<Expression> orderByAggregationExpressions = orderByAggregationExpressionsBuilder.build().stream()
                    .filter(expression -> !orderByExpressionsReferencingOutputScope.contains(expression))
                    .collect(toImmutableList());

            // generate placeholder fields
            List<Field> orderByAggregationSourceFields = orderByAggregationExpressions.stream()
                    .map(analysis::getType)
                    .map(type -> Field.newUnqualified(Optional.empty(), type))
                    .collect(toImmutableList());

            Scope orderByAggregationScope = Scope.builder()
                    .withRelationType(new RelationType(orderByAggregationSourceFields))
                    .build();

            Scope orderByScope = Scope.builder()
                    .withParent(orderByAggregationScope)
                    .withRelationType(outputScope.getRelationType())
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
                        throw new SemanticException(WILDCARD_WITHOUT_FROM, item, "SELECT * not allowed in queries without FROM clause");
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
            Analyzer.verifyNoAggregatesOrWindowFunctions(metadata.getFunctionRegistry(), predicate, "WHERE clause");

            ExpressionAnalysis expressionAnalysis = analyzeExpression(predicate, scope);
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

        private List<FunctionCall> analyzeAggregations(
                QuerySpecification node,
                Scope sourceScope,
                Optional<Scope> orderByScope,
                List<List<Expression>> groupingSets,
                Set<Expression> columnReferences,
                List<Expression> outputExpressions,
                List<Expression> orderByExpressions)
        {
            checkState(orderByExpressions.isEmpty() || orderByScope.isPresent(), "non-empty orderByExpressions list without orderByScope provided");

            AggregateExtractor extractor = new AggregateExtractor(metadata.getFunctionRegistry());
            for (Expression expression : Iterables.concat(outputExpressions, orderByExpressions)) {
                extractor.process(expression);
            }
            analysis.setAggregates(node, extractor.getAggregates());

            // is this an aggregation query?
            if (!groupingSets.isEmpty()) {
                // ensure SELECT, ORDER BY and HAVING are constant with respect to group
                // e.g, these are all valid expressions:
                //     SELECT f(a) GROUP BY a
                //     SELECT f(a + 1) GROUP BY a + 1
                //     SELECT a + sum(b) GROUP BY a
                ImmutableList<Expression> distinctGroupingColumns = groupingSets.stream()
                        .flatMap(Collection::stream)
                        .distinct()
                        .collect(toImmutableList());

                for (Expression expression : outputExpressions) {
                    verifySourceAggregations(distinctGroupingColumns, sourceScope, expression, metadata, analysis);
                }

                for (Expression expression : orderByExpressions) {
                    verifyOrderByAggregations(distinctGroupingColumns, sourceScope, orderByScope.get(), expression, metadata, analysis);
                }
            }

            return extractor.getAggregates();
        }

        private boolean hasAggregates(QuerySpecification node)
        {
            AggregateExtractor extractor = new AggregateExtractor(metadata.getFunctionRegistry());

            node.getSelect()
                    .getSelectItems().stream()
                    .filter(SingleColumn.class::isInstance)
                    .forEach(extractor::process);

            getSortItemsFromOrderBy(node.getOrderBy()).forEach(extractor::process);

            node.getHaving()
                    .ifPresent(extractor::process);

            return !extractor.getAggregates().isEmpty();
        }

        private RelationType analyzeView(Query query, QualifiedObjectName name, Optional<String> catalog, Optional<String> schema, Optional<String> owner, Table node)
        {
            try {
                // run view as view owner if set; otherwise, run as session user
                Identity identity;
                AccessControl viewAccessControl;
                if (owner.isPresent()) {
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
                        .setSystemProperty(LEGACY_ORDER_BY, session.getSystemProperty(LEGACY_ORDER_BY, Boolean.class).toString())
                        .build();

                StatementAnalyzer analyzer = new StatementAnalyzer(analysis, metadata, sqlParser, viewAccessControl, viewSession);
                Scope queryScope = analyzer.analyze(query, Scope.create());
                return queryScope.getRelationType().withAlias(name.getObjectName(), null);
            }
            catch (RuntimeException e) {
                throw new SemanticException(VIEW_ANALYSIS_ERROR, node, "Failed analyzing stored view '%s': %s", name, e.getMessage());
            }
        }

        private Query parseView(String view, QualifiedObjectName name, Node node)
        {
            try {
                return (Query) sqlParser.createStatement(view);
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
                        !metadata.getTypeManager().canCoerce(field.getType(), column.getType())) {
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
                    expression);
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

                String name = withQuery.getName();
                if (withScopeBuilder.containsNamedQuery(name)) {
                    throw new SemanticException(DUPLICATE_RELATION, withQuery, "WITH query name '%s' specified more than once", name);
                }

                // check if all or none of the columns are explicitly alias
                if (withQuery.getColumnNames().isPresent()) {
                    List<String> columnNames = withQuery.getColumnNames().get();
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

        private void analyzeOrderBy(Query node, Scope orderByScope)
        {
            checkState(node.getOrderBy().isPresent(), "orderBy is absent");

            List<SortItem> sortItems = getSortItemsFromOrderBy(node.getOrderBy());
            analyzeOrderBy(node, sortItems, orderByScope);
        }

        private List<Expression> analyzeOrderBy(QuerySpecification node, Scope orderByScope, List<Expression> outputExpressions)
        {
            checkState(node.getOrderBy().isPresent(), "orderBy is absent");

            List<SortItem> sortItems = getSortItemsFromOrderBy(node.getOrderBy());

            if (node.getSelect().isDistinct()) {
                verifySelectDistinct(node, outputExpressions);
            }

            return analyzeOrderBy(node, sortItems, orderByScope);
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

                ExpressionAnalysis expressionAnalysis = ExpressionAnalyzer.analyzeExpression(session,
                        metadata,
                        accessControl, sqlParser,
                        orderByScope,
                        analysis,
                        expression);
                analysis.recordSubqueries(node, expressionAnalysis);

                Type type = analysis.getType(expression);
                if (!type.isOrderable()) {
                    throw new SemanticException(TYPE_MISMATCH, node, "Type %s is not orderable, and therefore cannot be used in ORDER BY: %s", type, expression);
                }

                orderByFieldsBuilder.add(expression);
            }

            List<Expression> orderByFields = orderByFieldsBuilder.build();
            analysis.setOrderByExpressions(node, orderByFields);
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
                    .withRelationType(relationType)
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
