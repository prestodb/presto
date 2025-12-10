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

import com.facebook.airlift.log.Logger;
import com.facebook.presto.Session;
import com.facebook.presto.SystemSessionProperties;
import com.facebook.presto.common.QualifiedObjectName;
import com.facebook.presto.common.SourceColumn;
import com.facebook.presto.common.Subfield;
import com.facebook.presto.common.function.OperatorType;
import com.facebook.presto.common.predicate.Domain;
import com.facebook.presto.common.predicate.TupleDomain;
import com.facebook.presto.common.type.ArrayType;
import com.facebook.presto.common.type.BigintType;
import com.facebook.presto.common.type.DoubleType;
import com.facebook.presto.common.type.MapType;
import com.facebook.presto.common.type.RealType;
import com.facebook.presto.common.type.RowType;
import com.facebook.presto.common.type.TimestampType;
import com.facebook.presto.common.type.TimestampWithTimeZoneType;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.VarcharType;
import com.facebook.presto.metadata.CatalogMetadata;
import com.facebook.presto.metadata.FunctionAndTypeManager;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.metadata.OperatorNotFoundException;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.ConnectorId;
import com.facebook.presto.spi.MaterializedViewDefinition;
import com.facebook.presto.spi.MaterializedViewStatus;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.PrestoWarning;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.StandardErrorCode;
import com.facebook.presto.spi.TableHandle;
import com.facebook.presto.spi.WarningCollector;
import com.facebook.presto.spi.analyzer.AccessControlInfo;
import com.facebook.presto.spi.analyzer.AccessControlInfoForTable;
import com.facebook.presto.spi.analyzer.MetadataResolver;
import com.facebook.presto.spi.analyzer.ViewDefinition;
import com.facebook.presto.spi.connector.ConnectorTableVersion;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.facebook.presto.spi.eventlistener.Column;
import com.facebook.presto.spi.eventlistener.OutputColumnMetadata;
import com.facebook.presto.spi.function.FunctionKind;
import com.facebook.presto.spi.function.Signature;
import com.facebook.presto.spi.function.SqlFunction;
import com.facebook.presto.spi.function.table.Argument;
import com.facebook.presto.spi.function.table.ArgumentSpecification;
import com.facebook.presto.spi.function.table.ConnectorTableFunction;
import com.facebook.presto.spi.function.table.DescribedTableReturnTypeSpecification;
import com.facebook.presto.spi.function.table.Descriptor;
import com.facebook.presto.spi.function.table.DescriptorArgument;
import com.facebook.presto.spi.function.table.DescriptorArgumentSpecification;
import com.facebook.presto.spi.function.table.ReturnTypeSpecification;
import com.facebook.presto.spi.function.table.ScalarArgument;
import com.facebook.presto.spi.function.table.ScalarArgumentSpecification;
import com.facebook.presto.spi.function.table.TableArgument;
import com.facebook.presto.spi.function.table.TableArgumentSpecification;
import com.facebook.presto.spi.function.table.TableFunctionAnalysis;
import com.facebook.presto.spi.function.table.TableFunctionMetadata;
import com.facebook.presto.spi.procedure.DistributedProcedure;
import com.facebook.presto.spi.procedure.TableDataRewriteDistributedProcedure;
import com.facebook.presto.spi.relation.DomainTranslator;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.security.AccessControl;
import com.facebook.presto.spi.security.AllowAllAccessControl;
import com.facebook.presto.spi.security.Identity;
import com.facebook.presto.spi.security.ViewAccessControl;
import com.facebook.presto.spi.security.ViewExpression;
import com.facebook.presto.spi.security.ViewSecurity;
import com.facebook.presto.spi.type.UnknownTypeException;
import com.facebook.presto.sql.ExpressionUtils;
import com.facebook.presto.sql.MaterializedViewUtils;
import com.facebook.presto.sql.analyzer.Analysis.MergeAnalysis;
import com.facebook.presto.sql.analyzer.Analysis.TableArgumentAnalysis;
import com.facebook.presto.sql.analyzer.Analysis.TableFunctionInvocationAnalysis;
import com.facebook.presto.sql.parser.ParsingException;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.planner.ExpressionInterpreter;
import com.facebook.presto.sql.planner.TypeProvider;
import com.facebook.presto.sql.planner.VariablesExtractor;
import com.facebook.presto.sql.relational.RowExpressionDomainTranslator;
import com.facebook.presto.sql.relational.SqlToRowExpressionTranslator;
import com.facebook.presto.sql.tree.AddColumn;
import com.facebook.presto.sql.tree.AddConstraint;
import com.facebook.presto.sql.tree.AliasedRelation;
import com.facebook.presto.sql.tree.AllColumns;
import com.facebook.presto.sql.tree.AlterColumnNotNull;
import com.facebook.presto.sql.tree.AlterFunction;
import com.facebook.presto.sql.tree.Analyze;
import com.facebook.presto.sql.tree.Call;
import com.facebook.presto.sql.tree.Commit;
import com.facebook.presto.sql.tree.CreateFunction;
import com.facebook.presto.sql.tree.CreateMaterializedView;
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
import com.facebook.presto.sql.tree.DropConstraint;
import com.facebook.presto.sql.tree.DropFunction;
import com.facebook.presto.sql.tree.DropMaterializedView;
import com.facebook.presto.sql.tree.DropSchema;
import com.facebook.presto.sql.tree.DropTable;
import com.facebook.presto.sql.tree.DropView;
import com.facebook.presto.sql.tree.EmptyTableTreatment;
import com.facebook.presto.sql.tree.Except;
import com.facebook.presto.sql.tree.Execute;
import com.facebook.presto.sql.tree.Explain;
import com.facebook.presto.sql.tree.ExplainFormat;
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
import com.facebook.presto.sql.tree.LogicalBinaryExpression;
import com.facebook.presto.sql.tree.LongLiteral;
import com.facebook.presto.sql.tree.Merge;
import com.facebook.presto.sql.tree.MergeCase;
import com.facebook.presto.sql.tree.MergeInsert;
import com.facebook.presto.sql.tree.MergeUpdate;
import com.facebook.presto.sql.tree.NaturalJoin;
import com.facebook.presto.sql.tree.Node;
import com.facebook.presto.sql.tree.NodeLocation;
import com.facebook.presto.sql.tree.NodeRef;
import com.facebook.presto.sql.tree.Offset;
import com.facebook.presto.sql.tree.OrderBy;
import com.facebook.presto.sql.tree.Parameter;
import com.facebook.presto.sql.tree.Prepare;
import com.facebook.presto.sql.tree.Property;
import com.facebook.presto.sql.tree.QualifiedName;
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
import com.facebook.presto.sql.tree.Rollback;
import com.facebook.presto.sql.tree.Rollup;
import com.facebook.presto.sql.tree.Row;
import com.facebook.presto.sql.tree.SampledRelation;
import com.facebook.presto.sql.tree.Select;
import com.facebook.presto.sql.tree.SelectItem;
import com.facebook.presto.sql.tree.SetOperation;
import com.facebook.presto.sql.tree.SetProperties;
import com.facebook.presto.sql.tree.SetSession;
import com.facebook.presto.sql.tree.SimpleGroupBy;
import com.facebook.presto.sql.tree.SingleColumn;
import com.facebook.presto.sql.tree.SortItem;
import com.facebook.presto.sql.tree.SqlParameterDeclaration;
import com.facebook.presto.sql.tree.StartTransaction;
import com.facebook.presto.sql.tree.Statement;
import com.facebook.presto.sql.tree.Table;
import com.facebook.presto.sql.tree.TableFunctionArgument;
import com.facebook.presto.sql.tree.TableFunctionDescriptorArgument;
import com.facebook.presto.sql.tree.TableFunctionInvocation;
import com.facebook.presto.sql.tree.TableFunctionTableArgument;
import com.facebook.presto.sql.tree.TableSubquery;
import com.facebook.presto.sql.tree.TruncateTable;
import com.facebook.presto.sql.tree.Union;
import com.facebook.presto.sql.tree.Unnest;
import com.facebook.presto.sql.tree.Update;
import com.facebook.presto.sql.tree.UpdateAssignment;
import com.facebook.presto.sql.tree.Use;
import com.facebook.presto.sql.tree.Values;
import com.facebook.presto.sql.tree.Window;
import com.facebook.presto.sql.tree.WindowFrame;
import com.facebook.presto.sql.tree.With;
import com.facebook.presto.sql.tree.WithQuery;
import com.facebook.presto.sql.util.AstUtils;
import com.facebook.presto.transaction.TransactionManager;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Multimap;
import com.google.common.collect.Streams;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.facebook.presto.SystemSessionProperties.getMaxGroupingSets;
import static com.facebook.presto.SystemSessionProperties.isAllowWindowOrderByLiterals;
import static com.facebook.presto.SystemSessionProperties.isLegacyMaterializedViews;
import static com.facebook.presto.SystemSessionProperties.isMaterializedViewDataConsistencyEnabled;
import static com.facebook.presto.SystemSessionProperties.isMaterializedViewPartitionFilteringEnabled;
import static com.facebook.presto.common.RuntimeMetricName.SKIP_READING_FROM_MATERIALIZED_VIEW_COUNT;
import static com.facebook.presto.common.RuntimeUnit.NONE;
import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.BooleanType.BOOLEAN;
import static com.facebook.presto.common.type.DoubleType.DOUBLE;
import static com.facebook.presto.common.type.TypeSignature.parseTypeSignature;
import static com.facebook.presto.common.type.UnknownType.UNKNOWN;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static com.facebook.presto.execution.CallTask.extractParameterValuesInOrder;
import static com.facebook.presto.metadata.MetadataUtil.createQualifiedObjectName;
import static com.facebook.presto.metadata.MetadataUtil.getConnectorIdOrThrow;
import static com.facebook.presto.metadata.MetadataUtil.toSchemaTableName;
import static com.facebook.presto.spi.StandardErrorCode.DATATYPE_MISMATCH;
import static com.facebook.presto.spi.StandardErrorCode.INVALID_COLUMN_MASK;
import static com.facebook.presto.spi.StandardErrorCode.INVALID_ROW_FILTER;
import static com.facebook.presto.spi.StandardErrorCode.MV_MISSING_TOO_MUCH_DATA;
import static com.facebook.presto.spi.StandardWarningCode.PERFORMANCE_WARNING;
import static com.facebook.presto.spi.StandardWarningCode.REDUNDANT_ORDER_BY;
import static com.facebook.presto.spi.StandardWarningCode.SEMANTIC_WARNING;
import static com.facebook.presto.spi.analyzer.AccessControlRole.TABLE_CREATE;
import static com.facebook.presto.spi.analyzer.AccessControlRole.TABLE_DELETE;
import static com.facebook.presto.spi.analyzer.AccessControlRole.TABLE_INSERT;
import static com.facebook.presto.spi.connector.ConnectorTableVersion.VersionOperator;
import static com.facebook.presto.spi.connector.ConnectorTableVersion.VersionType;
import static com.facebook.presto.spi.function.FunctionKind.AGGREGATE;
import static com.facebook.presto.spi.function.FunctionKind.WINDOW;
import static com.facebook.presto.spi.function.table.DescriptorArgument.NULL_DESCRIPTOR;
import static com.facebook.presto.spi.function.table.GenericTableReturnTypeSpecification.GENERIC_TABLE;
import static com.facebook.presto.spi.security.ViewSecurity.DEFINER;
import static com.facebook.presto.spi.security.ViewSecurity.INVOKER;
import static com.facebook.presto.sql.MaterializedViewUtils.buildOwnerSession;
import static com.facebook.presto.sql.MaterializedViewUtils.generateBaseTablePredicates;
import static com.facebook.presto.sql.MaterializedViewUtils.generateFalsePredicates;
import static com.facebook.presto.sql.MaterializedViewUtils.getOwnerIdentity;
import static com.facebook.presto.sql.NodeUtils.getSortItemsFromOrderBy;
import static com.facebook.presto.sql.NodeUtils.mapFromProperties;
import static com.facebook.presto.sql.QueryUtil.selectList;
import static com.facebook.presto.sql.QueryUtil.simpleQuery;
import static com.facebook.presto.sql.SqlFormatter.formatSql;
import static com.facebook.presto.sql.SqlFormatterUtil.getFormattedSql;
import static com.facebook.presto.sql.analyzer.AggregationAnalyzer.verifyOrderByAggregations;
import static com.facebook.presto.sql.analyzer.AggregationAnalyzer.verifySourceAggregations;
import static com.facebook.presto.sql.analyzer.Analysis.MaterializedViewAnalysisState;
import static com.facebook.presto.sql.analyzer.Analyzer.verifyNoAggregateWindowOrGroupingFunctions;
import static com.facebook.presto.sql.analyzer.Analyzer.verifyNoExternalFunctions;
import static com.facebook.presto.sql.analyzer.ExpressionAnalyzer.createConstantAnalyzer;
import static com.facebook.presto.sql.analyzer.ExpressionAnalyzer.getExpressionTypes;
import static com.facebook.presto.sql.analyzer.ExpressionTreeUtils.extractAggregateFunctions;
import static com.facebook.presto.sql.analyzer.ExpressionTreeUtils.extractExpressions;
import static com.facebook.presto.sql.analyzer.ExpressionTreeUtils.extractWindowFunctions;
import static com.facebook.presto.sql.analyzer.PredicateStitcher.PredicateStitcherContext;
import static com.facebook.presto.sql.analyzer.RefreshMaterializedViewPredicateAnalyzer.extractTablePredicates;
import static com.facebook.presto.sql.analyzer.ScopeReferenceExtractor.hasReferencesToScope;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.AMBIGUOUS_ATTRIBUTE;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.COLUMN_NAME_NOT_SPECIFIED;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.COLUMN_TYPE_UNKNOWN;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.DUPLICATE_COLUMN_NAME;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.DUPLICATE_PARAMETER_NAME;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.DUPLICATE_PROPERTY;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.DUPLICATE_RELATION;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.FUNCTION_NOT_FOUND;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.INVALID_FUNCTION_NAME;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.INVALID_OFFSET_ROW_COUNT;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.INVALID_ORDINAL;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.INVALID_PROCEDURE_ARGUMENTS;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.INVALID_WINDOW_FRAME;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.MATERIALIZED_VIEW_ALREADY_EXISTS;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.MATERIALIZED_VIEW_IS_RECURSIVE;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.MISMATCHED_COLUMN_ALIASES;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.MISMATCHED_SET_COLUMN_TYPES;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.MISSING_ATTRIBUTE;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.MISSING_CATALOG;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.MISSING_COLUMN;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.MISSING_MATERIALIZED_VIEW;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.MISSING_SCHEMA;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.MISSING_TABLE;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.MUST_BE_WINDOW_FUNCTION;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.NESTED_WINDOW;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.NONDETERMINISTIC_ORDER_BY_EXPRESSION_WITH_SELECT_DISTINCT;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.NON_NUMERIC_SAMPLE_PERCENTAGE;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.NOT_SUPPORTED;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.ORDER_BY_MUST_BE_IN_SELECT;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.PROCEDURE_NOT_FOUND;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.TABLE_ALREADY_EXISTS;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.TABLE_FUNCTION_AMBIGUOUS_RETURN_TYPE;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.TABLE_FUNCTION_COLUMN_NOT_FOUND;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.TABLE_FUNCTION_DUPLICATE_RANGE_VARIABLE;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.TABLE_FUNCTION_IMPLEMENTATION_ERROR;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.TABLE_FUNCTION_INVALID_ARGUMENTS;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.TABLE_FUNCTION_INVALID_COLUMN_REFERENCE;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.TABLE_FUNCTION_INVALID_COPARTITIONING;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.TABLE_FUNCTION_INVALID_FUNCTION_ARGUMENT;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.TABLE_FUNCTION_INVALID_TABLE_FUNCTION_INVOCATION;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.TABLE_FUNCTION_MISSING_ARGUMENT;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.TABLE_FUNCTION_MISSING_RETURN_TYPE;
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
import static com.facebook.presto.sql.planner.ExpressionInterpreter.evaluateConstantExpression;
import static com.facebook.presto.sql.planner.ExpressionInterpreter.expressionOptimizer;
import static com.facebook.presto.sql.tree.DereferenceExpression.getQualifiedName;
import static com.facebook.presto.sql.tree.ExplainFormat.Type.JSON;
import static com.facebook.presto.sql.tree.ExplainFormat.Type.TEXT;
import static com.facebook.presto.sql.tree.ExplainType.Type.DISTRIBUTED;
import static com.facebook.presto.sql.tree.FrameBound.Type.CURRENT_ROW;
import static com.facebook.presto.sql.tree.FrameBound.Type.FOLLOWING;
import static com.facebook.presto.sql.tree.FrameBound.Type.PRECEDING;
import static com.facebook.presto.sql.tree.FrameBound.Type.UNBOUNDED_FOLLOWING;
import static com.facebook.presto.sql.tree.FrameBound.Type.UNBOUNDED_PRECEDING;
import static com.facebook.presto.sql.tree.TableVersionExpression.TableVersionOperator;
import static com.facebook.presto.sql.tree.TableVersionExpression.TableVersionType;
import static com.facebook.presto.sql.tree.TableVersionExpression.TableVersionType.TIMESTAMP;
import static com.facebook.presto.sql.tree.TableVersionExpression.TableVersionType.VERSION;
import static com.facebook.presto.util.AnalyzerUtil.createParsingOptions;
import static com.facebook.presto.util.MetadataUtils.getMaterializedViewDefinition;
import static com.facebook.presto.util.MetadataUtils.getTableColumnsMetadata;
import static com.facebook.presto.util.MetadataUtils.getViewDefinition;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Throwables.throwIfInstanceOf;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static com.google.common.collect.Iterables.getOnlyElement;
import static java.lang.Math.toIntExact;
import static java.lang.String.format;
import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static java.util.Locale.ENGLISH;
import static java.util.Map.Entry;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.counting;
import static java.util.stream.Collectors.groupingBy;

class StatementAnalyzer
{
    private static final Logger log = Logger.get(StatementAnalyzer.class);
    private static final int UNION_DISTINCT_FIELDS_WARNING_THRESHOLD = 3;
    private final Analysis analysis;
    private final Metadata metadata;
    private final FunctionAndTypeResolver functionAndTypeResolver;
    private final Session session;
    private final SqlParser sqlParser;
    private final AccessControl accessControl;
    private final WarningCollector warningCollector;
    private final MetadataResolver metadataResolver;

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
        this.metadataResolver = requireNonNull(metadata.getMetadataResolver(session), "metadataResolver is null");
        requireNonNull(metadata.getFunctionAndTypeManager(), "functionAndTypeManager is null");
        this.functionAndTypeResolver = requireNonNull(metadata.getFunctionAndTypeManager().getFunctionAndTypeResolver(), "functionAndTypeResolver is null");

        analysis.addQueryAccessControlInfo(new AccessControlInfo(accessControl, session.getIdentity(), session.getTransactionId(), session.getAccessControlContext()));
    }

    public Scope analyze(Node node, Scope outerQueryScope)
    {
        return analyze(node, Optional.of(outerQueryScope));
    }

    public Scope analyze(Node node, Optional<Scope> outerQueryScope)
    {
        return new Visitor(metadata, session, outerQueryScope, warningCollector).process(node, Optional.empty());
    }

    /**
     * Visitor context represents local query scope (if exists). The invariant is
     * that the local query scopes hierarchy should always have outer query scope
     * (if provided) as ancestor.
     */
    private class Visitor
            extends DefaultTraversalVisitor<Scope, Optional<Scope>>
    {
        private final Metadata metadata;
        private final Session session;
        private final Optional<Scope> outerQueryScope;
        private final WarningCollector warningCollector;

        private Visitor(
                Metadata metadata,
                Session session,
                Optional<Scope> outerQueryScope,
                WarningCollector warningCollector)
        {
            this.metadata = requireNonNull(metadata, "metadata is null");
            this.session = requireNonNull(session, "session is null");
            this.outerQueryScope = requireNonNull(outerQueryScope, "outerQueryScope is null");
            this.warningCollector = requireNonNull(warningCollector, "warningCollector is null");
        }

        public Scope process(Node node, Optional<Scope> scope)
        {
            Scope returnScope = super.process(node, scope);
            checkState(returnScope.getOuterQueryParent().equals(outerQueryScope), "result scope should have outer query scope equal with parameter outer query scope");
            scope.ifPresent(value -> checkState(hasScopeAsLocalParent(returnScope, value), "return scope should have context scope as one of ancestors"));
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
            QualifiedObjectName targetTable = createQualifiedObjectName(session, insert, insert.getTarget(), metadata);

            MetadataHandle metadataHandle = analysis.getMetadataHandle();
            if (getViewDefinition(session, metadataResolver, metadataHandle, targetTable).isPresent()) {
                throw new SemanticException(NOT_SUPPORTED, insert, "Inserting into views is not supported");
            }

            if (getMaterializedViewDefinition(session, metadataResolver, metadataHandle, targetTable).isPresent()) {
                throw new SemanticException(NOT_SUPPORTED, insert, "Inserting into materialized views is not supported");
            }

            // analyze the query that creates the data
            Scope queryScope = process(insert.getQuery(), scope);

            analysis.setUpdateInfo(insert.getUpdateInfo());

            TableColumnMetadata tableColumnsMetadata = getTableColumnsMetadata(session, metadataResolver, metadataHandle, targetTable);
            // verify the insert destination columns match the query

            analysis.addAccessControlCheckForTable(TABLE_INSERT, new AccessControlInfoForTable(accessControl, session.getIdentity(), session.getTransactionId(), session.getAccessControlContext(), targetTable));

            if (!accessControl.getRowFilters(session.getRequiredTransactionId(), session.getIdentity(), session.getAccessControlContext(), targetTable).isEmpty()) {
                throw new SemanticException(NOT_SUPPORTED, insert, "Insert into table with row filter is not supported");
            }

            List<ColumnMetadata> columnsMetadata = tableColumnsMetadata.getColumnsMetadata();

            if (!accessControl.getColumnMasks(session.getRequiredTransactionId(), session.getIdentity(), session.getAccessControlContext(), targetTable, columnsMetadata).isEmpty()) {
                throw new SemanticException(NOT_SUPPORTED, insert, "Insert into table with column masks is not supported");
            }

            List<String> tableColumns = columnsMetadata.stream()
                    .filter(column -> !column.isHidden())
                    .map(ColumnMetadata::getName)
                    .collect(toImmutableList());

            List<String> insertColumns;
            if (insert.getColumns().isPresent()) {
                insertColumns = insert.getColumns().get().stream()
                        .map(Identifier::getValue)
                        .map(column -> metadata.normalizeIdentifier(session, targetTable.getCatalogName(), column))
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
                    .map(insertColumn -> getColumnMetadata(columnsMetadata, insertColumn))
                    .collect(toImmutableList());

            checkTypesMatchForInsert(insert, queryScope, expectedColumns);

            Map<String, ColumnHandle> columnHandles = tableColumnsMetadata.getColumnHandles();
            analysis.setInsert(new Analysis.Insert(
                    tableColumnsMetadata.getTableHandle().get(),
                    insertColumns.stream().map(columnHandles::get).collect(toImmutableList())));

            List<Type> types = queryScope.getRelationType().getVisibleFields().stream()
                    .map(Field::getType)
                    .collect(toImmutableList());

            Stream<Column> columnStream = Streams.zip(
                    insertColumns.stream(),
                    types.stream()
                            .map(Type::toString),
                    Column::new);

            analysis.setUpdatedSourceColumns(Optional.of(Streams.zip(
                            columnStream, queryScope.getRelationType().getVisibleFields().stream(), (column, field) -> new OutputColumnMetadata(column.getName(), column.getType(), analysis.getSourceColumns(field)))
                    .collect(toImmutableList())));

            return createAndAssignScope(insert, scope, Field.newUnqualified(insert.getLocation(), "rows", BIGINT));
        }

        private ColumnMetadata getColumnMetadata(List<ColumnMetadata> columnsMetadata, String columnName)
        {
            return columnsMetadata.stream()
                    .filter(columnMetadata -> columnMetadata.getName().equals(columnName))
                    .findFirst()
                    .orElseThrow(() -> new IllegalArgumentException(String.format("Invalid column name: %s", columnName)));
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
                if (!functionAndTypeResolver.canCoerce(
                        queryColumnTypes.get(i),
                        expectedColumns.get(i).getType())) {
                    if (queryColumnTypes.get(i) instanceof RowType && expectedColumns.get(i).getType() instanceof RowType) {
                        String fieldName = expectedColumns.get(i).getName();
                        List<Type> columnRowTypes = queryColumnTypes.get(i).getTypeParameters();
                        List<RowType.Field> expectedRowFields = ((RowType) expectedColumns.get(i).getType()).getFields();
                        checkTypesMatchForNestedStructs(
                                node,
                                errorMessage,
                                i + 1,
                                fieldName,
                                expectedRowFields,
                                columnRowTypes);
                    }
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

        private void checkTypesMatchForNestedStructs(
                Node node,
                String errorMessage,
                int columnIndex,
                String fieldName,
                List<RowType.Field> expectedRowFields,
                List<Type> columnRowTypes)
        {
            if (expectedRowFields.size() != columnRowTypes.size()) {
                throw new SemanticException(MISMATCHED_SET_COLUMN_TYPES,
                        node,
                        errorMessage + "Mismatch at column %d: '%s' has %d field(s) but expression has %d field(s)",
                        columnIndex,
                        fieldName,
                        expectedRowFields.size(),
                        columnRowTypes.size());
            }
            for (int rowFieldIndex = 0; rowFieldIndex < expectedRowFields.size(); rowFieldIndex++) {
                if (!functionAndTypeResolver.canCoerce(
                        columnRowTypes.get(rowFieldIndex),
                        expectedRowFields.get(rowFieldIndex).getType())) {
                    fieldName += "." + expectedRowFields.get(rowFieldIndex).getName().get();
                    if (columnRowTypes.get(rowFieldIndex) instanceof RowType && expectedRowFields.get(rowFieldIndex).getType() instanceof RowType) {
                        checkTypesMatchForNestedStructs(
                                node,
                                errorMessage,
                                columnIndex,
                                fieldName,
                                ((RowType) expectedRowFields.get(rowFieldIndex).getType()).getFields(),
                                columnRowTypes.get(rowFieldIndex).getTypeParameters());
                    }
                    else {
                        throw new SemanticException(MISMATCHED_SET_COLUMN_TYPES,
                                node,
                                errorMessage + "Mismatch at column %d: '%s' is of type %s but expression is of type %s",
                                columnIndex,
                                fieldName,
                                expectedRowFields.get(rowFieldIndex).getType(),
                                columnRowTypes.get(rowFieldIndex));
                    }
                }
            }
        }

        @Override
        protected Scope visitDelete(Delete node, Optional<Scope> scope)
        {
            Table table = node.getTable();
            QualifiedObjectName tableName = createQualifiedObjectName(session, table, table.getName(), metadata);
            MetadataHandle metadataHandle = analysis.getMetadataHandle();

            if (getViewDefinition(session, metadataResolver, metadataHandle, tableName).isPresent()) {
                throw new SemanticException(NOT_SUPPORTED, node, "Deleting from views is not supported");
            }

            if (getMaterializedViewDefinition(session, metadataResolver, metadataHandle, tableName).isPresent()) {
                throw new SemanticException(NOT_SUPPORTED, node, "Deleting from materialized views is not supported");
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

            analysis.setUpdateInfo(node.getUpdateInfo());

            analysis.addAccessControlCheckForTable(TABLE_DELETE, new AccessControlInfoForTable(accessControl, session.getIdentity(), session.getTransactionId(), session.getAccessControlContext(), tableName));

            if (!accessControl.getRowFilters(session.getRequiredTransactionId(), session.getIdentity(), session.getAccessControlContext(), tableName).isEmpty()) {
                throw new SemanticException(NOT_SUPPORTED, node, "Delete from table with row filter is not supported");
            }

            TableColumnMetadata tableColumnsMetadata = getTableColumnsMetadata(session, metadataResolver, analysis.getMetadataHandle(), tableName);
            List<ColumnMetadata> columnsMetadata = tableColumnsMetadata.getColumnsMetadata();
            if (!accessControl.getColumnMasks(session.getRequiredTransactionId(), session.getIdentity(), session.getAccessControlContext(), tableName, columnsMetadata).isEmpty()) {
                throw new SemanticException(NOT_SUPPORTED, node, "Delete from table with column mask is not supported");
            }

            return createAndAssignScope(node, scope, Field.newUnqualified(node.getLocation(), "rows", BIGINT));
        }

        @Override
        protected Scope visitAnalyze(Analyze node, Optional<Scope> scope)
        {
            QualifiedObjectName tableName = createQualifiedObjectName(session, node, node.getTableName(), metadata);
            analysis.setUpdateInfo(node.getUpdateInfo());
            MetadataHandle metadataHandle = analysis.getMetadataHandle();

            // verify the target table exists, and it's not a view
            if (getViewDefinition(session, metadataResolver, metadataHandle, tableName).isPresent()) {
                throw new SemanticException(NOT_SUPPORTED, node, "Analyzing views is not supported");
            }

            validateProperties(node.getProperties(), scope);

            Map<String, Object> analyzeProperties = metadata.getAnalyzePropertyManager().getProperties(
                    getConnectorIdOrThrow(session, metadata, tableName.getCatalogName()),
                    tableName.getCatalogName(),
                    mapFromProperties(node.getProperties()),
                    session,
                    metadata,
                    analysis.getParameters());
            TableHandle tableHandle = metadata.getTableHandleForStatisticsCollection(session, tableName, analyzeProperties)
                    .orElseThrow(() -> (new SemanticException(MISSING_TABLE, node, "Table '%s' does not exist", tableName)));

            // user must have read and insert permission in order to analyze stats of a table
            Multimap<QualifiedObjectName, Subfield> tableColumnMap = ImmutableMultimap.<QualifiedObjectName, Subfield>builder()
                    .putAll(tableName, metadataResolver.getColumnHandles(tableHandle).keySet().stream().map(column -> new Subfield(column, ImmutableList.of())).collect(toImmutableSet()))
                    .build();
            analysis.addTableColumnAndSubfieldReferences(accessControl, session.getIdentity(), session.getTransactionId(), session.getAccessControlContext(), tableColumnMap, tableColumnMap);
            analysis.addAccessControlCheckForTable(TABLE_INSERT, new AccessControlInfoForTable(accessControl, session.getIdentity(), session.getTransactionId(), session.getAccessControlContext(), tableName));

            analysis.setAnalyzeTarget(tableHandle);
            return createAndAssignScope(node, scope, Field.newUnqualified(node.getLocation(), "rows", BIGINT));
        }

        @Override
        protected Scope visitCreateTableAsSelect(CreateTableAsSelect node, Optional<Scope> scope)
        {
            analysis.setUpdateInfo(node.getUpdateInfo());

            // turn this into a query that has a new table writer node on top.
            QualifiedObjectName targetTable = createQualifiedObjectName(session, node, node.getName(), metadata);
            analysis.setCreateTableDestination(targetTable);

            if (metadataResolver.tableExists(targetTable)) {
                if (node.isNotExists()) {
                    analysis.setCreateTableAsSelectNoOp(true);
                    return createAndAssignScope(node, scope, Field.newUnqualified(node.getLocation(), "rows", BIGINT));
                }
                throw new SemanticException(TABLE_ALREADY_EXISTS, node, "Destination table '%s' already exists", targetTable);
            }

            validateProperties(node.getProperties(), scope);
            analysis.setCreateTableProperties(mapFromProperties(node.getProperties()));

            node.getColumnAliases().ifPresent(analysis::setCreateTableColumnAliases);
            analysis.setCreateTableComment(node.getComment());

            analysis.addAccessControlCheckForTable(TABLE_CREATE, new AccessControlInfoForTable(accessControl, session.getIdentity(), session.getTransactionId(), session.getAccessControlContext(), targetTable));

            analysis.setCreateTableAsSelectWithData(node.isWithData());

            // analyze the query that creates the table
            Scope queryScope = process(node.getQuery(), scope);

            ImmutableList.Builder<OutputColumnMetadata> outputColumns = ImmutableList.builder();

            if (node.getColumnAliases().isPresent()) {
                validateColumnAliases(node.getColumnAliases().get(), queryScope.getRelationType().getVisibleFieldCount());
                int aliasPosition = 0;
                // analyze only column types in subquery if column alias exists
                for (Field field : queryScope.getRelationType().getVisibleFields()) {
                    if (field.getType().equals(UNKNOWN)) {
                        throw new SemanticException(COLUMN_TYPE_UNKNOWN, node, "Column type is unknown at position %s", queryScope.getRelationType().indexOf(field) + 1);
                    }
                    String columnName = node.getColumnAliases().get().get(aliasPosition).getValue();
                    outputColumns.add(new OutputColumnMetadata(columnName, field.getType().toString(), analysis.getSourceColumns(field)));
                    aliasPosition++;
                }
            }
            else {
                validateColumns(node, queryScope.getRelationType());
                queryScope.getRelationType().getVisibleFields().stream()
                        .map(this::createOutputColumn)
                        .forEach(outputColumns::add);
            }
            analysis.setUpdatedSourceColumns(Optional.of(outputColumns.build()));
            return createAndAssignScope(node, scope, Field.newUnqualified(node.getLocation(), "rows", BIGINT));
        }

        private OutputColumnMetadata createOutputColumn(Field field)
        {
            return new OutputColumnMetadata(field.getName().get(), field.getType().toString(), analysis.getSourceColumns(field));
        }

        @Override
        protected Scope visitCreateView(CreateView node, Optional<Scope> scope)
        {
            QualifiedObjectName viewName = createQualifiedObjectName(session, node, node.getName(), metadata);
            analysis.setUpdateInfo(node.getUpdateInfo());

            // analyze the query that creates the view
            StatementAnalyzer analyzer = new StatementAnalyzer(analysis, metadata, sqlParser, accessControl, session, warningCollector);

            Scope queryScope = analyzer.analyze(node.getQuery(), scope);

            // TODO: this should be removed from here once DDL statements are onboarded to pluggable analyzer
            accessControl.checkCanCreateView(session.getRequiredTransactionId(), session.getIdentity(), session.getAccessControlContext(), viewName);

            validateColumns(node, queryScope.getRelationType());

            return createAndAssignScope(node, scope);
        }

        @Override
        protected Scope visitCreateMaterializedView(CreateMaterializedView node, Optional<Scope> scope)
        {
            QualifiedObjectName viewName = createQualifiedObjectName(session, node, node.getName(), metadata);
            analysis.setUpdateInfo(node.getUpdateInfo());
            analysis.setCreateTableDestination(viewName);

            if (metadataResolver.tableExists(viewName)) {
                if (node.isNotExists()) {
                    return createAndAssignScope(node, scope);
                }
                throw new SemanticException(MATERIALIZED_VIEW_ALREADY_EXISTS, node, "Destination materialized view '%s' already exists", viewName);
            }
            validateProperties(node.getProperties(), scope);

            analysis.setCreateTableProperties(mapFromProperties(node.getProperties()));
            analysis.setCreateTableComment(node.getComment());

            accessControl.checkCanCreateTable(session.getRequiredTransactionId(), session.getIdentity(), session.getAccessControlContext(), viewName);
            accessControl.checkCanCreateView(session.getRequiredTransactionId(), session.getIdentity(), session.getAccessControlContext(), viewName);

            // analyze the query that creates the table
            Scope queryScope = process(node.getQuery(), scope);

            validateColumns(node, queryScope.getRelationType());

            validateBaseTables(analysis.getTableNodes(), node);

            return createAndAssignScope(node, scope);
        }

        @Override
        protected Scope visitRefreshMaterializedView(RefreshMaterializedView node, Optional<Scope> scope)
        {
            QualifiedObjectName viewName = createQualifiedObjectName(session, node.getTarget(), node.getTarget().getName(), metadata);
            analysis.setUpdateInfo(node.getUpdateInfo());

            MaterializedViewDefinition view = getMaterializedViewDefinition(session, metadataResolver, analysis.getMetadataHandle(), viewName)
                    .orElseThrow(() -> new SemanticException(MISSING_MATERIALIZED_VIEW, node, "Materialized view '%s' does not exist", viewName));

            // the original refresh statement will always be one line
            analysis.setExpandedQuery(format("-- Expanded Query: %s%nINSERT INTO %s %s",
                    getFormattedSql(node, sqlParser, Optional.empty()),
                    viewName.getObjectName(),
                    view.getOriginalSql()));

            if (!isLegacyMaterializedViews(session)) {
                analysis.addAccessControlCheckForTable(
                        TABLE_DELETE,
                        new AccessControlInfoForTable(
                                accessControl,
                                getOwnerIdentity(view.getOwner(), session),
                                session.getTransactionId(),
                                session.getAccessControlContext(),
                                viewName));
            }

            analysis.addAccessControlCheckForTable(
                    TABLE_INSERT,
                    new AccessControlInfoForTable(
                            accessControl,
                            getOwnerIdentity(view.getOwner(), session),
                            session.getTransactionId(),
                            session.getAccessControlContext(),
                            viewName));

            // Use AllowAllAccessControl; otherwise Analyzer will check SELECT permission on the materialized view, which is not necessary.
            StatementAnalyzer viewAnalyzer = new StatementAnalyzer(analysis, metadata, sqlParser, new AllowAllAccessControl(), session, warningCollector);
            Scope viewScope = viewAnalyzer.analyze(node.getTarget(), scope);

            Map<SchemaTableName, Expression> tablePredicates = getTablePredicatesForMaterializedViewRefresh(
                    session, node, viewName, viewScope, metadata);

            Query viewQuery = parseView(view.getOriginalSql(), viewName, node);
            Query refreshQuery = tablePredicates.containsKey(toSchemaTableName(viewName)) ?
                    buildSubqueryWithPredicate(viewQuery, tablePredicates.get(toSchemaTableName(viewName)))
                    : viewQuery;
            // Check if the owner has SELECT permission on the base tables
            StatementAnalyzer queryAnalyzer = new StatementAnalyzer(
                    analysis,
                    metadata,
                    sqlParser,
                    accessControl,
                    buildOwnerSession(session, view.getOwner(), metadata.getSessionPropertyManager(), viewName.getCatalogName(), view.getSchema()),
                    warningCollector);
            queryAnalyzer.analyze(refreshQuery, Scope.create());

            TableColumnMetadata tableColumnsMetadata = getTableColumnsMetadata(session, metadataResolver, analysis.getMetadataHandle(), viewName);
            TableHandle tableHandle = tableColumnsMetadata.getTableHandle()
                    .orElseThrow(() -> new SemanticException(MISSING_MATERIALIZED_VIEW, node, "Materialized view '%s' does not exist", viewName));

            Map<String, ColumnHandle> columnHandles = tableColumnsMetadata.getColumnHandles();
            List<ColumnHandle> targetColumnHandles = tableColumnsMetadata.getColumnsMetadata().stream()
                    .filter(column -> !column.isHidden())
                    .map(column -> columnHandles.get(column.getName()))
                    .collect(toImmutableList());
            analysis.setRefreshMaterializedViewAnalysis(new Analysis.RefreshMaterializedViewAnalysis(
                    tableHandle,
                    targetColumnHandles,
                    refreshQuery));

            return createAndAssignScope(node, scope, Field.newUnqualified(node.getLocation(), "rows", BIGINT));
        }

        private Map<SchemaTableName, Expression> analyzeAutoRefreshMaterializedView(
                RefreshMaterializedView node,
                QualifiedObjectName viewName)
        {
            MaterializedViewStatus viewStatus = metadataResolver.getMaterializedViewStatus(viewName, TupleDomain.all());
            Map<SchemaTableName, MaterializedViewStatus.MaterializedDataPredicates> missingPartitionsPerTable =
                    viewStatus.getPartitionsFromBaseTables();

            if (viewStatus.isFullyMaterialized() || missingPartitionsPerTable.isEmpty()) {
                warningCollector.add(new PrestoWarning(SEMANTIC_WARNING,
                        format("Materialized view %s is already fully refreshed", viewName)));
                return ImmutableMap.of();
            }
            if ((viewStatus.isNotMaterialized() || viewStatus.isTooManyPartitionsMissing()) &&
                    !SystemSessionProperties.isMaterializedViewAllowFullRefreshEnabled(session)) {
                throw new PrestoException(MV_MISSING_TOO_MUCH_DATA,
                        format("%s misses too many partitions or is never refreshed and may incur high cost. " +
                                "Consider refreshing with predicates first.", viewName.toString()));
            }

            return MaterializedViewUtils.generatePredicatesForMissingPartitions(missingPartitionsPerTable, metadata);
        }

        private Optional<RelationType> analyzeBaseTableForRefreshMaterializedView(Table baseTable, Optional<Scope> scope)
        {
            checkState(analysis.getStatement() instanceof RefreshMaterializedView, "Not analyzing RefreshMaterializedView statement");

            RefreshMaterializedView refreshMaterializedView = (RefreshMaterializedView) analysis.getStatement();
            QualifiedObjectName viewName = createQualifiedObjectName(session, refreshMaterializedView.getTarget(), refreshMaterializedView.getTarget().getName(), metadata);

            // Use AllowAllAccessControl; otherwise Analyzer will check SELECT permission on the materialized view, which is not necessary.
            StatementAnalyzer viewAnalyzer = new StatementAnalyzer(analysis, metadata, sqlParser, new AllowAllAccessControl(), session, warningCollector);
            Scope viewScope = viewAnalyzer.analyze(refreshMaterializedView.getTarget(), scope);

            Map<SchemaTableName, Expression> tablePredicates = getTablePredicatesForMaterializedViewRefresh(
                    session, refreshMaterializedView, viewName, viewScope, metadata);

            SchemaTableName baseTableName = toSchemaTableName(createQualifiedObjectName(session, baseTable, baseTable.getName(), metadata));
            if (tablePredicates.containsKey(baseTableName)) {
                Query tableSubquery = buildTableQueryWithPredicate(baseTable, tablePredicates.get(baseTableName));
                analysis.registerNamedQuery(baseTable, tableSubquery, true);

                Scope subqueryScope = process(tableSubquery, scope);

                return Optional.of(subqueryScope.getRelationType().withAlias(baseTableName.getTableName(), null));
            }

            return Optional.empty();
        }

        private Map<SchemaTableName, Expression> getTablePredicatesForMaterializedViewRefresh(
                Session session,
                RefreshMaterializedView node,
                QualifiedObjectName viewName,
                Scope viewScope,
                Metadata metadata)
        {
            if (isLegacyMaterializedViews(session)) {
                // There are some duplicated logic for where.isPresent condition across existing/rfc approaches, but let's keep them separate
                // for now so it's cleaner for the two paths
                if (!node.getWhere().isPresent()) {
                    return analyzeAutoRefreshMaterializedView(node, viewName);
                }
                else {
                    return extractTablePredicates(viewName, node.getWhere().get(), viewScope, metadata, session);
                }
            }
            else {
                if (node.getWhere().isPresent()) {
                    throw new SemanticException(NOT_SUPPORTED, node, "WHERE clause in REFRESH MATERIALIZED VIEW is not supported. " +
                            "Connectors automatically determine which data needs refreshing based on staleness detection.");
                }
                return ImmutableMap.of();
            }
        }

        private Query buildTableQueryWithPredicate(Table table, Expression predicate)
        {
            Query query = simpleQuery(selectList(new AllColumns()), table, predicate);
            String formattedSql = formatSql(query, Optional.empty());
            return (Query) sqlParser.createStatement(formattedSql, createParsingOptions(session, warningCollector));
        }

        private Query buildSubqueryWithPredicate(Query originalQuery, Expression predicate)
        {
            Query query = simpleQuery(selectList(new AllColumns()), new TableSubquery(originalQuery), predicate);
            return (Query) sqlParser.createStatement(
                    getFormattedSql(query, sqlParser, Optional.empty()),
                    createParsingOptions(session, warningCollector));
        }

        @Override
        protected Scope visitCreateFunction(CreateFunction node, Optional<Scope> scope)
        {
            analysis.setUpdateInfo(node.getUpdateInfo());

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
            Type returnType = functionAndTypeResolver.getType(parseTypeSignature(node.getReturnType()));
            List<Field> fields = node.getParameters().stream()
                    .map(parameter -> Field.newUnqualified(parameter.getName().getLocation(), parameter.getName().getValue(), functionAndTypeResolver.getType(parseTypeSignature(parameter.getType()))))
                    .collect(toImmutableList());
            Scope functionScope = Scope.builder()
                    .withRelationType(RelationId.anonymous(), new RelationType(fields))
                    .build();
            if (node.getBody() instanceof Return) {
                Expression returnExpression = ((Return) node.getBody()).getExpression();
                Type bodyType = analyzeExpression(returnExpression, functionScope).getExpressionTypes().get(NodeRef.of(returnExpression));
                if (!functionAndTypeResolver.canCoerce(bodyType, returnType)) {
                    throw new SemanticException(TYPE_MISMATCH, node, "Function implementation type '%s' does not match declared return type '%s'", bodyType, returnType);
                }

                verifyNoAggregateWindowOrGroupingFunctions(analysis.getFunctionHandles(), functionAndTypeResolver, returnExpression, "CREATE FUNCTION body");
                verifyNoExternalFunctions(analysis.getFunctionHandles(), functionAndTypeResolver, returnExpression, "CREATE FUNCTION body");

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
            analysis.setUpdateInfo(node.getUpdateInfo());
            return createAndAssignScope(node, scope);
        }

        @Override
        protected Scope visitCreateSchema(CreateSchema node, Optional<Scope> scope)
        {
            analysis.setUpdateInfo(node.getUpdateInfo());
            validateProperties(node.getProperties(), scope);
            return createAndAssignScope(node, scope);
        }

        @Override
        protected Scope visitDropSchema(DropSchema node, Optional<Scope> scope)
        {
            analysis.setUpdateInfo(node.getUpdateInfo());
            return createAndAssignScope(node, scope);
        }

        @Override
        protected Scope visitRenameSchema(RenameSchema node, Optional<Scope> scope)
        {
            analysis.setUpdateInfo(node.getUpdateInfo());
            return createAndAssignScope(node, scope);
        }

        @Override
        protected Scope visitCreateTable(CreateTable node, Optional<Scope> scope)
        {
            analysis.setUpdateInfo(node.getUpdateInfo());
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
        protected Scope visitTruncateTable(TruncateTable node, Optional<Scope> scope)
        {
            analysis.setUpdateInfo(node.getUpdateInfo());
            return createAndAssignScope(node, scope);
        }

        @Override
        protected Scope visitDropTable(DropTable node, Optional<Scope> scope)
        {
            analysis.setUpdateInfo(node.getUpdateInfo());
            return createAndAssignScope(node, scope);
        }

        @Override
        protected Scope visitRenameTable(RenameTable node, Optional<Scope> scope)
        {
            analysis.setUpdateInfo(node.getUpdateInfo());
            return createAndAssignScope(node, scope);
        }

        @Override
        protected Scope visitSetProperties(SetProperties node, Optional<Scope> scope)
        {
            return createAndAssignScope(node, scope);
        }

        @Override
        protected Scope visitRenameColumn(RenameColumn node, Optional<Scope> scope)
        {
            analysis.setUpdateInfo(node.getUpdateInfo());
            return createAndAssignScope(node, scope);
        }

        @Override
        protected Scope visitDropColumn(DropColumn node, Optional<Scope> scope)
        {
            analysis.setUpdateInfo(node.getUpdateInfo());
            return createAndAssignScope(node, scope);
        }

        @Override
        protected Scope visitDropConstraint(DropConstraint node, Optional<Scope> scope)
        {
            analysis.setUpdateInfo(node.getUpdateInfo());
            return createAndAssignScope(node, scope);
        }

        @Override
        protected Scope visitAddConstraint(AddConstraint node, Optional<Scope> scope)
        {
            analysis.setUpdateInfo(node.getUpdateInfo());
            return createAndAssignScope(node, scope);
        }

        @Override
        protected Scope visitAlterColumnNotNull(AlterColumnNotNull node, Optional<Scope> scope)
        {
            analysis.setUpdateInfo(node.getUpdateInfo());
            return createAndAssignScope(node, scope);
        }

        @Override
        protected Scope visitRenameView(RenameView node, Optional<Scope> scope)
        {
            analysis.setUpdateInfo(node.getUpdateInfo());
            return createAndAssignScope(node, scope);
        }

        @Override
        protected Scope visitDropView(DropView node, Optional<Scope> scope)
        {
            analysis.setUpdateInfo(node.getUpdateInfo());
            return createAndAssignScope(node, scope);
        }

        @Override
        protected Scope visitDropMaterializedView(DropMaterializedView node, Optional<Scope> scope)
        {
            analysis.setUpdateInfo(node.getUpdateInfo());
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
        protected Scope visitCall(Call call, Optional<Scope> scope)
        {
            if (analysis.isDescribe()) {
                return createAndAssignScope(call, scope);
            }
            Optional<QualifiedObjectName> procedureNameOptional = analysis.getProcedureName();
            QualifiedObjectName procedureName;
            if (!procedureNameOptional.isPresent()) {
                procedureName = createQualifiedObjectName(session, call, call.getName(), metadata);
                analysis.setProcedureName(Optional.of(procedureName));
            }
            else {
                procedureName = procedureNameOptional.get();
            }
            ConnectorId connectorId = metadata.getCatalogHandle(session, procedureName.getCatalogName())
                    .orElseThrow(() -> new SemanticException(MISSING_CATALOG, call, "Catalog %s does not exist", procedureName.getCatalogName()));

            if (!metadata.getProcedureRegistry().isDistributedProcedure(connectorId, toSchemaTableName(procedureName))) {
                throw new SemanticException(PROCEDURE_NOT_FOUND, "Distributed procedure not registered: " + procedureName);
            }
            DistributedProcedure procedure = metadata.getProcedureRegistry().resolveDistributed(connectorId, toSchemaTableName(procedureName));
            Object[] values = extractParameterValuesInOrder(call, procedure, metadata, session, analysis.getParameters());

            analysis.setUpdateInfo(call.getUpdateInfo());
            analysis.setDistributedProcedureType(Optional.of(procedure.getType()));
            analysis.setProcedureArguments(Optional.of(values));
            switch (procedure.getType()) {
                case TABLE_DATA_REWRITE:
                    TableDataRewriteDistributedProcedure tableDataRewriteDistributedProcedure = (TableDataRewriteDistributedProcedure) procedure;
                    QualifiedName qualifiedName = QualifiedName.of(tableDataRewriteDistributedProcedure.getSchema(values), tableDataRewriteDistributedProcedure.getTableName(values));
                    QualifiedObjectName tableName = createQualifiedObjectName(session, call, qualifiedName, metadata);

                    String filter = tableDataRewriteDistributedProcedure.getFilter(values);
                    Expression filterExpression = sqlParser.createExpression(filter);
                    QuerySpecification querySpecification = new QuerySpecification(
                            selectList(new AllColumns()),
                            Optional.of(new Table(qualifiedName)),
                            Optional.of(filterExpression),
                            Optional.empty(),
                            Optional.empty(),
                            Optional.empty(),
                            Optional.empty(),
                            Optional.empty());
                    analyze(querySpecification, scope);
                    analysis.setTargetQuery(querySpecification);

                    TableHandle tableHandle = metadata.getHandleVersion(session, tableName, Optional.empty())
                            .orElseThrow(() -> (new SemanticException(MISSING_TABLE, call, "Table '%s' does not exist", tableName)));
                    analysis.setCallTarget(tableHandle);
                    break;
                default:
                    throw new PrestoException(StandardErrorCode.NOT_SUPPORTED, "Unsupported distributed procedure type: " + procedure.getType());
            }
            return createAndAssignScope(call, scope, Field.newUnqualified(Optional.empty(), "rows", BIGINT));
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
                if (names.contains(identifier.getValueLowerCase())) {
                    throw new SemanticException(DUPLICATE_COLUMN_NAME, identifier, "Column name '%s' specified more than once", identifier.getValue());
                }
                names.add(identifier.getValueLowerCase());
            }
        }

        private void validateBaseTables(List<Table> baseTables, Node node)
        {
            for (Table baseTable : baseTables) {
                QualifiedObjectName baseName = createQualifiedObjectName(session, baseTable, baseTable.getName(), metadata);

                Optional<MaterializedViewDefinition> optionalMaterializedView = getMaterializedViewDefinition(session, metadataResolver, analysis.getMetadataHandle(), baseName);
                if (optionalMaterializedView.isPresent()) {
                    throw new SemanticException(
                            NOT_SUPPORTED,
                            baseTable,
                            "%s on a materialized view %s is not supported.",
                            node.getClass().getSimpleName(), baseName);
                }
            }
        }

        @Override
        protected Scope visitExplain(Explain node, Optional<Scope> scope)
                throws SemanticException
        {
            checkState(node.isAnalyze(), "Non analyze explain should be rewritten to Query");
            List<ExplainFormat.Type> formats = node.getOptions().stream()
                    .filter(option -> option instanceof ExplainFormat)
                    .map(ExplainFormat.class::cast)
                    .map(ExplainFormat::getType)
                    .collect(toImmutableList());
            checkState(formats.size() <= 1, "only a single format option is supported in EXPLAIN ANALYZE");
            formats.stream().findFirst().ifPresent(format -> checkState(format.equals(TEXT) || format.equals(JSON),
                    "only TEXT and JSON formats are supported in EXPLAIN ANALYZE"));
            checkState(node.getOptions().stream()
                    .filter(option -> option instanceof ExplainType)
                    .findFirst()
                    .map(ExplainType.class::cast)
                    .map(ExplainType::getType)
                    .orElse(DISTRIBUTED).equals(DISTRIBUTED), "only DISTRIBUTED type is supported in EXPLAIN ANALYZE");
            process(node.getStatement(), scope);
            analysis.setUpdateInfo(null);
            return createAndAssignScope(node, scope, Field.newUnqualified(node.getLocation(), "Query Plan", VARCHAR));
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

            if (node.getOffset().isPresent()) {
                analyzeOffset(node.getOffset().get());
            }
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
                if (!expressionAnalysis.getScalarSubqueries().isEmpty()) {
                    throw new SemanticException(
                            NOT_SUPPORTED,
                            node,
                            "Scalar subqueries in UNNEST are not supported");
                }
                Type expressionType = expressionAnalysis.getType(expression);
                if (expressionType instanceof ArrayType) {
                    Type elementType = ((ArrayType) expressionType).getElementType();
                    if (!SystemSessionProperties.isLegacyUnnest(session) && elementType instanceof RowType) {
                        ((RowType) elementType).getFields().stream()
                                .map(field -> Field.newUnqualified(expression.getLocation(), field.getName(), field.getType()))
                                .forEach(outputFields::add);
                    }
                    else {
                        outputFields.add(Field.newUnqualified(expression.getLocation(), Optional.empty(), elementType));
                    }
                }
                else if (expressionType instanceof MapType) {
                    outputFields.add(Field.newUnqualified(expression.getLocation(), Optional.empty(), ((MapType) expressionType).getKeyType()));
                    outputFields.add(Field.newUnqualified(expression.getLocation(), Optional.empty(), ((MapType) expressionType).getValueType()));
                }
                else {
                    throw new PrestoException(StandardErrorCode.INVALID_FUNCTION_ARGUMENT, "Cannot unnest type: " + expressionType);
                }
            }
            if (node.isWithOrdinality()) {
                outputFields.add(Field.newUnqualified(node.getLocation(), Optional.empty(), BIGINT));
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
        protected Scope visitTableFunctionInvocation(TableFunctionInvocation node, Optional<Scope> scope)
        {
            TableFunctionMetadata tableFunctionMetadata = metadata.getFunctionAndTypeManager().resolveTableFunction(session, node.getName());
            if (tableFunctionMetadata == null) {
                throw new SemanticException(FUNCTION_NOT_FOUND, node, "Table function %s not registered", node.getName());
            }

            ConnectorTableFunction function = tableFunctionMetadata.getFunction();
            ConnectorId connectorId = tableFunctionMetadata.getConnectorId();

            ArgumentsAnalysis argumentsAnalysis = analyzeArguments(node, function.getArguments(), scope);

            TransactionManager transactionManager = metadata.getFunctionAndTypeManager().getTransactionManager();
            CatalogMetadata registrationCatalogMetadata = transactionManager.getOptionalCatalogMetadata(session.getRequiredTransactionId(), connectorId.getCatalogName()).orElseThrow(() -> new IllegalStateException("Missing catalog metadata"));
            // a call to getRequiredCatalogHandle() is necessary so that the catalog is recorded by the TransactionManager
            ConnectorTransactionHandle transactionHandle = transactionManager.getConnectorTransaction(
                    session.getRequiredTransactionId(), registrationCatalogMetadata.getConnectorId());

            TableFunctionAnalysis functionAnalysis = function.analyze(session.toConnectorSession(connectorId), transactionHandle, argumentsAnalysis.getPassedArguments());
            List<List<String>> copartitioningLists = analyzeCopartitioning(node.getCopartitioning(), argumentsAnalysis.getTableArgumentAnalyses());

            // determine the result relation type per SQL standard ISO/IEC 9075-2, 4.33 SQL-invoked routines, p. 123, 413, 414
            ReturnTypeSpecification returnTypeSpecification = function.getReturnTypeSpecification();
            if (returnTypeSpecification == GENERIC_TABLE || !argumentsAnalysis.getTableArgumentAnalyses().isEmpty()) {
                analysis.addPolymorphicTableFunction(node);
            }
            Optional<Descriptor> analyzedProperColumnsDescriptor = functionAnalysis.getReturnedType();
            Descriptor properColumnsDescriptor = verifyProperColumnsDescriptor(node, function, returnTypeSpecification, analyzedProperColumnsDescriptor);

            Map<String, TableArgumentAnalysis> tableArgumentsByName = argumentsAnalysis.getTableArgumentAnalyses().stream()
                    .collect(toImmutableMap(TableArgumentAnalysis::getArgumentName, Function.identity()));
            verifyRequiredColumns(node, functionAnalysis.getRequiredColumns(), tableArgumentsByName);

            // The result relation type of a table function consists of:
            // 1. columns created by the table function, called the proper columns.
            // 2. passed columns from input tables:
            // - for tables with the "pass through columns" option, these are all columns of the table,
            // - for tables without the "pass through columns" option, these are the partitioning columns of the table, if any.
            ImmutableList.Builder<Field> fields = ImmutableList.builder();

            // proper columns first
            if (properColumnsDescriptor != null) {
                properColumnsDescriptor.getFields().stream()
                        // per spec, field names are mandatory. We support anonymous fields.
                        .map(field -> Field.newUnqualified(Optional.empty(), field.getName(), field.getType().orElseThrow(() -> new IllegalStateException("missing returned type for proper field"))))
                        .forEach(fields::add);
            }

            // next, columns derived from table arguments, in order of argument declarations
            List<String> tableArgumentNames = function.getArguments().stream()
                    .filter(argumentSpecification -> argumentSpecification instanceof TableArgumentSpecification)
                    .map(ArgumentSpecification::getName)
                    .collect(toImmutableList());

            // table arguments in order of argument declarations
            ImmutableList.Builder<TableArgumentAnalysis> orderedTableArguments = ImmutableList.builder();

            for (String name : tableArgumentNames) {
                TableArgumentAnalysis argument = tableArgumentsByName.get(name);
                orderedTableArguments.add(argument);
                Scope argumentScope = analysis.getScope(argument.getRelation());
                if (argument.isPassThroughColumns()) {
                    argumentScope.getRelationType().getAllFields().stream()
                            .forEach(fields::add);
                }
                else if (argument.getPartitionBy().isPresent()) {
                    argument.getPartitionBy().get().stream()
                            .map(expression -> validateAndGetInputField(expression, argumentScope))
                            .forEach(fields::add);
                }
            }

            analysis.setTableFunctionAnalysis(node, new TableFunctionInvocationAnalysis(
                    connectorId,
                    function.getName(),
                    argumentsAnalysis.getPassedArguments(),
                    orderedTableArguments.build(),
                    functionAnalysis.getRequiredColumns(),
                    copartitioningLists,
                    properColumnsDescriptor == null ? 0 : properColumnsDescriptor.getFields().size(),
                    functionAnalysis.getHandle(),
                    transactionHandle));

            return createAndAssignScope(node, scope, fields.build());
        }

        private void verifyRequiredColumns(TableFunctionInvocation node, Map<String, List<Integer>> requiredColumns, Map<String, TableArgumentAnalysis> tableArgumentsByName)
        {
            Set<String> allInputs = ImmutableSet.copyOf(tableArgumentsByName.keySet());
            requiredColumns.forEach((name, columns) -> {
                if (!allInputs.contains(name)) {
                    throw new SemanticException(TABLE_FUNCTION_IMPLEMENTATION_ERROR, "Table function %s specifies required columns from table argument %s which cannot be found", node.getName(), name);
                }
                if (columns.isEmpty()) {
                    throw new SemanticException(TABLE_FUNCTION_IMPLEMENTATION_ERROR, "Table function %s specifies empty list of required columns from table argument %s", node.getName(), name);
                }
                // the scope is recorded, because table arguments are already analyzed
                Scope inputScope = analysis.getScope(tableArgumentsByName.get(name).getRelation());
                columns.stream()
                        .filter(column -> column < 0 || column >= inputScope.getRelationType().getVisibleFieldCount())
                        .findFirst()
                        .ifPresent(column -> {
                            throw new SemanticException(TABLE_FUNCTION_IMPLEMENTATION_ERROR, "Invalid index: %s of required column from table argument %s", column, name);
                        });
            });
            Set<String> requiredInputs = ImmutableSet.copyOf(requiredColumns.keySet());
            allInputs.stream()
                    .filter(input -> !requiredInputs.contains(input))
                    .findFirst()
                    .ifPresent(input -> {
                        throw new SemanticException(TABLE_FUNCTION_IMPLEMENTATION_ERROR, "Table function %s does not specify required input columns from table argument %s", node.getName(), input);
                    });
        }

        private Descriptor verifyProperColumnsDescriptor(TableFunctionInvocation node, ConnectorTableFunction function, ReturnTypeSpecification returnTypeSpecification, Optional<Descriptor> analyzedProperColumnsDescriptor)
        {
            switch (returnTypeSpecification.getReturnType()) {
                case "PASSTHROUGH":
                    if (analysis.isAliased(node)) {
                        // According to SQL standard ISO/IEC 9075-2, 7.6 <table reference>, p. 409,
                        // table alias is prohibited for a table function with ONLY PASS THROUGH returned type.
                        throw new SemanticException(TABLE_FUNCTION_INVALID_TABLE_FUNCTION_INVOCATION, node, "Alias specified for table function with ONLY PASS THROUGH return type");
                    }
                    if (analyzedProperColumnsDescriptor.isPresent()) {
                        // If a table function has ONLY PASS THROUGH returned type, it does not produce any proper columns,
                        // so the function's analyze() method should not return the proper columns descriptor.
                        throw new SemanticException(TABLE_FUNCTION_AMBIGUOUS_RETURN_TYPE, node, "Returned relation type for table function %s is ambiguous", node.getName());
                    }
                    if (function.getArguments().stream()
                            .filter(TableArgumentSpecification.class::isInstance)
                            .map(TableArgumentSpecification.class::cast)
                            .noneMatch(TableArgumentSpecification::isPassThroughColumns)) {
                        // According to SQL standard ISO/IEC 9075-2, 10.4 <routine invocation>, p. 764,
                        // if there is no generic table parameter that specifies PASS THROUGH, then number of proper columns shall be positive.
                        // For GENERIC_TABLE and DescribedTable returned types, this is enforced by the Descriptor constructor, which requires positive number of fields.
                        // Here we enforce it for the remaining returned type specification: ONLY_PASS_THROUGH.
                        throw new SemanticException(TABLE_FUNCTION_IMPLEMENTATION_ERROR, "A table function with ONLY_PASS_THROUGH return type must have a table argument with pass-through columns.");
                    }
                    return null;
                case "GENERIC":
                    // According to SQL standard ISO/IEC 9075-2, 7.6 <table reference>, p. 409,
                    // table alias is mandatory for a polymorphic table function invocation which produces proper columns.
                    // We don't enforce this requirement.
                    return analyzedProperColumnsDescriptor
                            .orElseThrow(() -> new SemanticException(TABLE_FUNCTION_MISSING_RETURN_TYPE, node, "Cannot determine returned relation type for table function " + node.getName()));
                default:
                    // returned type is statically declared at function declaration and cannot be overridden
                    // According to SQL standard ISO/IEC 9075-2, 7.6 <table reference>, p. 409,
                    // table alias is mandatory for a polymorphic table function invocation which produces proper columns.
                    // We don't enforce this requirement.
                    if (analyzedProperColumnsDescriptor.isPresent()) {
                        // If a table function has statically declared returned type, it is returned in TableFunctionMetadata
                        // so the function's analyze() method should not return the proper columns descriptor.
                        throw new SemanticException(TABLE_FUNCTION_AMBIGUOUS_RETURN_TYPE, node, "Returned relation type for table function %s is ambiguous", node.getName());
                    }
                    return ((DescribedTableReturnTypeSpecification) returnTypeSpecification).getDescriptor();
            }
        }

        private ArgumentsAnalysis analyzeArguments(TableFunctionInvocation node, List<ArgumentSpecification> argumentSpecifications, Optional<Scope> scope)
        {
            List<TableFunctionArgument> arguments = node.getArguments();
            Node errorLocation = node;
            if (!arguments.isEmpty()) {
                errorLocation = arguments.get(0);
            }
            if (argumentSpecifications.size() < arguments.size()) {
                throw new SemanticException(TABLE_FUNCTION_INVALID_ARGUMENTS, errorLocation, "Too many arguments. Expected at most %s arguments, got %s arguments", argumentSpecifications.size(), arguments.size());
            }

            if (argumentSpecifications.isEmpty()) {
                return new ArgumentsAnalysis(ImmutableMap.of(), ImmutableList.of());
            }

            boolean argumentsPassedByName = !arguments.isEmpty() && arguments.stream().allMatch(argument -> argument.getName().isPresent());
            boolean argumentsPassedByPosition = arguments.stream().allMatch(argument -> !argument.getName().isPresent());
            if (!argumentsPassedByName && !argumentsPassedByPosition) {
                throw new SemanticException(TABLE_FUNCTION_INVALID_ARGUMENTS, errorLocation, "All arguments must be passed by name or all must be passed positionally");
            }

            if (argumentsPassedByName) {
                return mapTableFunctionsArgsByName(argumentSpecifications, arguments, errorLocation, scope);
            }
            else {
                return mapTableFunctionArgsByPosition(argumentSpecifications, arguments, errorLocation, scope);
            }
        }

        private ArgumentsAnalysis mapTableFunctionsArgsByName(List<ArgumentSpecification> argumentSpecifications, List<TableFunctionArgument> arguments, Node errorLocation, Optional<Scope> scope)
        {
            ImmutableMap.Builder<String, Argument> passedArguments = ImmutableMap.builder();
            ImmutableList.Builder<TableArgumentAnalysis> tableArgumentAnalyses = ImmutableList.builder();
            Map<String, ArgumentSpecification> argumentSpecificationsByName = new HashMap<>();
            for (ArgumentSpecification argumentSpecification : argumentSpecifications) {
                if (argumentSpecificationsByName.put(argumentSpecification.getName(), argumentSpecification) != null) {
                    // this should never happen, because the argument names are validated at function registration time
                    throw new IllegalStateException("Duplicate argument specification for name: " + argumentSpecification.getName());
                }
            }
            Set<String> uniqueArgumentNames = new HashSet<>();
            for (TableFunctionArgument argument : arguments) {
                String argumentName = argument.getName().orElseThrow(() -> new IllegalStateException("Missing table function argument name")).getCanonicalValue();
                if (!uniqueArgumentNames.add(argumentName)) {
                    throw new SemanticException(TABLE_FUNCTION_INVALID_FUNCTION_ARGUMENT, argument, "Duplicate argument name: %s", argumentName);
                }
                ArgumentSpecification argumentSpecification = argumentSpecificationsByName.remove(argumentName);
                if (argumentSpecification == null) {
                    throw new SemanticException(TABLE_FUNCTION_INVALID_FUNCTION_ARGUMENT, argument, "Unexpected argument name: %s", argumentName);
                }
                ArgumentAnalysis argumentAnalysis = analyzeArgument(argumentSpecification, argument, scope);
                passedArguments.put(argumentSpecification.getName(), argumentAnalysis.getArgument());
                argumentAnalysis.getTableArgumentAnalysis().ifPresent(tableArgumentAnalyses::add);
            }
            // apply defaults for not specified arguments
            for (Map.Entry<String, ArgumentSpecification> entry : argumentSpecificationsByName.entrySet()) {
                ArgumentSpecification argumentSpecification = entry.getValue();
                passedArguments.put(argumentSpecification.getName(), analyzeDefault(argumentSpecification, errorLocation));
            }
            return new ArgumentsAnalysis(passedArguments.buildOrThrow(), tableArgumentAnalyses.build());
        }

        private ArgumentsAnalysis mapTableFunctionArgsByPosition(List<ArgumentSpecification> argumentSpecifications, List<TableFunctionArgument> arguments, Node errorLocation, Optional<Scope> scope)
        {
            ImmutableMap.Builder<String, Argument> passedArguments = ImmutableMap.builder();
            ImmutableList.Builder<TableArgumentAnalysis> tableArgumentAnalyses = ImmutableList.builder();
            for (int i = 0; i < arguments.size(); i++) {
                TableFunctionArgument argument = arguments.get(i);
                ArgumentSpecification argumentSpecification = argumentSpecifications.get(i); // TODO args passed positionally - can one only pass some prefix of args?
                ArgumentAnalysis argumentAnalysis = analyzeArgument(argumentSpecification, argument, scope);
                passedArguments.put(argumentSpecification.getName(), argumentAnalysis.getArgument());
                argumentAnalysis.getTableArgumentAnalysis().ifPresent(tableArgumentAnalyses::add);
            }
            // apply defaults for not specified arguments
            for (int i = arguments.size(); i < argumentSpecifications.size(); i++) {
                ArgumentSpecification argumentSpecification = argumentSpecifications.get(i);
                passedArguments.put(argumentSpecification.getName(), analyzeDefault(argumentSpecification, errorLocation));
            }
            return new ArgumentsAnalysis(passedArguments.buildOrThrow(), tableArgumentAnalyses.build());
        }

        private ArgumentAnalysis analyzeArgument(ArgumentSpecification argumentSpecification, TableFunctionArgument argument, Optional<Scope> scope)
        {
            String actualType = getArgumentTypeString(argument);
            switch (argumentSpecification.getArgumentType()) {
                case TableArgumentSpecification.argumentType:
                    return analyzeTableArgument(argument, (TableArgumentSpecification) argumentSpecification, scope, actualType);
                case DescriptorArgumentSpecification.argumentType:
                    return analyzeDescriptorArgument(argument, (DescriptorArgumentSpecification) argumentSpecification, actualType);
                case ScalarArgumentSpecification.argumentType:
                    return analyzeScalarArgument(argument, argumentSpecification, actualType);
                default:
                    throw new IllegalStateException("Unexpected argument specification: " + argumentSpecification.getClass().getSimpleName());
            }
        }

        private Argument analyzeDefault(ArgumentSpecification argumentSpecification, Node errorLocation)
        {
            if (argumentSpecification.isRequired()) {
                throw new SemanticException(TABLE_FUNCTION_MISSING_ARGUMENT, errorLocation, "Missing argument: " + argumentSpecification.getName());
            }

            checkArgument(!(argumentSpecification instanceof TableArgumentSpecification), "invalid table argument specification: default set");

            if (argumentSpecification instanceof DescriptorArgumentSpecification) {
                return DescriptorArgument.builder()
                        .descriptor((Descriptor) argumentSpecification.getDefaultValue())
                        .build();
            }
            if (argumentSpecification instanceof ScalarArgumentSpecification) {
                return ScalarArgument.builder()
                        .type(((ScalarArgumentSpecification) argumentSpecification).getType())
                        .value(argumentSpecification.getDefaultValue())
                        .build();
            }

            throw new IllegalStateException("Unexpected argument specification: " + argumentSpecification.getClass().getSimpleName());
        }

        private String getArgumentTypeString(TableFunctionArgument argument)
        {
            try {
                return argument.getValue().getArgumentTypeString();
            }
            catch (IllegalArgumentException e) {
                throw new SemanticException(TABLE_FUNCTION_INVALID_FUNCTION_ARGUMENT, argument, "Unexpected table function argument type: ", argument.getClass().getSimpleName());
            }
        }

        private ArgumentAnalysis analyzeScalarArgument(TableFunctionArgument argument, ArgumentSpecification argumentSpecification, String actualType)
        {
            Type type = ((ScalarArgumentSpecification) argumentSpecification).getType();
            if (!(argument.getValue() instanceof Expression)) {
                throw new SemanticException(TABLE_FUNCTION_INVALID_FUNCTION_ARGUMENT, argument, "Invalid argument %s. Expected expression, got %s", argumentSpecification.getName(), actualType);
            }
            Expression expression = (Expression) argument.getValue();
            // 'descriptor' as a function name is not allowed in this context
            if (expression instanceof FunctionCall && ((FunctionCall) expression).getName().hasSuffix(QualifiedName.of("descriptor"))) { // function name is always compared case-insensitive
                throw new SemanticException(TABLE_FUNCTION_INVALID_FUNCTION_ARGUMENT, argument, "'descriptor' function is not allowed as a table function argument");
            }
            // inline parameters
            Expression inlined = ExpressionTreeRewriter.rewriteWith(new ExpressionRewriter<Void>()
            {
                @Override
                public Expression rewriteParameter(Parameter node, Void context, ExpressionTreeRewriter<Void> treeRewriter)
                {
                    if (analysis.isDescribe()) {
                        // We cannot handle DESCRIBE when a table function argument involves a parameter.
                        // In DESCRIBE, the parameter values are not known. We cannot pass a dummy value for a parameter.
                        // The value of a table function argument can affect the returned relation type. The returned
                        // relation type can affect the assumed types for other parameters in the query.
                        throw new SemanticException(NOT_SUPPORTED, node, "DESCRIBE is not supported if a table function uses parameters");
                    }
                    return analysis.getParameters().get(NodeRef.of(node));
                }
            }, expression);
            // currently, only constant arguments are supported
            Object constantValue = ExpressionInterpreter.evaluateConstantExpression(inlined, type, metadata, session, analysis.getParameters());
            return new ArgumentAnalysis(
                    ScalarArgument.builder()
                            .type(type)
                            .value(constantValue)
                            .build(),
                    Optional.empty());
        }

        private ArgumentAnalysis analyzeTableArgument(TableFunctionArgument argument, TableArgumentSpecification argumentSpecification, Optional<Scope> scope, String actualType)
        {
            if (!(argument.getValue() instanceof TableFunctionTableArgument)) {
                if (argument.getValue() instanceof FunctionCall) {
                    // probably an attempt to pass a table function call, which is not supported, and was parsed as a function call
                    throw new SemanticException(NOT_SUPPORTED, argument, "Invalid table argument %s. Table functions are not allowed as table function arguments", argumentSpecification.getName());
                }
                throw new SemanticException(TABLE_FUNCTION_INVALID_FUNCTION_ARGUMENT, argument, "Invalid argument %s. Expected table, got %s", argumentSpecification.getName(), actualType);
            }
            TableFunctionTableArgument tableArgument = (TableFunctionTableArgument) argument.getValue();

            TableArgument.Builder argumentBuilder = TableArgument.builder();
            TableArgumentAnalysis.Builder analysisBuilder = TableArgumentAnalysis.builder();
            analysisBuilder.withArgumentName(argumentSpecification.getName());

            // process the relation
            Relation relation = tableArgument.getTable();
            analysisBuilder.withRelation(relation);
            Scope argumentScope = process(relation, scope);
            QualifiedName relationName = analysis.getRelationName(relation);
            if (relationName != null) {
                analysisBuilder.withName(relationName);
            }

            argumentBuilder.rowType(RowType.from(argumentScope.getRelationType().getVisibleFields().stream()
                    .map(field -> new RowType.Field(field.getName(), field.getType()))
                    .collect(toImmutableList())));

            // analyze PARTITION BY
            if (tableArgument.getPartitionBy().isPresent()) {
                if (argumentSpecification.isRowSemantics()) {
                    throw new SemanticException(TABLE_FUNCTION_INVALID_FUNCTION_ARGUMENT, argument, "Invalid argument %s. Partitioning specified for table argument with row semantics", argumentSpecification.getName());
                }
                List<Expression> partitionBy = tableArgument.getPartitionBy().get();
                analysisBuilder.withPartitionBy(partitionBy);
                partitionBy.stream()
                        .forEach(partitioningColumn -> {
                            validateAndGetInputField(partitioningColumn, argumentScope);
                            Type type = analyzeExpression(partitioningColumn, argumentScope).getType(partitioningColumn);
                            if (!type.isComparable()) {
                                throw new SemanticException(TYPE_MISMATCH, partitioningColumn, "%s is not comparable, and therefore cannot be used in PARTITION BY", type);
                            }
                        });
                argumentBuilder.partitionBy(partitionBy.stream()
                        // each expression is either an Identifier or a DereferenceExpression
                        .map(Expression::toString)
                        .collect(toImmutableList()));
            }

            // analyze ORDER BY
            if (tableArgument.getOrderBy().isPresent()) {
                if (argumentSpecification.isRowSemantics()) {
                    throw new SemanticException(TABLE_FUNCTION_INVALID_FUNCTION_ARGUMENT, argument, "Invalid argument %s. Ordering specified for table argument with row semantics", argumentSpecification.getName());
                }
                OrderBy orderBy = tableArgument.getOrderBy().get();
                analysisBuilder.withOrderBy(orderBy);
                orderBy.getSortItems().stream()
                        .map(SortItem::getSortKey)
                        .forEach(orderingColumn -> {
                            validateAndGetInputField(orderingColumn, argumentScope);
                            Type type = analyzeExpression(orderingColumn, argumentScope).getType(orderingColumn);
                            if (!type.isOrderable()) {
                                throw new SemanticException(TYPE_MISMATCH, orderingColumn, "%s is not orderable, and therefore cannot be used in ORDER BY", type);
                            }
                        });
                argumentBuilder.orderBy(orderBy.getSortItems().stream()
                        // each sort key is either an Identifier or a DereferenceExpression
                        .map(sortItem -> sortItem.getSortKey().toString())
                        .collect(toImmutableList()));
            }

            // analyze the PRUNE/KEEP WHEN EMPTY property
            boolean pruneWhenEmpty = argumentSpecification.isPruneWhenEmpty();
            if (tableArgument.getEmptyTableTreatment().isPresent()) {
                if (argumentSpecification.isRowSemantics()) {
                    throw new SemanticException(TABLE_FUNCTION_INVALID_FUNCTION_ARGUMENT, tableArgument.getEmptyTableTreatment().get(), "Invalid argument %s. Empty behavior specified for table argument with row semantics", argumentSpecification.getName());
                }
                pruneWhenEmpty = tableArgument.getEmptyTableTreatment().get().getTreatment() == EmptyTableTreatment.Treatment.PRUNE;
            }
            analysisBuilder.withPruneWhenEmpty(pruneWhenEmpty);

            // record remaining properties
            analysisBuilder.withRowSemantics(argumentSpecification.isRowSemantics());
            analysisBuilder.withPassThroughColumns(argumentSpecification.isPassThroughColumns());

            return new ArgumentAnalysis(argumentBuilder.build(), Optional.of(analysisBuilder.build()));
        }

        private ArgumentAnalysis analyzeDescriptorArgument(TableFunctionArgument argument, DescriptorArgumentSpecification argumentSpecification, String actualType)
        {
            if (!(argument.getValue() instanceof TableFunctionDescriptorArgument)) {
                if (argument.getValue() instanceof FunctionCall && ((FunctionCall) argument.getValue()).getName().hasSuffix(QualifiedName.of("descriptor"))) { // function name is always compared case-insensitive
                    // malformed descriptor which parsed as a function call
                    throw new SemanticException(TABLE_FUNCTION_INVALID_FUNCTION_ARGUMENT, argument, "Invalid descriptor argument %s. Descriptors should be formatted as 'DESCRIPTOR(name [type], ...)'", (Object) argumentSpecification.getName());
                }
                throw new SemanticException(TABLE_FUNCTION_INVALID_FUNCTION_ARGUMENT, argument, "Invalid argument %s. Expected descriptor, got %s", argumentSpecification.getName(), actualType);
            }
            return new ArgumentAnalysis(
                    ((TableFunctionDescriptorArgument) argument.getValue()).getDescriptor()
                            .map(descriptor -> DescriptorArgument.builder()
                                    .descriptor(new Descriptor(descriptor.getFields().stream()
                                            .map(field -> new Descriptor.Field(
                                                    field.getName().getCanonicalValue(),
                                                    field.getType().map(type -> {
                                                        try {
                                                            return functionAndTypeResolver.getType(parseTypeSignature(type));
                                                        }
                                                        catch (IllegalArgumentException | UnknownTypeException e) {
                                                            throw new SemanticException(TYPE_MISMATCH, field, "Unknown type: %s", type);
                                                        }
                                                    })))
                                            .collect(toImmutableList())))
                                    .build())
                            .orElse(NULL_DESCRIPTOR),
                    Optional.empty());
        }

        private Field validateAndGetInputField(Expression expression, Scope inputScope)
        {
            QualifiedName qualifiedName;
            if (expression instanceof Identifier) {
                qualifiedName = QualifiedName.of(ImmutableList.of(((Identifier) expression)));
            }
            else if (expression instanceof DereferenceExpression) {
                qualifiedName = getQualifiedName((DereferenceExpression) expression);
            }
            else {
                throw new SemanticException(TABLE_FUNCTION_INVALID_COLUMN_REFERENCE, expression, "Expected column reference. Actual: %s", expression);
            }
            Optional<ResolvedField> field = inputScope.tryResolveField(expression, qualifiedName);
            if (!field.isPresent() || !field.get().isLocal()) {
                throw new SemanticException(TABLE_FUNCTION_COLUMN_NOT_FOUND, expression, "Column %s is not present in the input relation", expression);
            }

            return field.get().getField();
        }

        private List<List<String>> analyzeCopartitioning(List<List<QualifiedName>> copartitioning, List<TableArgumentAnalysis> tableArgumentAnalyses)
        {
            // map table arguments by relation names. usa a multimap, because multiple arguments can have the same value, e.g. input_1 => tpch.tiny.orders, input_2 => tpch.tiny.orders
            ImmutableMultimap.Builder<QualifiedName, TableArgumentAnalysis> unqualifiedInputsBuilder = ImmutableMultimap.builder();
            ImmutableMultimap.Builder<QualifiedName, TableArgumentAnalysis> qualifiedInputsBuilder = ImmutableMultimap.builder();
            tableArgumentAnalyses.stream()
                    .filter(argument -> argument.getName().isPresent())
                    .forEach(argument -> {
                        QualifiedName name = argument.getName().get();
                        if (name.getParts().size() == 1) {
                            unqualifiedInputsBuilder.put(name, argument);
                        }
                        else if (name.getParts().size() == 3) {
                            qualifiedInputsBuilder.put(name, argument);
                        }
                        else {
                            throw new IllegalStateException("relation name should be unqualified or fully qualified");
                        }
                    });
            Multimap<QualifiedName, TableArgumentAnalysis> unqualifiedInputs = unqualifiedInputsBuilder.build();
            Multimap<QualifiedName, TableArgumentAnalysis> qualifiedInputs = qualifiedInputsBuilder.build();

            ImmutableList.Builder<List<String>> copartitionBuilder = ImmutableList.builder();
            Set<String> referencedArguments = new HashSet<>();
            for (List<QualifiedName> nameList : copartitioning) {
                ImmutableList.Builder<TableArgumentAnalysis> copartitionListBuilder = ImmutableList.builder();

                // resolve copartition tables as references to table arguments
                for (QualifiedName name : nameList) {
                    Collection<TableArgumentAnalysis> candidates = emptyList();
                    if (name.getParts().size() == 1) {
                        // try to match unqualified name. it might be a reference to a CTE or an aliased relation
                        candidates = unqualifiedInputs.get(name);
                    }
                    if (candidates.isEmpty()) {
                        // qualify the name using current schema and catalog
                        // Since we lost the Identifier context, create a new one here
                        QualifiedObjectName fullyQualifiedName = createQualifiedObjectName(session, new Identifier(name.getOriginalParts().get(0).getValue()), name, metadata);
                        candidates = qualifiedInputs.get(QualifiedName.of(fullyQualifiedName.getCatalogName(), fullyQualifiedName.getSchemaName(), fullyQualifiedName.getObjectName()));
                    }
                    if (candidates.isEmpty()) {
                        throw new SemanticException(TABLE_FUNCTION_INVALID_COPARTITIONING, name.getOriginalParts().get(0), "No table argument found for name: " + name);
                    }
                    if (candidates.size() > 1) {
                        throw new SemanticException(TABLE_FUNCTION_INVALID_COPARTITIONING, name.getOriginalParts().get(0), "Ambiguous reference: multiple table arguments found for name: " + name);
                    }
                    TableArgumentAnalysis argument = getOnlyElement(candidates);
                    if (!referencedArguments.add(argument.getArgumentName())) {
                        // multiple references to argument in COPARTITION clause are implicitly prohibited by
                        // ISO/IEC TR REPORT 19075-7, p.33, Feature B203, “More than one copartition specification”
                        throw new SemanticException(TABLE_FUNCTION_INVALID_COPARTITIONING, name.getOriginalParts().get(0), "Multiple references to table argument: %s in COPARTITION clause", name);
                    }
                    copartitionListBuilder.add(argument);
                }
                List<TableArgumentAnalysis> copartitionList = copartitionListBuilder.build();

                // analyze partitioning columns
                copartitionList.stream()
                        .filter(argument -> !argument.getPartitionBy().isPresent())
                        .findFirst().ifPresent(unpartitioned -> {
                            throw new SemanticException(TABLE_FUNCTION_INVALID_COPARTITIONING, unpartitioned.getRelation(), "Table %s referenced in COPARTITION clause is not partitioned", unpartitioned.getName().orElseThrow(() -> new IllegalStateException("Missing unpartitioned TableArgumentAnalysis name")));
                        });
                //  TODO(#26147): make sure that copartitioned tables cannot have empty partitioning lists.
                //  ISO/IEC TR REPORT 19075-7, 4.5 Partitioning and ordering, p.25 is not clear: "With copartitioning, the copartitioned table arguments must have the same number of partitioning columns,
                //  and corresponding partitioning columns must be comparable. The DBMS effectively performs a full outer equijoin on the copartitioning columns"
                copartitionList.stream()
                        .filter(argument -> argument.getPartitionBy().orElseThrow(() -> new IllegalStateException("PartitionBy not present in copartitionList")).isEmpty())
                        .findFirst().ifPresent(partitionedOnEmpty -> {
                            // table is partitioned but no partitioning columns are specified (single partition)
                            throw new SemanticException(TABLE_FUNCTION_INVALID_COPARTITIONING, partitionedOnEmpty.getRelation(), "No partitioning columns specified for table %s referenced in COPARTITION clause", partitionedOnEmpty.getName().orElseThrow(() -> new IllegalStateException("Missing partitionedOnEmpty TableArgumentAnalysis name")));
                        });
                List<List<Expression>> partitioningColumns = copartitionList.stream()
                        .map(TableArgumentAnalysis::getPartitionBy)
                        .map(opt -> opt.orElseThrow(() -> new IllegalStateException("PartitionBy not present in partitioningColumns")))
                        .collect(toImmutableList());
                if (partitioningColumns.stream()
                        .map(List::size)
                        .distinct()
                        .count() > 1) {
                    throw new SemanticException(TABLE_FUNCTION_INVALID_COPARTITIONING, nameList.get(0).getOriginalParts().get(0), "Numbers of partitioning columns in copartitioned tables do not match");
                }

                // coerce corresponding copartition columns to common supertype
                for (int index = 0; index < partitioningColumns.get(0).size(); index++) {
                    Type commonSuperType = analysis.getType(partitioningColumns.get(0).get(index));
                    // find common supertype
                    for (List<Expression> columnList : partitioningColumns) {
                        Optional<Type> superType = functionAndTypeResolver.getCommonSuperType(commonSuperType, analysis.getType(columnList.get(index)));
                        if (!superType.isPresent()) {
                            throw new SemanticException(TYPE_MISMATCH, nameList.get(0).getOriginalParts().get(0), "Partitioning columns in copartitioned tables have incompatible types");
                        }
                        commonSuperType = superType.get();
                    }
                    for (List<Expression> columnList : partitioningColumns) {
                        Expression column = columnList.get(index);
                        Type type = analysis.getType(column);
                        if (!type.equals(commonSuperType)) {
                            if (!functionAndTypeResolver.canCoerce(type, commonSuperType)) {
                                throw new SemanticException(TYPE_MISMATCH, column, "Cannot coerce column of type %s to common supertype: %s", type.getDisplayName(), commonSuperType.getDisplayName());
                            }
                            analysis.addCoercion(column, commonSuperType, functionAndTypeResolver.isTypeOnlyCoercion(type, commonSuperType));
                        }
                    }
                }

                // record the resolved copartition arguments by argument names
                copartitionBuilder.add(copartitionList.stream()
                        .map(TableArgumentAnalysis::getArgumentName)
                        .collect(toImmutableList()));
            }

            return copartitionBuilder.build();
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
                    analysis.registerNamedQuery(table, query, false);
                    analysis.setRelationName(table, table.getName());

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
                            Field field = Field.newQualified(
                                    columnName.getLocation(),
                                    QualifiedName.of(name),
                                    Optional.of(columnName.getValue()),
                                    inputField.getType(),
                                    false,
                                    inputField.getOriginTable(),
                                    inputField.getOriginColumnName(),
                                    inputField.isAliased());
                            fieldBuilder.add(field);
                            analysis.addSourceColumns(field, analysis.getSourceColumns(inputField));
                        }

                        fields = fieldBuilder.build();
                    }
                    else {
                        ImmutableList.Builder<Field> fieldBuilder = ImmutableList.builder();
                        for (Field inputField : queryDescriptor.getAllFields()) {
                            Field field = Field.newQualified(
                                    inputField.getNodeLocation(),
                                    QualifiedName.of(name),
                                    inputField.getName(),
                                    inputField.getType(),
                                    inputField.isHidden(),
                                    inputField.getOriginTable(),
                                    inputField.getOriginColumnName(),
                                    inputField.isAliased());
                            fieldBuilder.add(field);
                            analysis.addSourceColumns(field, analysis.getSourceColumns(inputField));
                        }
                        fields = fieldBuilder.build();
                    }

                    return createAndAssignScope(table, scope, fields);
                }
            }

            QualifiedObjectName name = createQualifiedObjectName(session, table, table.getName(), metadata);
            analysis.setRelationName(table, QualifiedName.of(name.getCatalogName(), name.getSchemaName(), name.getObjectName()));
            if (name.getObjectName().isEmpty()) {
                throw new SemanticException(MISSING_TABLE, table, "Table name is empty");
            }
            if (name.getSchemaName().isEmpty()) {
                throw new SemanticException(MISSING_SCHEMA, table, "Schema name is empty");
            }
            analysis.addEmptyColumnReferencesForTable(accessControl, session.getIdentity(), session.getTransactionId(), session.getAccessControlContext(), name);

            Optional<ViewDefinition> optionalView = getViewDefinition(session, metadataResolver, analysis.getMetadataHandle(), name);
            if (optionalView.isPresent()) {
                return processView(table, scope, name, optionalView);
            }

            Optional<MaterializedViewDefinition> optionalMaterializedView = getMaterializedViewDefinition(session, metadataResolver, analysis.getMetadataHandle(), name);
            // Prevent INSERT and CREATE TABLE when selecting from a materialized view.
            if (optionalMaterializedView.isPresent()
                    && (analysis.getStatement() instanceof Insert || analysis.getStatement() instanceof CreateTableAsSelect)) {
                throw new SemanticException(
                        NOT_SUPPORTED,
                        table,
                        "%s by selecting from a materialized view %s is not supported",
                        analysis.getStatement().getClass().getSimpleName(),
                        optionalMaterializedView.get().getTable());
            }
            Statement statement = analysis.getStatement();
            if (optionalMaterializedView.isPresent() && statement instanceof Query) {
                if (isMaterializedViewDataConsistencyEnabled(session) || !isLegacyMaterializedViews(session)) {
                    // When the materialized view has already been expanded, do not process it. Just use it as a table.
                    MaterializedViewAnalysisState materializedViewAnalysisState = analysis.getMaterializedViewAnalysisState(table);

                    if (materializedViewAnalysisState.isNotVisited()) {
                        return processMaterializedView(table, name, scope, optionalMaterializedView.get());
                    }
                    if (materializedViewAnalysisState.isVisited()) {
                        throw new SemanticException(MATERIALIZED_VIEW_IS_RECURSIVE, table, "Materialized view is recursive");
                    }
                }
                else {
                    // when stitching is not enabled, still check permission of each base table
                    MaterializedViewDefinition materializedViewDefinition = optionalMaterializedView.get();
                    analysis.getAccessControlReferences().addMaterializedViewDefinitionReference(name, materializedViewDefinition);

                    Query viewQuery = (Query) sqlParser.createStatement(
                            materializedViewDefinition.getOriginalSql(),
                            createParsingOptions(session, warningCollector));

                    analysis.registerMaterializedViewForAnalysis(name, table, materializedViewDefinition.getOriginalSql());
                    process(viewQuery, scope);
                    analysis.unregisterMaterializedViewForAnalysis(table);
                }
            }

            TableColumnMetadata tableColumnsMetadata = getTableColumnsMetadata(session, metadataResolver, analysis.getMetadataHandle(), name);
            List<ColumnMetadata> columnsMetadata = tableColumnsMetadata.getColumnsMetadata();
            Optional<TableHandle> tableHandle = getTableHandle(tableColumnsMetadata, table, name, scope);

            Map<String, ColumnHandle> columnHandles = tableColumnsMetadata.getColumnHandles();

            // TODO: discover columns lazily based on where they are needed (to support connectors that can't enumerate all tables)
            ImmutableList.Builder<Field> fields = ImmutableList.builder();

            for (ColumnMetadata column : columnsMetadata) {
                Field field = Field.newQualified(
                        Optional.empty(),
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
                analysis.addSourceColumns(field, ImmutableSet.of(new SourceColumn(name, column.getName())));
            }

            boolean isMergeIntoStatement = statement instanceof Merge && ((Merge) statement).getTargetTable().equals(table);
            if (isMergeIntoStatement) {
                // Add the target table row id field used to process the MERGE command.
                ColumnHandle targetTableRowIdColumnHandle = metadata.getMergeTargetTableRowIdColumnHandle(session, tableHandle.get());
                Type targetTableRowIdType = metadata.getColumnMetadata(session, tableHandle.get(), targetTableRowIdColumnHandle).getType();
                Field targetTableRowIdField = Field.newUnqualified(table.getLocation(), "$target_table_row_id", targetTableRowIdType);
                fields.add(targetTableRowIdField);
                analysis.setColumn(targetTableRowIdField, targetTableRowIdColumnHandle);
            }

            analysis.registerTable(table, tableHandle.get());

            List<Field> outputFields = fields.build();

            Scope accessControlScope = Scope.builder()
                    .withRelationType(RelationId.anonymous(), new RelationType(outputFields))
                    .build();
            analyzeFiltersAndMasks(table, name, accessControlScope, columnsMetadata);

            analysis.registerTable(table, tableHandle.get());

            if (statement instanceof RefreshMaterializedView) {
                Table view = ((RefreshMaterializedView) statement).getTarget();
                if (!table.equals(view) && !analysis.hasTableRegisteredForMaterializedView(view, table)) {
                    analysis.registerTableForMaterializedView(view, table);
                    Optional<RelationType> descriptor = analyzeBaseTableForRefreshMaterializedView(table, scope);
                    analysis.unregisterTableForMaterializedView(view, table);

                    if (descriptor.isPresent()) {
                        return createAndAssignScope(table, scope, descriptor.get());
                    }
                }
            }

            Scope tableScope = createAndAssignScope(table, scope, outputFields);

            if (isMergeIntoStatement) {
                // Set the target table row id field reference used to process the MERGE command.
                FieldReference targetTableRowIdFieldReference = new FieldReference(outputFields.size() - 1);
                analyzeExpression(targetTableRowIdFieldReference, tableScope);
                analysis.setRowIdField(table, targetTableRowIdFieldReference);
            }

            return tableScope;
        }

        private Optional<TableHandle> getTableHandle(TableColumnMetadata tableColumnsMetadata, Table table, QualifiedObjectName name, Optional<Scope> scope)
        {
            // Process table version AS OF/BEFORE expression
            if (table.getTableVersionExpression().isPresent()) {
                return processTableVersion(table, name, scope);
            }
            else {
                return tableColumnsMetadata.getTableHandle();
            }
        }

        private VersionOperator toVersionOperator(TableVersionOperator operator)
        {
            switch (operator) {
                case EQUAL:
                    return VersionOperator.EQUAL;
                case LESS_THAN:
                    return VersionOperator.LESS_THAN;
            }
            throw new SemanticException(NOT_SUPPORTED, "Table version operator %s not supported." + operator);
        }

        private VersionType toVersionType(TableVersionType type)
        {
            switch (type) {
                case TIMESTAMP:
                    return VersionType.TIMESTAMP;
                case VERSION:
                    return VersionType.VERSION;
            }
            throw new SemanticException(NOT_SUPPORTED, "Table version type %s not supported." + type);
        }
        private Optional<TableHandle> processTableVersion(Table table, QualifiedObjectName name, Optional<Scope> scope)
        {
            Expression stateExpr = table.getTableVersionExpression().get().getStateExpression();
            TableVersionType tableVersionType = table.getTableVersionExpression().get().getTableVersionType();
            TableVersionOperator tableVersionOperator = table.getTableVersionExpression().get().getTableVersionOperator();
            ExpressionAnalysis expressionAnalysis = analyzeExpression(stateExpr, scope.get());
            analysis.recordSubqueries(table, expressionAnalysis);
            Type stateExprType = expressionAnalysis.getType(stateExpr);
            if (stateExprType == UNKNOWN) {
                throw new PrestoException(StandardErrorCode.INVALID_ARGUMENTS, format("Table version AS OF/BEFORE expression cannot be NULL for %s", name.toString()));
            }
            Object evalStateExpr = evaluateConstantExpression(stateExpr, stateExprType, metadata, session, analysis.getParameters());
            if (tableVersionType == TIMESTAMP) {
                if (!(stateExprType instanceof TimestampWithTimeZoneType || stateExprType instanceof TimestampType)) {
                    throw new SemanticException(TYPE_MISMATCH, stateExpr,
                            "Type %s is invalid. Supported table version AS OF/BEFORE expression type is Timestamp or Timestamp with Time Zone.",
                            stateExprType.getDisplayName());
                }
            }
            if (tableVersionType == VERSION) {
                if (!(stateExprType instanceof BigintType || stateExprType instanceof VarcharType)) {
                    throw new SemanticException(TYPE_MISMATCH, stateExpr,
                            "Type %s is invalid. Supported table version AS OF/BEFORE expression type is BIGINT or VARCHAR",
                            stateExprType.getDisplayName());
                }
            }

            ConnectorTableVersion tableVersion = new ConnectorTableVersion(toVersionType(tableVersionType), toVersionOperator(tableVersionOperator), stateExprType, evalStateExpr);
            return metadata.getHandleVersion(session, name, Optional.of(tableVersion));
        }

        private Scope getScopeFromTable(Table table, Optional<Scope> scope)
        {
            QualifiedObjectName tableName = createQualifiedObjectName(session, table, table.getName(), metadata);
            TableColumnMetadata tableColumnsMetadata = getTableColumnsMetadata(session, metadataResolver, analysis.getMetadataHandle(), tableName);

            // TODO: discover columns lazily based on where they are needed (to support connectors that can't enumerate all tables)
            ImmutableList.Builder<Field> fields = ImmutableList.builder();

            List<ColumnMetadata> columnsMetadata = tableColumnsMetadata.getColumnsMetadata();

            for (ColumnMetadata columnMetadata : columnsMetadata) {
                Field field = Field.newQualified(
                        Optional.empty(),
                        table.getName(),
                        Optional.of(columnMetadata.getName()),
                        columnMetadata.getType(),
                        columnMetadata.isHidden(),
                        Optional.of(tableName),
                        Optional.of(columnMetadata.getName()),
                        false);
                fields.add(field);
            }

            return createAndAssignScope(table, scope, fields.build());
        }

        private Scope processView(Table table, Optional<Scope> scope, QualifiedObjectName name, Optional<ViewDefinition> optionalView)
        {
            Statement statement = analysis.getStatement();
            if (statement instanceof CreateView) {
                CreateView viewStatement = (CreateView) statement;
                QualifiedObjectName viewNameFromStatement = createQualifiedObjectName(session, viewStatement, viewStatement.getName(), metadata);
                if (viewStatement.isReplace() && viewNameFromStatement.equals(name)) {
                    throw new SemanticException(VIEW_IS_RECURSIVE, table, "Statement would create a recursive view");
                }
            }
            if (analysis.hasTableInView(table)) {
                throw new SemanticException(VIEW_IS_RECURSIVE, table, "View is recursive");
            }
            ViewDefinition view = optionalView.get();

            analysis.getAccessControlReferences().addViewDefinitionReference(name, view);

            Optional<Expression> savedViewAccessorWhereClause = analysis.getCurrentQuerySpecification()
                    .flatMap(QuerySpecification::getWhere);
            savedViewAccessorWhereClause.ifPresent(analysis::setViewAccessorWhereClause);

            Query query = parseView(view.getOriginalSql(), name, table);

            analysis.registerNamedQuery(table, query, true);
            analysis.registerTableForView(table);
            RelationType descriptor = analyzeView(query, name, view.getCatalog(), view.getSchema(), view.getOwner(), table);
            analysis.unregisterTableForView();

            if (savedViewAccessorWhereClause.isPresent()) {
                analysis.clearViewAccessorWhereClause();
            }

            if (isViewStale(view.getColumns(), descriptor.getVisibleFields())) {
                throw new SemanticException(VIEW_IS_STALE, table, "View '%s' is stale; it must be re-created", name);
            }

            // Derive the type of the view from the stored definition, not from the analysis of the underlying query.
            // This is needed in case the underlying table(s) changed and the query in the view now produces types that
            // are implicitly coercible to the declared view types.
            List<Field> outputFields = view.getColumns().stream()
                    .map(column -> Field.newQualified(
                            table.getLocation(),
                            table.getName(),
                            Optional.of(column.getName()),
                            column.getType(),
                            false,
                            Optional.of(name),
                            Optional.of(column.getName()),
                            false))
                    .collect(toImmutableList());

            analysis.addRelationCoercion(table, outputFields.stream().map(Field::getType).toArray(Type[]::new));

            Scope accessControlScope = Scope.builder()
                    .withRelationType(RelationId.anonymous(), new RelationType(outputFields))
                    .build();
            analyzeFiltersAndMasks(table, name, accessControlScope, outputFields);

            return createAndAssignScope(table, scope, outputFields);
        }

        private Scope processMaterializedView(
                Table materializedView,
                QualifiedObjectName materializedViewName,
                Optional<Scope> scope,
                MaterializedViewDefinition materializedViewDefinition)
        {
            MaterializedViewPlanValidator.validate((Query) sqlParser.createStatement(materializedViewDefinition.getOriginalSql(), createParsingOptions(session, warningCollector)));

            analysis.getAccessControlReferences().addMaterializedViewDefinitionReference(materializedViewName, materializedViewDefinition);

            analysis.registerMaterializedViewForAnalysis(materializedViewName, materializedView, materializedViewDefinition.getOriginalSql());

            if (isLegacyMaterializedViews(session)) {
                // Legacy SQL stitching approach: create UNION query with base tables
                String newSql = getMaterializedViewSQL(materializedView, materializedViewName, materializedViewDefinition, scope);

                Query query = (Query) sqlParser.createStatement(newSql, createParsingOptions(session, warningCollector));
                analysis.registerNamedQuery(materializedView, query, true);

                Scope queryScope = process(query, scope);
                RelationType relationType = queryScope.getRelationType().withAlias(materializedViewName.getObjectName(), null);
                analysis.unregisterMaterializedViewForAnalysis(materializedView);

                Scope accessControlScope = Scope.builder()
                        .withRelationType(RelationId.anonymous(), relationType)
                        .build();
                analyzeFiltersAndMasks(materializedView, materializedViewName, accessControlScope, relationType.getAllFields());

                return createAndAssignScope(materializedView, scope, relationType);
            }
            else {
                Query viewQuery = (Query) sqlParser.createStatement(
                        materializedViewDefinition.getOriginalSql(),
                        createParsingOptions(session, warningCollector));

                QualifiedName dataTableName = QualifiedName.of(
                        materializedViewName.getCatalogName(),
                        materializedViewDefinition.getSchema(),
                        materializedViewDefinition.getTable());
                Table dataTable = new Table(dataTableName);

                Analysis.MaterializedViewInfo mvInfo = new Analysis.MaterializedViewInfo(
                        materializedViewName,
                        dataTable,
                        viewQuery,
                        materializedViewDefinition);
                analysis.setMaterializedViewInfo(materializedView, mvInfo);

                // Legacy materialized views are treated as INVOKER rights
                ViewSecurity securityMode = materializedViewDefinition.getSecurityMode().orElse(INVOKER);

                Identity queryIdentity;
                AccessControl queryAccessControl;
                if (securityMode == DEFINER) {
                    Optional<String> owner = materializedViewDefinition.getOwner();
                    if (!owner.isPresent()) {
                        throw new SemanticException(NOT_SUPPORTED, "Owner must be present for DEFINER security mode");
                    }
                    queryIdentity = new Identity(owner.get(), Optional.empty(), session.getIdentity().getExtraCredentials());
                    // For materialized views, use regular access control (not ViewAccessControl)
                    // to check SELECT permissions on base tables, not CREATE VIEW permissions
                    queryAccessControl = accessControl;
                }
                else {
                    queryIdentity = session.getIdentity();
                    queryAccessControl = accessControl;
                }

                Session materializedViewSession = createViewSession(
                        Optional.of(materializedViewName.getCatalogName()),
                        Optional.of(materializedViewDefinition.getSchema()),
                        queryIdentity);

                StatementAnalyzer materializedViewAnalyzer = new StatementAnalyzer(
                        analysis,
                        metadata,
                        sqlParser,
                        queryAccessControl,
                        materializedViewSession,
                        warningCollector);
                materializedViewAnalyzer.analyze(viewQuery, scope);

                Scope queryScope = process(dataTable, scope);
                RelationType relationType = queryScope.getRelationType().withOnlyVisibleFields().withAlias(materializedViewName.getObjectName(), null);
                analysis.unregisterMaterializedViewForAnalysis(materializedView);

                Scope accessControlScope = Scope.builder()
                        .withRelationType(RelationId.anonymous(), relationType)
                        .build();
                analyzeFiltersAndMasks(materializedView, materializedViewName, accessControlScope, relationType.getAllFields());

                return createAndAssignScope(materializedView, scope, relationType);
            }
        }

        private String getMaterializedViewSQL(
                Table materializedView,
                QualifiedObjectName materializedViewName,
                MaterializedViewDefinition materializedViewDefinition,
                Optional<Scope> scope)
        {
            MaterializedViewStatus materializedViewStatus = getMaterializedViewStatus(materializedViewName, scope, materializedView);

            String materializedViewCreateSql = materializedViewDefinition.getOriginalSql();

            if (materializedViewStatus.isNotMaterialized() || materializedViewStatus.isTooManyPartitionsMissing()) {
                session.getRuntimeStats().addMetricValue(SKIP_READING_FROM_MATERIALIZED_VIEW_COUNT, NONE, 1);
                return materializedViewCreateSql;
            }

            Statement createSqlStatement = sqlParser.createStatement(materializedViewCreateSql, createParsingOptions(session, warningCollector));

            Map<SchemaTableName, Expression> baseTablePredicates = emptyMap();
            if (materializedViewStatus.isFullyMaterialized()) {
                // We need to include base table queries by Union in order to add required access control for the base tables and utilized columns during visit.
                // Here we stitch with the predicate WHERE FALSE, and the optimizer will then prune the FALSE branch with no extra overhead introduced.
                baseTablePredicates = generateFalsePredicates(materializedViewDefinition.getBaseTables());
            }
            else if (materializedViewStatus.isPartiallyMaterialized()) {
                baseTablePredicates = generateBaseTablePredicates(materializedViewStatus.getPartitionsFromBaseTables(), metadata);
            }

            Query predicateStitchedQuery = (Query) new PredicateStitcher(session, baseTablePredicates, metadata).process(createSqlStatement, new PredicateStitcherContext());

            // TODO: consider materialized view predicates https://github.com/prestodb/presto/issues/16034
            QuerySpecification materializedViewQuerySpecification = new QuerySpecification(
                    selectList(new AllColumns()),
                    Optional.of(materializedView),
                    Optional.empty(),
                    Optional.empty(),
                    Optional.empty(),
                    Optional.empty(),
                    Optional.empty(),
                    Optional.empty());

            // When union, keep predicateStitchedQuery before materializedViewQuerySpecification. Given Scope of Union contains RelationType of the first Relation,
            // this would allow utilizedTableColumnReferences to trace back to base table columns, which is required for correct materialized view access control.
            Union union = new Union(ImmutableList.of(predicateStitchedQuery.getQueryBody(), materializedViewQuerySpecification), Optional.of(Boolean.FALSE));
            Query unionQuery = new Query(predicateStitchedQuery.getWith(), union, predicateStitchedQuery.getOrderBy(), predicateStitchedQuery.getOffset(), predicateStitchedQuery.getLimit());
            // can we return the above query object, instead of building a query string?
            // in case of returning the query object, make sure to clone the original query object.
            return getFormattedSql(unionQuery, sqlParser, Optional.empty());
        }

        /**
         * Returns materialized view status, considering filter conditions from query
         */
        private MaterializedViewStatus getMaterializedViewStatus(QualifiedObjectName materializedViewName, Optional<Scope> scope, Table table)
        {
            TupleDomain<String> baseQueryDomain = TupleDomain.all();
            // We can use filter from base query WHERE clause to consider only relevant partitions when getting
            // materialized view status
            // TODO: Collect predicates in outer queries and apply the union to this criteria. i.e. in
            //  SELECT x FROM (SELECT x FROM tbl WHERE y < 20) WHERE y > 5 where y is a partition key,
            //  right now we would only remove partitions y >= 20. We should eventually consider y <= 5 as well.

            checkArgument(analysis.getCurrentQuerySpecification().isPresent(), "Current subquery should be set when processing materialized view");
            QuerySpecification currentSubquery = analysis.getCurrentQuerySpecification().get();

            // Collect where clause from both current subquery and possible logical view
            List<Expression> wherePredicates = new ArrayList<>();
            currentSubquery.getWhere().ifPresent(wherePredicates::add);
            analysis.getViewAccessorWhereClause().ifPresent(wherePredicates::add);

            if (!wherePredicates.isEmpty() && isMaterializedViewPartitionFilteringEnabled(session)) {
                Optional<MaterializedViewDefinition> materializedViewDefinition = getMaterializedViewDefinition(session, metadataResolver, analysis.getMetadataHandle(), materializedViewName);
                if (!materializedViewDefinition.isPresent()) {
                    log.warn("Materialized view definition not present as expected when fetching materialized view status");
                    return metadataResolver.getMaterializedViewStatus(materializedViewName, baseQueryDomain);
                }

                Scope sourceScope = getScopeFromTable(table, scope);
                Expression combinedWhereClause = ExpressionUtils.combineConjuncts(wherePredicates);

                // Extract column names from materialized view scope
                Set<QualifiedName> materializedViewColumns = sourceScope.getRelationType().getAllFields().stream()
                        .map(field -> field.getName())
                        .filter(Optional::isPresent)
                        .map(Optional::get)
                        .map(QualifiedName::of)
                        .collect(Collectors.toSet());

                // Only proceed with partition filtering if there are conjuncts that reference MV columns
                List<Expression> conjuncts = ExpressionUtils.extractConjuncts(combinedWhereClause);
                List<Expression> mvConjuncts = conjuncts.stream()
                        .filter(conjunct -> {
                            Set<QualifiedName> referencedColumns = VariablesExtractor.extractNames(conjunct, analysis.getColumnReferences());
                            return !referencedColumns.isEmpty() && referencedColumns.stream().allMatch(materializedViewColumns::contains);
                        })
                        .collect(Collectors.toList());

                if (!mvConjuncts.isEmpty()) {
                    Expression filteredWhereClause = ExpressionUtils.combineConjuncts(mvConjuncts);

                    // Analyze the filtered WHERE clause only for type inference, don't record it in analysis
                    // to avoid preventing the full WHERE clause from being analyzed later
                    ExpressionAnalysis expressionAnalysis = analyzeExpression(filteredWhereClause, sourceScope);

                    DomainTranslator domainTranslator = new RowExpressionDomainTranslator(metadata);
                    RowExpression rowExpression = SqlToRowExpressionTranslator.translate(
                            filteredWhereClause,
                            analysis.getTypes(),
                            ImmutableMap.of(),
                            metadata.getFunctionAndTypeManager(),
                            session);

                    TupleDomain<String> viewQueryDomain = MaterializedViewUtils.getDomainFromFilter(session, domainTranslator, rowExpression);

                    Map<String, Map<SchemaTableName, String>> directColumnMappings = materializedViewDefinition.get().getDirectColumnMappingsAsMap();

                    // Get base query domain we have mapped from view query- if there are not direct mappings, don't filter partition count for predicate
                    boolean mappedToOneTable = true;
                    Map<String, Domain> rewrittenDomain = new HashMap<>();

                    for (Map.Entry<String, Domain> entry : viewQueryDomain.getDomains().orElse(ImmutableMap.of()).entrySet()) {
                        Map<SchemaTableName, String> baseTableMapping = null;
                        for (String columnName : directColumnMappings.keySet()) {
                            if (columnName.equalsIgnoreCase(entry.getKey())) {
                                baseTableMapping = directColumnMappings.get(columnName);
                                break;
                            }
                        }

                        if (baseTableMapping == null || baseTableMapping.size() != 1) {
                            mappedToOneTable = false;
                            break;
                        }

                        String baseColumnName = baseTableMapping.entrySet().stream().findAny().get().getValue();
                        rewrittenDomain.put(baseColumnName, entry.getValue());
                    }

                    if (mappedToOneTable) {
                        baseQueryDomain = TupleDomain.withColumnDomains(rewrittenDomain);
                    }
                }
            }

            return metadataResolver.getMaterializedViewStatus(materializedViewName, baseQueryDomain);
        }

        @Override
        protected Scope visitAliasedRelation(AliasedRelation relation, Optional<Scope> scope)
        {
            analysis.setRelationName(relation, QualifiedName.of(relation.getAlias().getValue()));
            analysis.addAliased(relation.getRelation());
            Scope relationScope = process(relation.getRelation(), scope);

            RelationType relationType = relationScope.getRelationType();

            // special-handle table function invocation
            if (relation.getRelation() instanceof TableFunctionInvocation) {
                return createAndAssignScope(relation, scope,
                        aliasTableFunctionInvocation(relation, relationType, (TableFunctionInvocation) relation.getRelation()));
            }

            // todo this check should be inside of TupleDescriptor.withAlias, but the exception needs the node object
            if (relation.getColumnNames() != null) {
                int totalColumns = relationType.getVisibleFieldCount();
                if (totalColumns != relation.getColumnNames().size()) {
                    throw new SemanticException(MISMATCHED_COLUMN_ALIASES, relation, "Column alias list has %s entries but '%s' has %s columns available", relation.getColumnNames().size(), relation.getAlias(), totalColumns);
                }
            }

            List<String> aliases = null;
            Collection<Field> inputFields = relationType.getAllFields();
            if (relation.getColumnNames() != null) {
                aliases = relation.getColumnNames().stream()
                        .map(Identifier::getValue)
                        .collect(toImmutableList());
                inputFields = relationType.getVisibleFields();
            }

            RelationType descriptor = relationType.withAlias(relation.getAlias().getValue(), aliases);
            checkArgument(inputFields.size() == descriptor.getAllFieldCount(),
                    "Expected %s fields, got %s",
                    descriptor.getAllFieldCount(),
                    inputFields.size());

            Streams.forEachPair(
                    descriptor.getAllFields().stream(),
                    inputFields.stream(),
                    (newField, field) -> analysis.addSourceColumns(newField, analysis.getSourceColumns(field)));

            return createAndAssignScope(relation, scope, descriptor);
        }

        // As described by the SQL standard ISO/IEC 9075-2, 7.6 <table reference>, p. 409
        private RelationType aliasTableFunctionInvocation(AliasedRelation relation, RelationType relationType, TableFunctionInvocation function)
        {
            TableFunctionInvocationAnalysis tableFunctionAnalysis = analysis.getTableFunctionAnalysis(function);
            int properColumnsCount = tableFunctionAnalysis.getProperColumnsCount();

            // check that relation alias is different from range variables of all table arguments
            tableFunctionAnalysis.getTableArgumentAnalyses().stream()
                    .map(TableArgumentAnalysis::getName)
                    .filter(Optional::isPresent)
                    .map(Optional::get)
                    .filter(name -> name.hasSuffix(QualifiedName.of(ImmutableList.of(relation.getAlias()))))
                    .findFirst()
                    .ifPresent(name -> {
                        throw new SemanticException(TABLE_FUNCTION_DUPLICATE_RANGE_VARIABLE, relation.getAlias(), "Relation alias: %s is a duplicate of input table name: %s", relation.getAlias(), name);
                    });

            // build the new relation type. the alias must be applied to the proper columns only,
            // and it must not shadow the range variables exposed by the table arguments
            ImmutableList.Builder<Field> fieldsBuilder = ImmutableList.builder();
            // first, put the table function's proper columns with alias
            if (relation.getColumnNames() != null) {
                // check that number of column aliases matches number of table function's proper columns
                if (properColumnsCount != relation.getColumnNames().size()) {
                    throw new SemanticException(MISMATCHED_COLUMN_ALIASES, relation, "Column alias list has %s entries but table function has %s proper columns", relation.getColumnNames().size(), properColumnsCount);
                }
                for (int i = 0; i < properColumnsCount; i++) {
                    // proper columns are not hidden, so we don't need to skip hidden fields
                    Field field = relationType.getFieldByIndex(i);
                    fieldsBuilder.add(Field.newQualified(
                            field.getNodeLocation(),
                            QualifiedName.of(ImmutableList.of(relation.getAlias())),
                            Optional.of(relation.getColumnNames().get(i).getCanonicalValue()), // although the canonical name is recorded, fields are resolved case-insensitive
                            field.getType(),
                            field.isHidden(),
                            field.getOriginTable(),
                            field.getOriginColumnName(),
                            field.isAliased()));
                }
            }
            else {
                for (int i = 0; i < properColumnsCount; i++) {
                    Field field = relationType.getFieldByIndex(i);
                    fieldsBuilder.add(Field.newQualified(
                            field.getNodeLocation(),
                            QualifiedName.of(ImmutableList.of(relation.getAlias())),
                            field.getName(),
                            field.getType(),
                            field.isHidden(),
                            field.getOriginTable(),
                            field.getOriginColumnName(),
                            field.isAliased()));
                }
            }

            // append remaining fields. They are not being aliased, so hidden fields are included
            for (int i = properColumnsCount; i < relationType.getAllFieldCount(); i++) {
                fieldsBuilder.add(relationType.getFieldByIndex(i));
            }

            List<Field> fields = fieldsBuilder.build();

            // check that there are no duplicate names within the table function's proper columns
            Set<String> names = new HashSet<>();
            fields.subList(0, properColumnsCount).stream()
                    .map(Field::getName)
                    .filter(Optional::isPresent)
                    .map(Optional::get)
                    // field names are resolved case-insensitive
                    .map(name -> name.toLowerCase(ENGLISH))
                    .forEach(name -> {
                        if (!names.add(name)) {
                            throw new SemanticException(DUPLICATE_COLUMN_NAME, relation.getRelation(), "Duplicate name of table function proper column: " + name);
                        }
                    });

            return new RelationType(fields);
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
            try {
                samplePercentageObject = evaluateConstantExpression(relation.getSamplePercentage(), DOUBLE, metadata, session,
                        analysis.getParameters());
            }
            catch (SemanticException e) {
                if (e.getCode() == TYPE_MISMATCH) {
                    throw new SemanticException(NON_NUMERIC_SAMPLE_PERCENTAGE, relation.getSamplePercentage(), "Sample percentage should evaluate to a double");
                }
                throw e;
            }

            double samplePercentageValue = (Double) samplePercentageObject;

            if (samplePercentageValue < 0.0) {
                throw new SemanticException(SemanticErrorCode.SAMPLE_PERCENTAGE_OUT_OF_RANGE, relation.getSamplePercentage(), "Sample percentage must be greater than or equal to 0");
            }
            if ((samplePercentageValue > 100.0)) {
                throw new SemanticException(SemanticErrorCode.SAMPLE_PERCENTAGE_OUT_OF_RANGE, relation.getSamplePercentage(), "Sample percentage must be less than or equal to 100");
            }

            analysis.setSampleRatio(relation, samplePercentageValue / 100);
            Scope relationScope = process(relation.getRelation(), scope);

            // TABLESAMPLE cannot be applied to a polymorphic table function (SQL standard ISO/IEC 9075-2, 7.6 <table reference>, p. 409)
            // Note: the below method finds a table function immediately nested in SampledRelation, or aliased.
            // Potentially, a table function could be also nested with intervening PatternRecognitionRelation.
            // Such case is handled in visitPatternRecognitionRelation().
            validateNoNestedTableFunction(relation.getRelation(), "sample");

            return createAndAssignScope(relation, scope, relationScope.getRelationType());
        }

        // this method should run after the `base` relation is processed, so that it is
        // determined whether the table function is polymorphic
        private void validateNoNestedTableFunction(Relation base, String context)
        {
            TableFunctionInvocation tableFunctionInvocation = null;
            if (base instanceof TableFunctionInvocation) {
                tableFunctionInvocation = (TableFunctionInvocation) base;
            }
            else if (base instanceof AliasedRelation &&
                    ((AliasedRelation) base).getRelation() instanceof TableFunctionInvocation) {
                tableFunctionInvocation = (TableFunctionInvocation) ((AliasedRelation) base).getRelation();
            }
            if (tableFunctionInvocation != null && analysis.isPolymorphicTableFunction(tableFunctionInvocation)) {
                throw new SemanticException(TABLE_FUNCTION_INVALID_TABLE_FUNCTION_INVOCATION, base, "Cannot apply %s to polymorphic table function invocation", context);
            }
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

            analysis.setCurrentSubquery(node);
            Scope sourceScope = analyzeFrom(node, scope);

            if (node.getWhere().isPresent()) {
                Expression predicate = node.getWhere().get();
                // If analysis already contains where clause information for this node, analyzeWhere
                // was already called for this node when fetching materialized view status
                if (analysis.getWhere(node) == null) {
                    analyzeWhere(node, sourceScope, predicate);
                }
            }

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
            if (node.getOffset().isPresent()) {
                analyzeOffset(node.getOffset().get());
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
                List<FunctionCall> orderByAggregations = extractAggregateFunctions(analysis.getFunctionHandles(), orderByExpressions, functionAndTypeResolver);
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
                    Optional<Type> commonSuperType = functionAndTypeResolver.getCommonSuperType(outputFieldTypes[i], descFieldType);
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
                        oldField.getNodeLocation(),
                        oldField.getRelationAlias(),
                        oldField.getName(),
                        outputFieldTypes[i],
                        oldField.isHidden(),
                        oldField.getOriginTable(),
                        oldField.getOriginColumnName(),
                        oldField.isAliased());

                int index = i;
                analysis.addSourceColumns(
                        outputDescriptorFields[index],
                        relationScopes.stream()
                                .map(relationType -> relationType.getRelationType().getFieldByIndex(index))
                                .flatMap(field -> analysis.getSourceColumns(field).stream())
                                .collect(toImmutableSet()));
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

        private boolean isJoinOnConditionReferencesRelatedFields(Expression expression, Scope leftScope, Scope rightScope)
        {
            return ExpressionUtils.extractDisjuncts(expression).stream().allMatch(disjunct -> {
                List<DereferenceExpression> dereferenceExpressions = extractExpressions(Arrays.asList(disjunct), DereferenceExpression.class);
                List<Identifier> identifiers = extractExpressions(Arrays.asList(disjunct), Identifier.class);
                boolean isJoinOnConditionReferencedVariableInLeftScope = dereferenceExpressions.stream().anyMatch(expr -> rightScope.tryResolveField(expr).isPresent()) ||
                        identifiers.stream().anyMatch(expr -> rightScope.tryResolveField(expr).isPresent());
                boolean isJoinOnConditionReferencedVariableInRightScope = dereferenceExpressions.stream().anyMatch(expr -> leftScope.tryResolveField(expr).isPresent()) ||
                        identifiers.stream().anyMatch(expr -> leftScope.tryResolveField(expr).isPresent());
                return isJoinOnConditionReferencedVariableInLeftScope && isJoinOnConditionReferencedVariableInRightScope;
            });
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

                if (expression instanceof LogicalBinaryExpression) {
                    if (((LogicalBinaryExpression) expression).getOperator() == LogicalBinaryExpression.Operator.OR) {
                        String warningMessage = createWarningMessage(expression, "JOIN conditions with an OR can cause performance issues as it may lead to a cross join with filter");
                        warningCollector.add(new PrestoWarning(PERFORMANCE_WARNING, warningMessage));
                    }
                }

                verifyJoinOnConditionReferencesRelatedFields(left, right, expression, node.getRight());
                verifyNoAggregateWindowOrGroupingFunctions(analysis.getFunctionHandles(), functionAndTypeResolver, expression, "JOIN clause");

                analysis.recordSubqueries(node, expressionAnalysis);
                analysis.setJoinCriteria(node, expression);
            }
            else {
                throw new UnsupportedOperationException("unsupported join criteria: " + criteria.getClass().getName());
            }

            return output;
        }

        private void verifyJoinOnConditionReferencesRelatedFields(Scope leftScope, Scope rightScope, Expression expression, Relation rightRelation)
        {
            if (!isJoinOnConditionReferencesRelatedFields(expression, leftScope, rightScope)) {
                Optional<String> tableName = tryGetTableName(rightRelation);
                String warningMessage = tableName.isPresent() ?
                        createWarningMessage(
                                expression,
                                format(
                                        "JOIN ON condition(s) do not reference the joined table '%s' and other tables in the same " +
                                                "expression that can cause performance issues as it may lead to a cross join with filter",
                                        tableName.get())) :
                        createWarningMessage(
                                expression,
                                "JOIN ON condition(s) do not reference the joined relation and other relation in the same " +
                                        "expression that can cause performance issues as it may lead to a cross join with filter");
                warningCollector.add(new PrestoWarning(PERFORMANCE_WARNING, warningMessage));
            }
        }

        private Optional<String> tryGetTableName(Relation relation)
        {
            if (relation instanceof Table) {
                return Optional.of(((Table) relation).getName().toString());
            }
            else if (relation instanceof AliasedRelation) {
                AliasedRelation aliasedRelation = (AliasedRelation) relation;
                if (aliasedRelation.getRelation() instanceof Table) {
                    return Optional.of(((Table) aliasedRelation.getRelation()).getName().toString());
                }
            }
            return Optional.empty();
        }

        private String createWarningMessage(Node node, String description)
        {
            NodeLocation nodeLocation = node.getLocation().get();
            return format("line %s:%s: %s", nodeLocation.getLineNumber(), nodeLocation.getColumnNumber(), description);
        }

        @Override
        protected Scope visitUpdate(Update update, Optional<Scope> scope)
        {
            Table table = update.getTable();
            QualifiedObjectName tableName = createQualifiedObjectName(session, table, table.getName(), metadata);
            MetadataHandle metadataHandle = analysis.getMetadataHandle();

            if (getViewDefinition(session, metadataResolver, metadataHandle, tableName).isPresent()) {
                throw new SemanticException(NOT_SUPPORTED, update, "Updating into views is not supported");
            }

            if (getMaterializedViewDefinition(session, metadataResolver, metadataHandle, tableName).isPresent()) {
                throw new SemanticException(NOT_SUPPORTED, update, "Updating into materialized views is not supported");
            }

            TableColumnMetadata tableMetadata = getTableColumnsMetadata(session, metadataResolver, metadataHandle, tableName);

            List<ColumnMetadata> allColumns = tableMetadata.getColumnsMetadata();

            Map<String, ColumnMetadata> columns = allColumns.stream()
                    .collect(toImmutableMap(ColumnMetadata::getName, Function.identity()));

            for (UpdateAssignment assignment : update.getAssignments()) {
                String columnName = metadata.normalizeIdentifier(session, tableName.getCatalogName(), assignment.getName().getValue());
                if (!columns.containsKey(columnName)) {
                    throw new SemanticException(MISSING_COLUMN, assignment.getName(), "The UPDATE SET target column %s doesn't exist", columnName);
                }
            }

            Set<String> assignmentTargets = update.getAssignments().stream()
                    .map(assignment -> metadata.normalizeIdentifier(session, tableName.getCatalogName(), assignment.getName().getValue()))
                    .collect(toImmutableSet());
            accessControl.checkCanUpdateTableColumns(session.getRequiredTransactionId(), session.getIdentity(), session.getAccessControlContext(), tableName, assignmentTargets);

            List<ColumnMetadata> updatedColumns = allColumns.stream()
                    .filter(column -> assignmentTargets.contains(column.getName()))
                    .collect(toImmutableList());
            analysis.setUpdateInfo(update.getUpdateInfo());
            analysis.setUpdatedColumns(updatedColumns);

            // Analyzer checks for select permissions but UPDATE has a separate permission, so disable access checks
            StatementAnalyzer analyzer = new StatementAnalyzer(
                    analysis,
                    metadata,
                    sqlParser,
                    new AllowAllAccessControl(),
                    session,
                    warningCollector);

            Scope tableScope = analyzer.analyze(table, scope);
            update.getWhere().ifPresent(where -> analyzeWhere(update, tableScope, where));

            ImmutableList.Builder<ExpressionAnalysis> analysesBuilder = ImmutableList.builder();
            ImmutableList.Builder<Type> expressionTypesBuilder = ImmutableList.builder();
            for (UpdateAssignment assignment : update.getAssignments()) {
                Expression expression = assignment.getValue();
                ExpressionAnalysis analysis = analyzeExpression(expression, tableScope);
                analysesBuilder.add(analysis);
                expressionTypesBuilder.add(analysis.getType(expression));
            }
            List<ExpressionAnalysis> analyses = analysesBuilder.build();
            List<Type> expressionTypes = expressionTypesBuilder.build();

            List<Type> tableTypes = update.getAssignments().stream()
                    .map(assignment -> requireNonNull(columns.get(metadata.normalizeIdentifier(session, tableName.getCatalogName(), assignment.getName().getValue()))))
                    .map(ColumnMetadata::getType)
                    .collect(toImmutableList());

            for (int index = 0; index < expressionTypes.size(); index++) {
                Expression expression = update.getAssignments().get(index).getValue();
                Type expressionType = expressionTypes.get(index);
                Type targetType = tableTypes.get(index);
                if (!targetType.equals(expressionType)) {
                    analysis.addCoercion(expression, targetType, functionAndTypeResolver.isTypeOnlyCoercion(expressionType, targetType));
                }
                analysis.recordSubqueries(update, analyses.get(index));
            }

            return createAndAssignScope(update, scope, Field.newUnqualified(update.getLocation(), "rows", BIGINT));
        }

        @Override
        protected Scope visitMerge(Merge merge, Optional<Scope> scope)
        {
            Relation targetRelation = merge.getTarget();
            Table targetTable = getMergeTargetTable(targetRelation);
            QualifiedObjectName targetTableQualifiedName = createQualifiedObjectName(session, targetTable, targetTable.getName(), metadata);
            MetadataHandle metadataHandle = analysis.getMetadataHandle();

            if (getViewDefinition(session, metadataResolver, metadataHandle, targetTableQualifiedName).isPresent()) {
                throw new SemanticException(NOT_SUPPORTED, merge, "Merging into views is not supported");
            }

            if (getMaterializedViewDefinition(session, metadataResolver, metadataHandle, targetTableQualifiedName).isPresent()) {
                throw new SemanticException(NOT_SUPPORTED, merge, "Merging into materialized views is not supported");
            }

            TableColumnMetadata targetTableColumnsMetadata = getTableColumnsMetadata(session, metadataResolver, metadataHandle, targetTableQualifiedName);

            TableHandle targetTableHandle = targetTableColumnsMetadata.getTableHandle()
                    .orElseThrow(() -> new SemanticException(MISSING_TABLE, targetTable, "Table '%s' does not exist", targetTableQualifiedName));

            // The analyzer checks for select permissions, but the MERGE INTO statement has different permissions, so disable access checks.
            StatementAnalyzer statementAnalyzer = new StatementAnalyzer(analysis, metadata, sqlParser,
                    new AllowAllAccessControl(), session, warningCollector);

            Scope targetTableScope = statementAnalyzer.analyze(targetRelation, scope);
            Scope sourceTableScope = process(merge.getSource(), scope);
            Scope joinScope = createAndAssignScope(merge, scope, targetTableScope.getRelationType().joinWith(sourceTableScope.getRelationType()));

            List<ColumnMetadata> targetColumnsMetadata = targetTableColumnsMetadata.getColumnsMetadata().stream()
                    .filter(column -> !column.isHidden())
                    .collect(toImmutableList());

            Map<String, ColumnHandle> targetAllColumnHandles = metadata.getColumnHandles(session, targetTableHandle);
            ImmutableList.Builder<ColumnHandle> targetColumnHandlesBuilder = ImmutableList.builder();
            ImmutableSet.Builder<String> targetColumnNamesBuilder = ImmutableSet.builder();
            for (ColumnMetadata columnMetadata : targetColumnsMetadata) {
                String targetColumnName = columnMetadata.getName();
                ColumnHandle targetColumnHandle = targetAllColumnHandles.get(targetColumnName);
                targetColumnHandlesBuilder.add(targetColumnHandle);
                targetColumnNamesBuilder.add(targetColumnName);
            }
            List<ColumnHandle> targetColumnHandles = targetColumnHandlesBuilder.build();
            Set<String> targetColumnNames = targetColumnNamesBuilder.build();

            Map<String, Type> targetColumnTypes = targetColumnsMetadata.stream().collect(toImmutableMap(ColumnMetadata::getName, ColumnMetadata::getType));

            // Analyze all expressions in the Merge node

            Expression mergePredicate = merge.getPredicate();
            ExpressionAnalysis mergePredicateAnalysis = analyzeExpression(mergePredicate, joinScope);
            Type mergePredicateType = mergePredicateAnalysis.getType(mergePredicate);
            if (!mergePredicateType.equals(BOOLEAN)) {
                if (!mergePredicateType.equals(UNKNOWN)) {
                    throw new SemanticException(TYPE_MISMATCH, mergePredicate, "The MERGE predicate must evaluate to a boolean: actual type %s", mergePredicateType);
                }
                // coerce null to boolean
                analysis.addCoercion(mergePredicate, BOOLEAN, false);
            }
            analysis.recordSubqueries(merge, mergePredicateAnalysis);

            Set<String> allUpdateColumnNames = new HashSet<>();

            for (int caseCounter = 0; caseCounter < merge.getMergeCases().size(); caseCounter++) {
                MergeCase mergeCase = merge.getMergeCases().get(caseCounter);
                List<String> setColumnNames = lowercaseIdentifierList(mergeCase.getSetColumns());
                if (mergeCase instanceof MergeUpdate) {
                    allUpdateColumnNames.addAll(setColumnNames);
                }
                else if (mergeCase instanceof MergeInsert && setColumnNames.isEmpty()) {
                    setColumnNames = targetColumnsMetadata.stream().map(ColumnMetadata::getName).collect(toImmutableList());
                }
                int mergeCaseSetColumnCount = setColumnNames.size();
                List<Expression> mergeCaseSetExpressions = mergeCase.getSetExpressions();
                checkArgument(
                        mergeCaseSetColumnCount == mergeCaseSetExpressions.size(),
                        "Number of merge columns (%s) isn't equal to number of expressions (%s)",
                        mergeCaseSetColumnCount, mergeCaseSetExpressions.size());
                Set<String> mergeCaseColumnNameSet = new HashSet<>(mergeCaseSetColumnCount);
                // Look for missing or duplicate column names.
                setColumnNames.forEach(mergeCaseColumnName -> {
                    if (!targetColumnNames.contains(mergeCaseColumnName)) {
                        throw new SemanticException(MISSING_COLUMN, merge, "Merge column name does not exist in target table: %s", mergeCaseColumnName);
                    }
                    if (!mergeCaseColumnNameSet.add(mergeCaseColumnName)) {
                        throw new SemanticException(DUPLICATE_COLUMN_NAME, merge, "Merge column name is specified more than once: %s", mergeCaseColumnName);
                    }
                });

                // Collects types for columns and expressions in this MergeCase.
                ImmutableList.Builder<Type> setColumnTypesBuilder = ImmutableList.builder();
                ImmutableList.Builder<Type> setExpressionTypesBuilder = ImmutableList.builder();
                for (int index = 0; index < setColumnNames.size(); index++) {
                    String columnName = setColumnNames.get(index);
                    Expression setExpression = mergeCaseSetExpressions.get(index);
                    ExpressionAnalysis setExpressionAnalysis = analyzeExpression(setExpression, joinScope);
                    analysis.recordSubqueries(merge, setExpressionAnalysis);
                    Type setColumnType = requireNonNull(targetColumnTypes.get(columnName));
                    setColumnTypesBuilder.add(setColumnType);
                    setExpressionTypesBuilder.add(setExpressionAnalysis.getType(setExpression));
                }
                List<Type> setColumnTypes = setColumnTypesBuilder.build();
                List<Type> setExpressionTypes = setExpressionTypesBuilder.build();

                // Check if the types of the columns and expressions match for the MERGE SET clause.
                if (!checkTypesMatchForMergeSet(setColumnTypes, setExpressionTypes)) {
                    throw new SemanticException(TYPE_MISMATCH,
                            mergeCase,
                            "MERGE table column types don't match for MERGE case %s, SET expressions: Table: [%s], Expressions: [%s]",
                            caseCounter,
                            Joiner.on(", ").join(setColumnTypes),
                            Joiner.on(", ").join(setExpressionTypes));
                }

                // Add coercion if the target column type and set expression type do not match.
                for (int index = 0; index < setColumnNames.size(); index++) {
                    Expression setExpression = mergeCase.getSetExpressions().get(index);
                    Type targetColumnType = targetColumnTypes.get(setColumnNames.get(index));
                    Type setExpressionType = setExpressionTypes.get(index);
                    if (!targetColumnType.equals(setExpressionType)) {
                        FunctionAndTypeManager functionAndTypeManager = metadata.getFunctionAndTypeManager();
                        analysis.addCoercion(setExpression, targetColumnType, functionAndTypeManager.isTypeOnlyCoercion(setExpressionType, targetColumnType));
                    }
                }
            }

            // Check if the user has permission to insert into the target table
            merge.getMergeCases().stream()
                    .filter(mergeCase -> mergeCase instanceof MergeInsert)
                    .findFirst()
                    .ifPresent(mergeCase -> accessControl.checkCanInsertIntoTable(session.getRequiredTransactionId(),
                            session.getIdentity(), session.getAccessControlContext(), targetTableQualifiedName));

            // If there are any columns to update then verify the user has permission to update these columns.
            if (!allUpdateColumnNames.isEmpty()) {
                accessControl.checkCanUpdateTableColumns(session.getRequiredTransactionId(), session.getIdentity(),
                        session.getAccessControlContext(), targetTableQualifiedName, allUpdateColumnNames);
            }

            analysis.setUpdateInfo(merge.getUpdateInfo());

            List<List<ColumnHandle>> mergeCaseColumnHandles = buildMergeCaseColumnLists(merge, targetColumnsMetadata, targetAllColumnHandles);

            ImmutableMap.Builder<ColumnHandle, Integer> columnHandleFieldNumbersBuilder = ImmutableMap.builder();
            Map<String, Integer> fieldIndexes = new HashMap<>();
            RelationType targetRelationType = targetTableScope.getRelationType();
            for (Field targetField : targetRelationType.getAllFields()) {
                targetField.getName()
                        .filter(targetFieldName -> !"$target_table_row_id".equals(targetFieldName)) // Skip "$target_table_row_id" column.
                        .ifPresent(targetFieldName -> {
                            int targetFieldIndex = targetRelationType.indexOf(targetField);
                            ColumnHandle targetColumnHandle = targetAllColumnHandles.get(targetFieldName);
                            verify(targetColumnHandle != null, "targetAllColumnHandles does not contain the named handle: %s", targetFieldName);
                            columnHandleFieldNumbersBuilder.put(targetColumnHandle, targetFieldIndex);
                            fieldIndexes.put(targetFieldName, targetFieldIndex);
                        });
            }
            Map<ColumnHandle, Integer> columnHandleFieldNumbers = columnHandleFieldNumbersBuilder.buildOrThrow();

            Set<ColumnHandle> nonNullableColumnHandles = metadata.getTableMetadata(session, targetTableHandle).getColumns().stream()
                    .filter(column -> !column.isNullable())
                    .map(ColumnMetadata::getName)
                    .map(targetAllColumnHandles::get)
                    .collect(toImmutableSet());

            analysis.setMergeAnalysis(new MergeAnalysis(
                    targetTable,
                    targetColumnsMetadata,
                    targetColumnHandles,
                    mergeCaseColumnHandles,
                    nonNullableColumnHandles,
                    columnHandleFieldNumbers,
                    targetTableScope,
                    joinScope));

            return createAndAssignScope(merge, Optional.empty(), Field.newUnqualified(merge.getLocation(), "rows", BIGINT));
        }

        private boolean checkTypesMatchForMergeSet(Iterable<Type> tableTypes, Iterable<Type> queryTypes)
        {
            if (Iterables.size(tableTypes) != Iterables.size(queryTypes)) {
                return false;
            }

            Iterator<Type> tableTypesIterator = tableTypes.iterator();
            Iterator<Type> queryTypesIterator = queryTypes.iterator();
            while (tableTypesIterator.hasNext()) {
                Type tableType = tableTypesIterator.next();
                Type queryType = queryTypesIterator.next();

                if (!metadata.getFunctionAndTypeManager().canCoerce(queryType, tableType)) {
                    return false;
                }
            }

            return true;
        }

        private Table getMergeTargetTable(Relation relation)
        {
            if (relation instanceof Table) {
                return (Table) relation;
            }
            checkArgument(relation instanceof AliasedRelation, "relation is neither a Table nor an AliasedRelation");
            return (Table) ((AliasedRelation) relation).getRelation();
        }

        /**
         * Builds a list of column handles for each merge case in the given merge statement.
         *
         * @param merge the merge statement
         * @param columnSchemas the list of column metadata for the target table.
         * @param allColumnHandles a map of column names to column handles for the target table.
         * @return a list of lists of column handles, where each inner list corresponds to a merge case.
         */
        private List<List<ColumnHandle>> buildMergeCaseColumnLists(Merge merge, List<ColumnMetadata> columnSchemas, Map<String, ColumnHandle> allColumnHandles)
        {
            ImmutableList.Builder<List<ColumnHandle>> mergeCaseColumnsListsBuilder = ImmutableList.builder();
            for (int caseCounter = 0; caseCounter < merge.getMergeCases().size(); caseCounter++) {
                MergeCase mergeCase = merge.getMergeCases().get(caseCounter);
                List<String> mergeColumnNames;
                if (mergeCase instanceof MergeInsert && mergeCase.getSetColumns().isEmpty()) {
                    mergeColumnNames = columnSchemas.stream().map(ColumnMetadata::getName).collect(toImmutableList());
                }
                else {
                    mergeColumnNames = lowercaseIdentifierList(mergeCase.getSetColumns());
                }
                mergeCaseColumnsListsBuilder.add(
                        mergeColumnNames.stream()
                                .map(name -> requireNonNull(allColumnHandles.get(name), "No column found for name"))
                                .collect(toImmutableList()));
            }
            return mergeCaseColumnsListsBuilder.build();
        }

        private List<String> lowercaseIdentifierList(Collection<Identifier> identifiers)
        {
            return identifiers.stream()
                    .map(identifier -> identifier.getValue().toLowerCase(ENGLISH))
                    .collect(toImmutableList());
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
                    functionAndTypeResolver.resolveOperator(OperatorType.EQUAL, fromTypes(
                            leftField.get().getType(), rightField.get().getType()));
                }
                catch (OperatorNotFoundException e) {
                    throw new SemanticException(TYPE_MISMATCH, column, "%s", e.getMessage());
                }

                Optional<Type> type = functionAndTypeResolver.getCommonSuperType(leftField.get().getType(), rightField.get().getType());
                analysis.addTypes(ImmutableMap.of(NodeRef.of(column), type.get()));

                joinFields.add(Field.newUnqualified(column.getLocation(), column.getValue(), type.get()));

                leftJoinFields.add(leftField.get().getRelationFieldIndex());
                rightJoinFields.add(rightField.get().getRelationFieldIndex());

                analysis.addColumnReference(NodeRef.of(column), FieldId.from(leftField.get()));
                analysis.addColumnReference(NodeRef.of(column), FieldId.from(rightField.get()));
                if (leftField.get().getField().getOriginTable().isPresent() && leftField.get().getField().getOriginColumnName().isPresent()) {
                    Multimap<QualifiedObjectName, Subfield> tableColumnMap = ImmutableMultimap.of(leftField.get().getField().getOriginTable().get(), new Subfield(leftField.get().getField().getOriginColumnName().get(), ImmutableList.of()));
                    analysis.addTableColumnAndSubfieldReferences(accessControl, session.getIdentity(), session.getTransactionId(), session.getAccessControlContext(), tableColumnMap, tableColumnMap);
                }
                if (rightField.get().getField().getOriginTable().isPresent() && rightField.get().getField().getOriginColumnName().isPresent()) {
                    Multimap<QualifiedObjectName, Subfield> tableColumnMap = ImmutableMultimap.of(rightField.get().getField().getOriginTable().get(), new Subfield(rightField.get().getField().getOriginColumnName().get(), ImmutableList.of()));
                    analysis.addTableColumnAndSubfieldReferences(accessControl, session.getIdentity(), session.getTransactionId(), session.getAccessControlContext(), tableColumnMap, tableColumnMap);
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

                    Optional<Type> commonSuperType = functionAndTypeResolver.getCommonSuperType(fieldType, superType);
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
                            analysis.addCoercion(item, expectedType, functionAndTypeResolver.isTypeOnlyCoercion(actualType, expectedType));
                        }
                    }
                }
                else {
                    Type actualType = analysis.getType(row);
                    Type expectedType = fieldTypes.get(0);
                    if (!actualType.equals(expectedType)) {
                        analysis.addCoercion(row, expectedType, functionAndTypeResolver.isTypeOnlyCoercion(actualType, expectedType));
                    }
                }
            }

            List<Field> fields = fieldTypes.stream()
                    .map(valueType -> Field.newUnqualified(node.getLocation(), Optional.empty(), valueType))
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
                new WindowFunctionValidator(functionAndTypeResolver).process(expression, analysis);
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
                if (window.getOrderBy().filter(orderBy -> orderBy.getSortItems().stream().anyMatch(item -> item.getSortKey() instanceof Literal)).isPresent()) {
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

                FunctionKind kind = functionAndTypeResolver.getFunctionMetadata(analysis.getFunctionHandle(windowFunction)).getFunctionKind();
                if (kind != AGGREGATE && kind != WINDOW) {
                    throw new SemanticException(MUST_BE_WINDOW_FUNCTION, node, "Not a window function: %s", windowFunction.getName());
                }
            }

            return windowFunctions;
        }

        private void analyzeWindowFrame(WindowFrame frame)
        {
            FrameBound.Type startType = frame.getStart().getType();
            FrameBound.Type endType = frame.getEnd().orElseGet(() -> new FrameBound(CURRENT_ROW)).getType();

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

                List<String> builtInFunctionNames = functionAndTypeResolver.listBuiltInFunctions().stream()
                        .map(SqlFunction::getSignature)
                        .map(Signature::getName)
                        .map(QualifiedObjectName::getObjectName)
                        .collect(toImmutableList());
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
                    return getOnlyElement(expressions);
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
                                verifyNoAggregateWindowOrGroupingFunctions(analysis.getFunctionHandles(), functionAndTypeResolver, column, "GROUP BY clause");
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
                        Field newField = Field.newUnqualified(node.getSelect().getLocation(), field.getName(), field.getType(), field.getOriginTable(), field.getOriginColumnName(), false);
                        analysis.addSourceColumns(newField, analysis.getSourceColumns(field));
                        outputFields.add(newField);
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
                        name = getQualifiedName((DereferenceExpression) expression);
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
                            field = Optional.of(name.getOriginalSuffix());
                        }
                    }
                    Field newField = Field.newUnqualified(expression.getLocation(), field.map(Identifier::getValue), analysis.getType(expression), originTable, originColumn, column.getAlias().isPresent());
                    if (originTable.isPresent()) {
                        analysis.addSourceColumns(newField, ImmutableSet.of(
                                new SourceColumn(originTable.get(), originColumn.orElseThrow(
                                        () -> new NoSuchElementException("originColumn not found")))));
                    }
                    else {
                        analysis.addSourceColumns(newField, analysis.getExpressionSourceColumns(expression));
                    }
                    outputFields.add(newField);
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
                                .orElse(Field.newUnqualified(expression.getLocation(), Optional.empty(), analysis.getType(expression)));
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
                        FieldReference expression = new FieldReference(field.getNodeLocation(), fieldIndex);
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

            verifyNoAggregateWindowOrGroupingFunctions(analysis.getFunctionHandles(), functionAndTypeResolver, predicate, "WHERE clause");

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
            List<FunctionCall> aggregates = extractAggregateFunctions(analysis.getFunctionHandles(), Iterables.concat(outputExpressions, orderByExpressions), functionAndTypeResolver);
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
                    verifySourceAggregations(distinctGroupingColumns, sourceScope, expression, functionAndTypeResolver, analysis, warningCollector, session);
                }

                for (Expression expression : orderByExpressions) {
                    verifyOrderByAggregations(distinctGroupingColumns, sourceScope, orderByScope.get(), expression, functionAndTypeResolver, analysis, warningCollector, session);
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
                    // definer mode
                    identity = new Identity(owner.get(), Optional.empty(), session.getIdentity().getExtraCredentials());
                    viewAccessControl = new ViewAccessControl(accessControl);
                }
                else {
                    identity = session.getIdentity();
                    viewAccessControl = accessControl;
                }

                Session viewSession = createViewSession(catalog, schema, identity);

                StatementAnalyzer analyzer = new StatementAnalyzer(analysis, metadata, sqlParser, viewAccessControl, viewSession, warningCollector);
                Scope queryScope = analyzer.analyze(query, Scope.create());
                return queryScope.getRelationType().withAlias(name.getObjectName(), null);
            }
            catch (RuntimeException e) {
                throwIfInstanceOf(e, PrestoException.class);
                throw new SemanticException(VIEW_ANALYSIS_ERROR, e, node.getLocation(), "Failed analyzing stored view '%s': %s", name, e.getMessage());
            }
        }

        private Session createViewSession(Optional<String> catalog, Optional<String> schema, Identity identity)
        {
            Session.SessionBuilder viewSessionBuilder = Session.builder(metadata.getSessionPropertyManager())
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
                    .setStartTime(session.getStartTime());
            session.getConnectorProperties().forEach((connectorId, properties) -> properties.forEach((k, v) -> viewSessionBuilder.setConnectionProperty(connectorId, k, v)));
            return viewSessionBuilder.build();
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
                        !functionAndTypeResolver.canCoerce(field.getType(), column.getType())) {
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

        private void analyzeFiltersAndMasks(Table table, QualifiedObjectName name, Scope accessControlScope, Collection<Field> fields)
        {
            List<ColumnMetadata> columnsMetadata = new ArrayList<>();
            for (Field field : fields) {
                if (field.getName().isPresent()) {
                    columnsMetadata.add(ColumnMetadata.builder().setName(field.getName().get()).setType(field.getType()).build());
                }
            }
            analyzeFiltersAndMasks(table, name, accessControlScope, columnsMetadata);
        }

        private void analyzeFiltersAndMasks(Table table, QualifiedObjectName name, Scope accessControlScope, List<ColumnMetadata> columnsMetadata)
        {
            Map<ColumnMetadata, ViewExpression> masks = accessControl.getColumnMasks(session.getRequiredTransactionId(), session.getIdentity(), session.getAccessControlContext(), name, columnsMetadata);

            for (Map.Entry<ColumnMetadata, ViewExpression> maskEntry : masks.entrySet()) {
                analyzeColumnMask(session.getIdentity().getUser(), table, name, maskEntry.getKey(), accessControlScope, maskEntry.getValue());
            }

            accessControl.getRowFilters(session.getRequiredTransactionId(), session.getIdentity(), session.getAccessControlContext(), name)
                    .forEach(filter -> analyzeRowFilter(session.getIdentity().getUser(), table, name, accessControlScope, filter));
        }

        private void analyzeRowFilter(String currentIdentity, Table table, QualifiedObjectName name, Scope scope, ViewExpression filter)
        {
            if (analysis.hasRowFilter(name, currentIdentity)) {
                throw new PrestoException(INVALID_ROW_FILTER, format("Row filter for '%s' is recursive", name), null);
            }

            Expression expression;
            try {
                expression = sqlParser.createExpression(filter.getExpression(), createParsingOptions(session));
            }
            catch (ParsingException e) {
                throw new PrestoException(INVALID_ROW_FILTER, format("Invalid row filter for '%s': %s", name, e.getErrorMessage()), e);
            }

            analysis.registerTableForRowFiltering(name, currentIdentity);
            ExpressionAnalysis expressionAnalysis;
            try {
                expressionAnalysis = ExpressionAnalyzer.analyzeExpression(
                        createViewSession(filter.getCatalog(), filter.getSchema(), new Identity(filter.getIdentity(), Optional.empty())), // TODO: path should be included in row filter
                        metadata,
                        accessControl,
                        sqlParser,
                        scope,
                        analysis,
                        expression,
                        warningCollector);
            }
            catch (PrestoException e) {
                throw new PrestoException(e::getErrorCode, format("Invalid row filter for '%s: %s'", name, e.getMessage()), e);
            }
            finally {
                analysis.unregisterTableForRowFiltering(name, currentIdentity);
            }

            verifyNoAggregateWindowOrGroupingFunctions(analysis.getFunctionHandles(), functionAndTypeResolver, expression, format("Row filter for '%s'", name));

            analysis.recordSubqueries(expression, expressionAnalysis);

            Type actualType = expressionAnalysis.getType(expression);
            if (!actualType.equals(BOOLEAN)) {
                if (!metadata.getFunctionAndTypeManager().canCoerce(actualType, BOOLEAN)) {
                    throw new PrestoException(DATATYPE_MISMATCH, format("Expected row filter for '%s' to be of type BOOLEAN, but was %s", name, actualType), null);
                }

                analysis.addCoercion(expression, BOOLEAN, false);
            }

            analysis.addRowFilter(table, expression);
        }

        private void analyzeColumnMask(String currentIdentity, Table table, QualifiedObjectName tableName, ColumnMetadata columnMetadata, Scope scope, ViewExpression mask)
        {
            String column = columnMetadata.getName();
            if (analysis.hasColumnMask(tableName, column, currentIdentity)) {
                throw new PrestoException(INVALID_COLUMN_MASK, format("Column mask for '%s.%s' is recursive", tableName, column), null);
            }

            Expression expression;
            try {
                expression = sqlParser.createExpression(mask.getExpression(), createParsingOptions(session));
            }
            catch (ParsingException e) {
                throw new PrestoException(INVALID_COLUMN_MASK, format("Invalid column mask for '%s.%s': %s", tableName, column, e.getErrorMessage()), e);
            }

            ExpressionAnalysis expressionAnalysis;
            analysis.registerTableForColumnMasking(tableName, column, currentIdentity);
            try {
                expressionAnalysis = ExpressionAnalyzer.analyzeExpression(
                        createViewSession(mask.getCatalog(), mask.getSchema(), new Identity(mask.getIdentity(), Optional.empty())), // TODO: path should be included in row filter
                        metadata,
                        accessControl,
                        sqlParser,
                        scope,
                        analysis,
                        expression,
                        warningCollector);
            }
            catch (PrestoException e) {
                throw new PrestoException(e::getErrorCode, format("Invalid column mask for '%s.%s: %s'", tableName, column, e.getMessage()), e);
            }
            finally {
                analysis.unregisterTableForColumnMasking(tableName, column, currentIdentity);
            }

            verifyNoAggregateWindowOrGroupingFunctions(analysis.getFunctionHandles(), functionAndTypeResolver, expression, format("Column mask for '%s.%s'", table.getName(), column));

            analysis.recordSubqueries(expression, expressionAnalysis);

            Type expectedType = columnMetadata.getType();
            Type actualType = expressionAnalysis.getType(expression);
            if (!actualType.equals(expectedType)) {
                if (!metadata.getFunctionAndTypeManager().canCoerce(actualType, columnMetadata.getType())) {
                    throw new PrestoException(DATATYPE_MISMATCH, format("Expected column mask for '%s.%s' to be of type %s, but was %s", tableName, column, columnMetadata.getType(), actualType), null);
                }

                // TODO: this should be "coercion.isTypeOnlyCoercion(actualType, expectedType)", but type-only coercions are broken
                // due to the line "changeType(value, returnType)" in SqlToRowExpressionTranslator.visitCast. If there's an expression
                // like CAST(CAST(x AS VARCHAR(1)) AS VARCHAR(2)), it determines that the outer cast is type-only and converts the expression
                // to CAST(x AS VARCHAR(2)) by changing the type of the inner cast.
                analysis.addCoercion(expression, expectedType, false);
            }

            analysis.addColumnMask(table, column, expression);
        }

        private List<Expression> descriptorToFields(Scope scope)
        {
            ImmutableList.Builder<Expression> builder = ImmutableList.builder();
            for (int fieldIndex = 0; fieldIndex < scope.getRelationType().getAllFieldCount(); fieldIndex++) {
                FieldReference expression = new FieldReference(scope.getRelationType().getFieldByIndex(fieldIndex).getNodeLocation(), fieldIndex);
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

                String name = withQuery.getName().getValueLowerCase();
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

        private void analyzeOffset(Offset node)
        {
            long rowCount;
            try {
                rowCount = Long.parseLong(node.getRowCount());
            }
            catch (NumberFormatException e) {
                throw new SemanticException(INVALID_OFFSET_ROW_COUNT, node, "Invalid OFFSET row count: %s", node.getRowCount());
            }
            if (rowCount < 0) {
                throw new SemanticException(INVALID_OFFSET_ROW_COUNT, node, "OFFSET row count must be greater or equal to 0 (actual value: %s)", rowCount);
            }
            analysis.setOffset(node, rowCount);
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

                    expression = new FieldReference(expression.getLocation(), toIntExact(ordinal - 1));
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

    private static final class ArgumentAnalysis
    {
        private final Argument argument;
        private final Optional<TableArgumentAnalysis> tableArgumentAnalysis;

        public ArgumentAnalysis(Argument argument, Optional<TableArgumentAnalysis> tableArgumentAnalysis)
        {
            this.argument = requireNonNull(argument, "argument is null");
            this.tableArgumentAnalysis = requireNonNull(tableArgumentAnalysis, "tableArgumentAnalysis is null");
        }

        public Argument getArgument()
        {
            return argument;
        }

        public Optional<TableArgumentAnalysis> getTableArgumentAnalysis()
        {
            return tableArgumentAnalysis;
        }
    }

    private static final class ArgumentsAnalysis
    {
        private final Map<String, Argument> passedArguments;
        private final List<TableArgumentAnalysis> tableArgumentAnalyses;

        public ArgumentsAnalysis(Map<String, Argument> passedArguments, List<TableArgumentAnalysis> tableArgumentAnalyses)
        {
            this.passedArguments = ImmutableMap.copyOf(requireNonNull(passedArguments, "passedArguments is null"));
            this.tableArgumentAnalyses = ImmutableList.copyOf(requireNonNull(tableArgumentAnalyses, "tableArgumentAnalyses is null"));
        }

        public Map<String, Argument> getPassedArguments()
        {
            return passedArguments;
        }

        public List<TableArgumentAnalysis> getTableArgumentAnalyses()
        {
            return tableArgumentAnalyses;
        }
    }
}
