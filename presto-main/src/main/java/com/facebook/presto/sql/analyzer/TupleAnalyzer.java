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
import com.facebook.presto.metadata.FunctionInfo;
import com.facebook.presto.metadata.FunctionRegistry;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.metadata.MetadataUtil;
import com.facebook.presto.metadata.QualifiedTableName;
import com.facebook.presto.metadata.TableHandle;
import com.facebook.presto.metadata.TableMetadata;
import com.facebook.presto.metadata.ViewDefinition;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.type.BigintType;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeSignature;
import com.facebook.presto.sql.ExpressionUtils;
import com.facebook.presto.sql.parser.ParsingException;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.planner.ExpressionInterpreter;
import com.facebook.presto.sql.planner.NoOpSymbolResolver;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.planner.SymbolResolver;
import com.facebook.presto.sql.planner.optimizations.CanonicalizeExpressions;
import com.facebook.presto.sql.tree.AliasedRelation;
import com.facebook.presto.sql.tree.AllColumns;
import com.facebook.presto.sql.tree.ComparisonExpression;
import com.facebook.presto.sql.tree.DefaultExpressionTraversalVisitor;
import com.facebook.presto.sql.tree.DefaultTraversalVisitor;
import com.facebook.presto.sql.tree.Except;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.FrameBound;
import com.facebook.presto.sql.tree.FunctionCall;
import com.facebook.presto.sql.tree.Intersect;
import com.facebook.presto.sql.tree.Join;
import com.facebook.presto.sql.tree.JoinCriteria;
import com.facebook.presto.sql.tree.JoinOn;
import com.facebook.presto.sql.tree.JoinUsing;
import com.facebook.presto.sql.tree.LongLiteral;
import com.facebook.presto.sql.tree.NaturalJoin;
import com.facebook.presto.sql.tree.Node;
import com.facebook.presto.sql.tree.QualifiedName;
import com.facebook.presto.sql.tree.QualifiedNameReference;
import com.facebook.presto.sql.tree.Query;
import com.facebook.presto.sql.tree.QuerySpecification;
import com.facebook.presto.sql.tree.Relation;
import com.facebook.presto.sql.tree.Row;
import com.facebook.presto.sql.tree.SampledRelation;
import com.facebook.presto.sql.tree.SelectItem;
import com.facebook.presto.sql.tree.SingleColumn;
import com.facebook.presto.sql.tree.SortItem;
import com.facebook.presto.sql.tree.Statement;
import com.facebook.presto.sql.tree.Table;
import com.facebook.presto.sql.tree.TableSubquery;
import com.facebook.presto.sql.tree.Union;
import com.facebook.presto.sql.tree.Unnest;
import com.facebook.presto.sql.tree.Values;
import com.facebook.presto.sql.tree.Window;
import com.facebook.presto.sql.tree.WindowFrame;
import com.facebook.presto.type.ArrayType;
import com.facebook.presto.type.MapType;
import com.facebook.presto.type.RowType;
import com.facebook.presto.util.ImmutableCollectors;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;

import java.util.ArrayList;
import java.util.Collection;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.facebook.presto.metadata.FunctionRegistry.getCommonSuperType;
import static com.facebook.presto.metadata.ViewDefinition.ViewColumn;
import static com.facebook.presto.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.sql.analyzer.ExpressionAnalyzer.getExpressionTypes;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.AMBIGUOUS_ATTRIBUTE;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.DUPLICATE_RELATION;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.INVALID_ORDINAL;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.INVALID_WINDOW_FRAME;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.MISMATCHED_COLUMN_ALIASES;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.MISMATCHED_SET_COLUMN_TYPES;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.MISSING_CATALOG;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.MISSING_SCHEMA;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.MISSING_TABLE;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.MUST_BE_AGGREGATE_OR_GROUP_BY;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.MUST_BE_WINDOW_FUNCTION;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.NESTED_WINDOW;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.NON_NUMERIC_SAMPLE_PERCENTAGE;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.NOT_SUPPORTED;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.ORDER_BY_MUST_BE_IN_SELECT;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.TYPE_MISMATCH;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.VIEW_ANALYSIS_ERROR;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.VIEW_IS_STALE;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.VIEW_PARSE_ERROR;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.WILDCARD_WITHOUT_FROM;
import static com.facebook.presto.sql.planner.ExpressionInterpreter.expressionOptimizer;
import static com.facebook.presto.sql.tree.ComparisonExpression.Type.EQUAL;
import static com.facebook.presto.sql.tree.FrameBound.Type.CURRENT_ROW;
import static com.facebook.presto.sql.tree.FrameBound.Type.FOLLOWING;
import static com.facebook.presto.sql.tree.FrameBound.Type.PRECEDING;
import static com.facebook.presto.sql.tree.FrameBound.Type.UNBOUNDED_FOLLOWING;
import static com.facebook.presto.sql.tree.FrameBound.Type.UNBOUNDED_PRECEDING;
import static com.facebook.presto.sql.tree.WindowFrame.Type.RANGE;
import static com.facebook.presto.type.UnknownType.UNKNOWN;
import static com.facebook.presto.util.ImmutableCollectors.toImmutableList;
import static com.facebook.presto.util.Types.checkType;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

public class TupleAnalyzer
        extends DefaultTraversalVisitor<TupleDescriptor, AnalysisContext>
{
    private final Analysis analysis;
    private final Session session;
    private final Metadata metadata;
    private final SqlParser sqlParser;
    private final boolean experimentalSyntaxEnabled;

    public TupleAnalyzer(Analysis analysis, Session session, Metadata metadata, SqlParser sqlParser, boolean experimentalSyntaxEnabled)
    {
        checkNotNull(analysis, "analysis is null");
        checkNotNull(session, "session is null");
        checkNotNull(metadata, "metadata is null");

        this.analysis = analysis;
        this.session = session;
        this.metadata = metadata;
        this.sqlParser = sqlParser;
        this.experimentalSyntaxEnabled = experimentalSyntaxEnabled;
    }

    @Override
    protected TupleDescriptor visitUnnest(Unnest node, AnalysisContext context)
    {
        ImmutableList.Builder<Field> outputFields = ImmutableList.builder();
        for (Expression expression : node.getExpressions()) {
            ExpressionAnalysis expressionAnalysis = analyzeExpression(expression, context.getLateralTupleDescriptor(), context);
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
            outputFields.add(Field.newUnqualified(Optional.empty(), BigintType.BIGINT));
        }
        TupleDescriptor descriptor = new TupleDescriptor(outputFields.build());
        analysis.setOutputDescriptor(node, descriptor);
        return descriptor;
    }

    @Override
    protected TupleDescriptor visitTable(Table table, AnalysisContext context)
    {
        if (!table.getName().getPrefix().isPresent()) {
            // is this a reference to a WITH query?
            String name = table.getName().getSuffix();

            Query query = context.getNamedQuery(name);
            if (query != null) {
                analysis.registerNamedQuery(table, query);

                // re-alias the fields with the name assigned to the query in the WITH declaration
                TupleDescriptor queryDescriptor = analysis.getOutputDescriptor(query);
                ImmutableList.Builder<Field> fields = ImmutableList.builder();
                for (Field field : queryDescriptor.getAllFields()) {
                    fields.add(Field.newQualified(QualifiedName.of(name), field.getName(), field.getType(), false));
                }

                TupleDescriptor descriptor = new TupleDescriptor(fields.build());
                analysis.setOutputDescriptor(table, descriptor);
                return descriptor;
            }
        }

        QualifiedTableName name = MetadataUtil.createQualifiedTableName(session, table.getName());

        Optional<ViewDefinition> optionalView = metadata.getView(session, name);
        if (optionalView.isPresent()) {
            ViewDefinition view = optionalView.get();

            Query query = parseView(view.getOriginalSql(), name, table);

            analysis.registerNamedQuery(table, query);

            TupleDescriptor descriptor = analyzeView(query, name, view.getCatalog(), view.getSchema(), table);

            if (isViewStale(view.getColumns(), descriptor.getVisibleFields())) {
                throw new SemanticException(VIEW_IS_STALE, table, "View '%s' is stale; it must be re-created", name);
            }

            analysis.setOutputDescriptor(table, descriptor);
            return descriptor;
        }

        Optional<TableHandle> tableHandle = metadata.getTableHandle(session, name);
        if (!tableHandle.isPresent()) {
            if (!metadata.getCatalogNames().containsKey(name.getCatalogName())) {
                throw new SemanticException(MISSING_CATALOG, table, "Catalog %s does not exist", name.getCatalogName());
            }
            if (!metadata.listSchemaNames(session, name.getCatalogName()).contains(name.getSchemaName())) {
                throw new SemanticException(MISSING_SCHEMA, table, "Schema %s does not exist", name.getSchemaName());
            }

            if (table.getName().getSuffix().equalsIgnoreCase("DUAL")) {
                // TODO: remove this in a few releases
                throw new SemanticException(MISSING_TABLE, table, "DUAL table is no longer supported. Please use VALUES or FROM-less queries instead");
            }

            throw new SemanticException(MISSING_TABLE, table, "Table %s does not exist", name);
        }
        TableMetadata tableMetadata = metadata.getTableMetadata(session, tableHandle.get());
        Map<String, ColumnHandle> columnHandles = metadata.getColumnHandles(session, tableHandle.get());

        // TODO: discover columns lazily based on where they are needed (to support datasources that can't enumerate all tables)
        ImmutableList.Builder<Field> fields = ImmutableList.builder();
        for (ColumnMetadata column : tableMetadata.getColumns()) {
            Field field = Field.newQualified(table.getName(), Optional.of(column.getName()), column.getType(), column.isHidden());
            fields.add(field);
            ColumnHandle columnHandle = columnHandles.get(column.getName());
            checkArgument(columnHandle != null, "Unknown field %s", field);
            analysis.setColumn(field, columnHandle);
        }

        analysis.registerTable(table, tableHandle.get());

        TupleDescriptor descriptor = new TupleDescriptor(fields.build());
        analysis.setOutputDescriptor(table, descriptor);
        return descriptor;
    }

    @Override
    protected TupleDescriptor visitAliasedRelation(AliasedRelation relation, AnalysisContext context)
    {
        TupleDescriptor child = process(relation.getRelation(), context);

        // todo this check should be inside of TupleDescriptor.withAlias, but the exception needs the node object
        if (relation.getColumnNames() != null) {
            int totalColumns = child.getVisibleFieldCount();
            if (totalColumns != relation.getColumnNames().size()) {
                throw new SemanticException(MISMATCHED_COLUMN_ALIASES, relation, "Column alias list has %s entries but '%s' has %s columns available", relation.getColumnNames().size(), relation.getAlias(), totalColumns);
            }
        }

        TupleDescriptor descriptor = child.withAlias(relation.getAlias(), relation.getColumnNames());

        analysis.setOutputDescriptor(relation, descriptor);
        return descriptor;
    }

    @Override
    protected TupleDescriptor visitSampledRelation(final SampledRelation relation, AnalysisContext context)
    {
        if (relation.getColumnsToStratifyOn().isPresent()) {
            throw new SemanticException(NOT_SUPPORTED, relation, "STRATIFY ON is not yet implemented");
        }

        if (!DependencyExtractor.extract(relation.getSamplePercentage()).isEmpty()) {
            throw new SemanticException(NON_NUMERIC_SAMPLE_PERCENTAGE, relation.getSamplePercentage(), "Sample percentage cannot contain column references");
        }

        IdentityHashMap<Expression, Type> expressionTypes = getExpressionTypes(session, metadata, sqlParser, ImmutableMap.<Symbol, Type>of(), relation.getSamplePercentage());
        ExpressionInterpreter samplePercentageEval = expressionOptimizer(relation.getSamplePercentage(), metadata, session, expressionTypes);

        Object samplePercentageObject = samplePercentageEval.optimize(new SymbolResolver()
        {
            @Override
            public Object getValue(Symbol symbol)
            {
                throw new SemanticException(NON_NUMERIC_SAMPLE_PERCENTAGE, relation.getSamplePercentage(), "Sample percentage cannot contain column references");
            }
        });

        if (!(samplePercentageObject instanceof Number)) {
            throw new SemanticException(NON_NUMERIC_SAMPLE_PERCENTAGE, relation.getSamplePercentage(), "Sample percentage should evaluate to a numeric expression");
        }

        double samplePercentageValue = ((Number) samplePercentageObject).doubleValue();

        if (samplePercentageValue < 0.0) {
            throw new SemanticException(SemanticErrorCode.SAMPLE_PERCENTAGE_OUT_OF_RANGE, relation.getSamplePercentage(), "Sample percentage must be greater than or equal to 0");
        }
        else if ((samplePercentageValue > 100.0) && (relation.getType() != SampledRelation.Type.POISSONIZED || relation.isRescaled())) {
            throw new SemanticException(SemanticErrorCode.SAMPLE_PERCENTAGE_OUT_OF_RANGE, relation.getSamplePercentage(), "Sample percentage must be less than or equal to 100");
        }

        if (relation.isRescaled() && !experimentalSyntaxEnabled) {
            throw new SemanticException(NOT_SUPPORTED, relation, "Rescaling is not enabled");
        }

        TupleDescriptor descriptor = process(relation.getRelation(), context);

        analysis.setOutputDescriptor(relation, descriptor);
        analysis.setSampleRatio(relation, samplePercentageValue / 100);

        return descriptor;
    }

    @Override
    protected TupleDescriptor visitTableSubquery(TableSubquery node, AnalysisContext context)
    {
        StatementAnalyzer analyzer = new StatementAnalyzer(analysis, metadata, sqlParser, session, experimentalSyntaxEnabled, Optional.empty());
        TupleDescriptor descriptor = analyzer.process(node.getQuery(), context);

        analysis.setOutputDescriptor(node, descriptor);

        return descriptor;
    }

    @Override
    protected TupleDescriptor visitQuerySpecification(QuerySpecification node, AnalysisContext parentContext)
    {
        // TODO: extract candidate names from SELECT, WHERE, HAVING, GROUP BY and ORDER BY expressions
        // to pass down to analyzeFrom

        AnalysisContext context = new AnalysisContext(parentContext);

        TupleDescriptor tupleDescriptor = analyzeFrom(node, context);

        node.getWhere().ifPresent(where -> analyzeWhere(node, tupleDescriptor, context, where));

        List<FieldOrExpression> outputExpressions = analyzeSelect(node, tupleDescriptor, context);
        List<FieldOrExpression> groupByExpressions = analyzeGroupBy(node, tupleDescriptor, context, outputExpressions);
        List<FieldOrExpression> orderByExpressions = analyzeOrderBy(node, tupleDescriptor, context, outputExpressions);
        analyzeHaving(node, tupleDescriptor, context);

        analyzeAggregations(node, tupleDescriptor, groupByExpressions, outputExpressions, orderByExpressions, context);
        analyzeWindowFunctions(node, outputExpressions, orderByExpressions);

        TupleDescriptor descriptor = computeOutputDescriptor(node, tupleDescriptor);
        analysis.setOutputDescriptor(node, descriptor);

        return descriptor;
    }

    @Override
    protected TupleDescriptor visitUnion(Union node, AnalysisContext context)
    {
        checkState(node.getRelations().size() >= 2);

        TupleAnalyzer analyzer = new TupleAnalyzer(analysis, session, metadata, sqlParser, experimentalSyntaxEnabled);

        TupleDescriptor[] descriptors = node.getRelations().stream()
                .map(relation -> analyzer.process(relation, context).withOnlyVisibleFields())
                .toArray(TupleDescriptor[]::new);
        Type[] outputFieldTypes = descriptors[0].getVisibleFields().stream()
                .map(field -> field.getType())
                .toArray(Type[]::new);
        for (TupleDescriptor descriptor : descriptors) {
            int outputFieldSize = outputFieldTypes.length;
            int descFieldSize = descriptor.getVisibleFields().size();
            if (outputFieldSize != descFieldSize) {
                throw new SemanticException(MISMATCHED_SET_COLUMN_TYPES,
                                            node,
                                            "union query has different number of fields: %d, %d",
                                            outputFieldSize, descFieldSize);
            }
            for (int i = 0; i < descriptor.getVisibleFields().size(); i++) {
                Type descFieldType = descriptor.getFieldByIndex(i).getType();
                Optional<Type> commonSuperType = FunctionRegistry.getCommonSuperType(outputFieldTypes[i], descFieldType);
                if (!commonSuperType.isPresent()) {
                    throw new SemanticException(TYPE_MISMATCH,
                                                node,
                                                "column %d in union query has incompatible types: %s, %s",
                                                i, outputFieldTypes[i].getDisplayName(), descFieldType.getDisplayName());
                }
                outputFieldTypes[i] = commonSuperType.get();
            }
        }

        Field[] outputDescriptorFields = new Field[outputFieldTypes.length];
        TupleDescriptor firstDescriptor = descriptors[0].withOnlyVisibleFields();
        for (int i = 0; i < outputFieldTypes.length; i++) {
            Field oldField = firstDescriptor.getFieldByIndex(i);
            outputDescriptorFields[i] = new Field(oldField.getRelationAlias(), oldField.getName(), outputFieldTypes[i], oldField.isHidden());
        }
        TupleDescriptor outputDescriptor = new TupleDescriptor(outputDescriptorFields);
        analysis.setOutputDescriptor(node, outputDescriptor);

        for (int i = 0; i < node.getRelations().size(); i++) {
            Relation relation = node.getRelations().get(i);
            TupleDescriptor descriptor = descriptors[i];
            for (int j = 0; j < descriptor.getVisibleFields().size(); j++) {
                Type outputFieldType = outputFieldTypes[j];
                Type descFieldType = descriptor.getFieldByIndex(j).getType();
                if (!outputFieldType.equals(descFieldType)) {
                    analysis.addRelationCoercion(relation, outputFieldTypes);
                    break;
                }
            }
        }

        return outputDescriptor;
    }

    @Override
    protected TupleDescriptor visitIntersect(Intersect node, AnalysisContext context)
    {
        throw new SemanticException(NOT_SUPPORTED, node, "INTERSECT not yet implemented");
    }

    @Override
    protected TupleDescriptor visitExcept(Except node, AnalysisContext context)
    {
        throw new SemanticException(NOT_SUPPORTED, node, "EXCEPT not yet implemented");
    }

    @Override
    protected TupleDescriptor visitJoin(Join node, AnalysisContext context)
    {
        JoinCriteria criteria = node.getCriteria().orElse(null);
        if (criteria instanceof NaturalJoin) {
            throw new SemanticException(NOT_SUPPORTED, node, "Natural join not supported");
        }

        AnalysisContext leftContext = new AnalysisContext(context);
        TupleDescriptor left = process(node.getLeft(), context);
        leftContext.setLateralTupleDescriptor(left);
        TupleDescriptor right = process(node.getRight(), leftContext);

        // todo this check should be inside of TupleDescriptor.join and then remove the public getRelationAlias method, but the exception needs the node object
        Sets.SetView<QualifiedName> duplicateAliases = Sets.intersection(left.getRelationAliases(), right.getRelationAliases());
        if (!duplicateAliases.isEmpty()) {
            throw new SemanticException(DUPLICATE_RELATION, node, "Relations appear more than once: %s", duplicateAliases);
        }

        TupleDescriptor output = left.joinWith(right);

        if (node.getType() == Join.Type.CROSS || node.getType() == Join.Type.IMPLICIT) {
            analysis.setOutputDescriptor(node, output);
            return output;
        }

        if (criteria instanceof JoinUsing) {
            // TODO: implement proper "using" semantics with respect to output columns
            List<String> columns = ((JoinUsing) criteria).getColumns();

            List<Expression> expressions = new ArrayList<>();
            for (String column : columns) {
                Expression leftExpression = new QualifiedNameReference(QualifiedName.of(column));
                Expression rightExpression = new QualifiedNameReference(QualifiedName.of(column));

                ExpressionAnalysis leftExpressionAnalysis = analyzeExpression(leftExpression, left, context);
                ExpressionAnalysis rightExpressionAnalysis = analyzeExpression(rightExpression, right, context);
                checkState(leftExpressionAnalysis.getSubqueryInPredicates().isEmpty(), "INVARIANT");
                checkState(rightExpressionAnalysis.getSubqueryInPredicates().isEmpty(), "INVARIANT");

                addCoercionForJoinCriteria(node, leftExpression, rightExpression);
                expressions.add(new ComparisonExpression(EQUAL, leftExpression, rightExpression));
            }

            analysis.setJoinCriteria(node, ExpressionUtils.and(expressions));
        }
        else if (criteria instanceof JoinOn) {
            Expression expression = ((JoinOn) criteria).getExpression();

            // ensure all names can be resolved, types match, etc (we don't need to record resolved names, subexpression types, etc. because
            // we do it further down when after we determine which subexpressions apply to left vs right tuple)
            ExpressionAnalyzer analyzer = ExpressionAnalyzer.create(analysis, session, metadata, sqlParser, experimentalSyntaxEnabled);
            analyzer.analyze(expression, output, context);

            Analyzer.verifyNoAggregatesOrWindowFunctions(metadata, expression, "JOIN");

            // expressionInterpreter/optimizer only understands a subset of expression types
            // TODO: remove this when the new expression tree is implemented
            Expression canonicalized = CanonicalizeExpressions.canonicalizeExpression(expression);

            Object optimizedExpression = expressionOptimizer(canonicalized, metadata, session, analyzer.getExpressionTypes()).optimize(NoOpSymbolResolver.INSTANCE);

            if (!(optimizedExpression instanceof Expression) && optimizedExpression instanceof Boolean) {
                // If the JoinOn clause evaluates to a boolean expression, simulate a cross join by adding the relevant redundant expression
                if (optimizedExpression.equals(Boolean.TRUE)) {
                    optimizedExpression = new ComparisonExpression(EQUAL, new LongLiteral("0"), new LongLiteral("0"));
                }
                else {
                    optimizedExpression = new ComparisonExpression(EQUAL, new LongLiteral("0"), new LongLiteral("1"));
                }
            }

            if (!(optimizedExpression instanceof Expression)) {
                throw new SemanticException(TYPE_MISMATCH, node, "Join clause must be a boolean expression");
            }
            // The optimization above may have rewritten the expression tree which breaks all the identity maps, so redo the analysis
            // to re-analyze coercions that might be necessary
            analyzer = ExpressionAnalyzer.create(analysis, session, metadata, sqlParser, experimentalSyntaxEnabled);
            analyzer.analyze((Expression) optimizedExpression, output, context);
            analysis.addCoercions(analyzer.getExpressionCoercions());

            for (Expression conjunct : ExpressionUtils.extractConjuncts((Expression) optimizedExpression)) {
                if (!(conjunct instanceof ComparisonExpression)) {
                    throw new SemanticException(NOT_SUPPORTED, node, "Non-equi joins not supported: %s", conjunct);
                }

                ComparisonExpression comparison = (ComparisonExpression) conjunct;
                Set<QualifiedName> firstDependencies = DependencyExtractor.extract(comparison.getLeft());
                Set<QualifiedName> secondDependencies = DependencyExtractor.extract(comparison.getRight());

                Expression leftExpression;
                Expression rightExpression;
                if (Iterables.all(firstDependencies, left.canResolvePredicate()) && Iterables.all(secondDependencies, right.canResolvePredicate())) {
                    leftExpression = comparison.getLeft();
                    rightExpression = comparison.getRight();
                }
                else if (Iterables.all(firstDependencies, right.canResolvePredicate()) && Iterables.all(secondDependencies, left.canResolvePredicate())) {
                    leftExpression = comparison.getRight();
                    rightExpression = comparison.getLeft();
                }
                else {
                    // must have a complex expression that involves both tuples on one side of the comparison expression (e.g., coalesce(left.x, right.x) = 1)
                    throw new SemanticException(NOT_SUPPORTED, node, "Non-equi joins not supported: %s", conjunct);
                }

                // analyze the clauses to record the types of all subexpressions and resolve names against the left/right underlying tuples
                ExpressionAnalysis leftExpressionAnalysis = analyzeExpression(leftExpression, left, context);
                ExpressionAnalysis rightExpressionAnalysis = analyzeExpression(rightExpression, right, context);
                addCoercionForJoinCriteria(node, leftExpression, rightExpression);
                analysis.addJoinInPredicates(node, new Analysis.JoinInPredicates(leftExpressionAnalysis.getSubqueryInPredicates(), rightExpressionAnalysis.getSubqueryInPredicates()));
            }

            analysis.setJoinCriteria(node, (Expression) optimizedExpression);
        }
        else {
            throw new UnsupportedOperationException("unsupported join criteria: " + criteria.getClass().getName());
        }

        analysis.setOutputDescriptor(node, output);
        return output;
    }

    private void addCoercionForJoinCriteria(Join node, Expression leftExpression, Expression rightExpression)
    {
        Type leftType = analysis.getType(leftExpression);
        Type rightType = analysis.getType(rightExpression);
        Optional<Type> superType = FunctionRegistry.getCommonSuperType(leftType, rightType);
        if (!superType.isPresent()) {
            throw new SemanticException(TYPE_MISMATCH, node, "Join criteria has incompatible types: %s, %s", leftType.getDisplayName(), rightType.getDisplayName());
        }
        if (!leftType.equals(superType.get())) {
            analysis.addCoercion(leftExpression, superType.get());
        }
        if (!rightType.equals(superType.get())) {
            analysis.addCoercion(rightExpression, superType.get());
        }
    }

    @Override
    protected TupleDescriptor visitValues(Values node, AnalysisContext context)
    {
        checkState(node.getRows().size() >= 1);

        // get unique row types
        Set<List<Type>> rowTypes = node.getRows().stream()
                .map(row -> analyzeExpression(row, new TupleDescriptor(), context).getType(row))
                .map(type -> {
                    if (type instanceof RowType) {
                        return type.getTypeParameters();
                    }
                    return ImmutableList.of(type);
                })
                .collect(ImmutableCollectors.toImmutableSet());

        // determine common super type of the rows
        List<Type> fieldTypes = new ArrayList<>(rowTypes.iterator().next());
        for (List<Type> rowType : rowTypes) {
            for (int i = 0; i < rowType.size(); i++) {
                Type fieldType = rowType.get(i);
                Type superType = fieldTypes.get(i);

                Optional<Type> commonSuperType = getCommonSuperType(fieldType, superType);
                if (!commonSuperType.isPresent()) {
                    throw new SemanticException(MISMATCHED_SET_COLUMN_TYPES,
                            node,
                            "Values rows have mismatched types: %s vs %s",
                            Iterables.get(rowTypes, 0),
                            Iterables.get(rowTypes, 1));
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
                    if (!analysis.getType(item).equals(expectedType)) {
                        analysis.addCoercion(item, expectedType);
                    }
                }
            }
            else {
                Type expectedType = fieldTypes.get(0);
                if (!analysis.getType(row).equals(expectedType)) {
                    analysis.addCoercion(row, expectedType);
                }
            }
        }

        TupleDescriptor descriptor = new TupleDescriptor(fieldTypes.stream()
                .map(valueType -> Field.newUnqualified(Optional.empty(), valueType))
                .collect(toImmutableList()));

        analysis.setOutputDescriptor(node, descriptor);
        return descriptor;
    }

    private void analyzeWindowFunctions(QuerySpecification node, List<FieldOrExpression> outputExpressions, List<FieldOrExpression> orderByExpressions)
    {
        WindowFunctionExtractor extractor = new WindowFunctionExtractor();

        for (FieldOrExpression fieldOrExpression : Iterables.concat(outputExpressions, orderByExpressions)) {
            if (fieldOrExpression.isExpression()) {
                extractor.process(fieldOrExpression.getExpression(), null);
                new WindowFunctionValidator().process(fieldOrExpression.getExpression(), analysis);
            }
        }

        List<FunctionCall> windowFunctions = extractor.getWindowFunctions();

        for (FunctionCall windowFunction : windowFunctions) {
            Window window = windowFunction.getWindow().get();

            WindowFunctionExtractor nestedExtractor = new WindowFunctionExtractor();
            for (Expression argument : windowFunction.getArguments()) {
                nestedExtractor.process(argument, null);
            }

            for (Expression expression : window.getPartitionBy()) {
                nestedExtractor.process(expression, null);
            }

            for (SortItem sortItem : window.getOrderBy()) {
                nestedExtractor.process(sortItem.getSortKey(), null);
            }

            if (window.getFrame().isPresent()) {
                nestedExtractor.process(window.getFrame().get(), null);
            }

            if (!nestedExtractor.getWindowFunctions().isEmpty()) {
                throw new SemanticException(NESTED_WINDOW, node, "Cannot nest window functions inside window function '%s': %s",
                        windowFunction,
                        extractor.getWindowFunctions());
            }

            if (windowFunction.isDistinct()) {
                throw new SemanticException(NOT_SUPPORTED, node, "DISTINCT in window function parameters not yet supported: %s", windowFunction);
            }

            if (window.getFrame().isPresent()) {
                analyzeWindowFrame(window.getFrame().get());
            }

            List<TypeSignature> argumentTypes = Lists.transform(windowFunction.getArguments(), expression -> analysis.getType(expression).getTypeSignature());

            FunctionInfo info = metadata.resolveFunction(windowFunction.getName(), argumentTypes, false);
            if (!info.isWindow()) {
                throw new SemanticException(MUST_BE_WINDOW_FUNCTION, node, "Not a window function: %s", windowFunction.getName());
            }
        }

        analysis.setWindowFunctions(node, windowFunctions);
    }

    private static void analyzeWindowFrame(WindowFrame frame)
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

    private void analyzeHaving(QuerySpecification node, TupleDescriptor tupleDescriptor, AnalysisContext context)
    {
        if (node.getHaving().isPresent()) {
            Expression predicate = node.getHaving().get();

            ExpressionAnalysis expressionAnalysis = analyzeExpression(predicate, tupleDescriptor, context);
            analysis.addInPredicates(node, expressionAnalysis.getSubqueryInPredicates());

            Type predicateType = expressionAnalysis.getType(predicate);
            if (!predicateType.equals(BOOLEAN) && !predicateType.equals(UNKNOWN)) {
                throw new SemanticException(TYPE_MISMATCH, predicate, "HAVING clause must evaluate to a boolean: actual type %s", predicateType);
            }

            analysis.setHaving(node, predicate);
        }
    }

    private List<FieldOrExpression> analyzeOrderBy(QuerySpecification node, TupleDescriptor tupleDescriptor, AnalysisContext context, List<FieldOrExpression> outputExpressions)
    {
        List<SortItem> items = node.getOrderBy();

        ImmutableList.Builder<FieldOrExpression> orderByExpressionsBuilder = ImmutableList.builder();

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

                FieldOrExpression orderByExpression = null;
                if (expression instanceof QualifiedNameReference && !((QualifiedNameReference) expression).getName().getPrefix().isPresent()) {
                    // if this is a simple name reference, try to resolve against output columns

                    QualifiedName name = ((QualifiedNameReference) expression).getName();
                    Collection<Expression> expressions = byAlias.get(name);
                    if (expressions.size() > 1) {
                        throw new SemanticException(AMBIGUOUS_ATTRIBUTE, expression, "'%s' in ORDER BY is ambiguous", name.getSuffix());
                    }
                    else if (expressions.size() == 1) {
                        orderByExpression = new FieldOrExpression(Iterables.getOnlyElement(expressions));
                    }

                    // otherwise, couldn't resolve name against output aliases, so fall through...
                }
                else if (expression instanceof LongLiteral) {
                    // this is an ordinal in the output tuple

                    long ordinal = ((LongLiteral) expression).getValue();
                    if (ordinal < 1 || ordinal > outputExpressions.size()) {
                        throw new SemanticException(INVALID_ORDINAL, expression, "ORDER BY position %s is not in select list", ordinal);
                    }

                    orderByExpression = outputExpressions.get((int) (ordinal - 1));

                    if (orderByExpression.isExpression()) {
                        Type type = analysis.getType(orderByExpression.getExpression());
                        if (!type.isOrderable()) {
                            throw new SemanticException(TYPE_MISMATCH, node, "The type of expression in position %s is not orderable (actual: %s), and therefore cannot be used in ORDER BY: %s", ordinal, type, orderByExpression);
                        }
                    }
                    else {
                        Type type = tupleDescriptor.getFieldByIndex(orderByExpression.getFieldIndex()).getType();
                        if (!type.isOrderable()) {
                            throw new SemanticException(TYPE_MISMATCH, node, "The type of expression in position %s is not orderable (actual: %s), and therefore cannot be used in ORDER BY", ordinal, type);
                        }
                    }
                }

                // otherwise, just use the expression as is
                if (orderByExpression == null) {
                    orderByExpression = new FieldOrExpression(expression);
                }

                if (orderByExpression.isExpression()) {
                    ExpressionAnalysis expressionAnalysis = analyzeExpression(orderByExpression.getExpression(), tupleDescriptor, context);
                    analysis.addInPredicates(node, expressionAnalysis.getSubqueryInPredicates());

                    Type type = expressionAnalysis.getType(orderByExpression.getExpression());
                    if (!type.isOrderable()) {
                        throw new SemanticException(TYPE_MISMATCH, node, "Type %s is not orderable, and therefore cannot be used in ORDER BY: %s", type, expression);
                    }
                }

                orderByExpressionsBuilder.add(orderByExpression);
            }
        }

        List<FieldOrExpression> orderByExpressions = orderByExpressionsBuilder.build();
        analysis.setOrderByExpressions(node, orderByExpressions);

        if (node.getSelect().isDistinct() && !outputExpressions.containsAll(orderByExpressions)) {
            throw new SemanticException(ORDER_BY_MUST_BE_IN_SELECT, node.getSelect(), "For SELECT DISTINCT, ORDER BY expressions must appear in select list");
        }
        return orderByExpressions;
    }

    private List<FieldOrExpression> analyzeGroupBy(QuerySpecification node, TupleDescriptor tupleDescriptor, AnalysisContext context, List<FieldOrExpression> outputExpressions)
    {
        ImmutableList.Builder<FieldOrExpression> groupByExpressionsBuilder = ImmutableList.builder();
        if (!node.getGroupBy().isEmpty()) {
            // Translate group by expressions that reference ordinals
            for (Expression expression : node.getGroupBy()) {
                // first, see if this is an ordinal
                FieldOrExpression groupByExpression;

                if (expression instanceof LongLiteral) {
                    long ordinal = ((LongLiteral) expression).getValue();
                    if (ordinal < 1 || ordinal > outputExpressions.size()) {
                        throw new SemanticException(INVALID_ORDINAL, expression, "GROUP BY position %s is not in select list", ordinal);
                    }

                    groupByExpression = outputExpressions.get((int) (ordinal - 1));
                }
                else {
                    ExpressionAnalysis expressionAnalysis = analyzeExpression(expression, tupleDescriptor, context);
                    analysis.addInPredicates(node, expressionAnalysis.getSubqueryInPredicates());
                    groupByExpression = new FieldOrExpression(expression);
                }

                Type type;
                if (groupByExpression.isExpression()) {
                    Analyzer.verifyNoAggregatesOrWindowFunctions(metadata, groupByExpression.getExpression(), "GROUP BY");
                    type = analysis.getType(groupByExpression.getExpression());
                }
                else {
                    type = tupleDescriptor.getFieldByIndex(groupByExpression.getFieldIndex()).getType();
                }
                if (!type.isComparable()) {
                    throw new SemanticException(TYPE_MISMATCH, node, "%s is not comparable, and therefore cannot be used in GROUP BY", type);
                }

                groupByExpressionsBuilder.add(groupByExpression);
            }
        }

        List<FieldOrExpression> groupByExpressions = groupByExpressionsBuilder.build();
        analysis.setGroupByExpressions(node, groupByExpressions);
        return groupByExpressions;
    }

    private TupleDescriptor computeOutputDescriptor(QuerySpecification node, TupleDescriptor inputTupleDescriptor)
    {
        ImmutableList.Builder<Field> outputFields = ImmutableList.builder();

        for (SelectItem item : node.getSelect().getSelectItems()) {
            if (item instanceof AllColumns) {
                // expand * and T.*
                Optional<QualifiedName> starPrefix = ((AllColumns) item).getPrefix();

                for (Field field : inputTupleDescriptor.resolveFieldsWithPrefix(starPrefix)) {
                    outputFields.add(Field.newUnqualified(field.getName(), field.getType()));
                }
            }
            else if (item instanceof SingleColumn) {
                SingleColumn column = (SingleColumn) item;

                Optional<String> alias = column.getAlias();
                if (!alias.isPresent() && column.getExpression() instanceof QualifiedNameReference) {
                    alias = Optional.of(((QualifiedNameReference) column.getExpression()).getName().getSuffix());
                }

                outputFields.add(Field.newUnqualified(alias, analysis.getType(column.getExpression()))); // TODO don't use analysis as a side-channel. Use outputExpressions to look up the type
            }
            else {
                throw new IllegalArgumentException("Unsupported SelectItem type: " + item.getClass().getName());
            }
        }

        return new TupleDescriptor(outputFields.build());
    }

    private List<FieldOrExpression> analyzeSelect(QuerySpecification node, TupleDescriptor tupleDescriptor, AnalysisContext context)
    {
        ImmutableList.Builder<FieldOrExpression> outputExpressionBuilder = ImmutableList.builder();

        for (SelectItem item : node.getSelect().getSelectItems()) {
            if (item instanceof AllColumns) {
                // expand * and T.*
                Optional<QualifiedName> starPrefix = ((AllColumns) item).getPrefix();

                List<Field> fields = tupleDescriptor.resolveFieldsWithPrefix(starPrefix);
                if (fields.isEmpty()) {
                    if (starPrefix.isPresent()) {
                        throw new SemanticException(MISSING_TABLE, item, "Table '%s' not found", starPrefix.get());
                    }
                    else {
                        throw new SemanticException(WILDCARD_WITHOUT_FROM, item, "SELECT * not allowed in queries without FROM clause");
                    }
                }

                for (Field field : fields) {
                    int fieldIndex = tupleDescriptor.indexOf(field);
                    outputExpressionBuilder.add(new FieldOrExpression(fieldIndex));

                    if (node.getSelect().isDistinct() && !field.getType().isComparable()) {
                        throw new SemanticException(TYPE_MISMATCH, node.getSelect(), "DISTINCT can only be applied to comparable types (actual: %s)", field.getType());
                    }
                }
            }
            else if (item instanceof SingleColumn) {
                SingleColumn column = (SingleColumn) item;
                ExpressionAnalysis expressionAnalysis = analyzeExpression(column.getExpression(), tupleDescriptor, context);
                analysis.addInPredicates(node, expressionAnalysis.getSubqueryInPredicates());
                outputExpressionBuilder.add(new FieldOrExpression(column.getExpression()));

                Type type = expressionAnalysis.getType(column.getExpression());
                if (node.getSelect().isDistinct() && !type.isComparable()) {
                    throw new SemanticException(TYPE_MISMATCH, node.getSelect(), "DISTINCT can only be applied to comparable types (actual: %s): %s", type, column.getExpression());
                }
            }
            else {
                throw new IllegalArgumentException("Unsupported SelectItem type: " + item.getClass().getName());
            }
        }

        ImmutableList<FieldOrExpression> result = outputExpressionBuilder.build();
        analysis.setOutputExpressions(node, result);

        return result;
    }

    public void analyzeWhere(Node node, TupleDescriptor tupleDescriptor, AnalysisContext context, Expression predicate)
    {
        Analyzer.verifyNoAggregatesOrWindowFunctions(metadata, predicate, "WHERE");

        ExpressionAnalysis expressionAnalysis = analyzeExpression(predicate, tupleDescriptor, context);
        analysis.addInPredicates(node, expressionAnalysis.getSubqueryInPredicates());

        Type predicateType = expressionAnalysis.getType(predicate);
        if (!predicateType.equals(BOOLEAN)) {
            if (!predicateType.equals(UNKNOWN)) {
                throw new SemanticException(TYPE_MISMATCH, predicate, "WHERE clause must evaluate to a boolean: actual type %s", predicateType);
            }
            // coerce null to boolean
            analysis.addCoercion(predicate, BOOLEAN);
        }

        analysis.setWhere(node, predicate);
    }

    private TupleDescriptor analyzeFrom(QuerySpecification node, AnalysisContext context)
    {
        TupleDescriptor fromDescriptor = new TupleDescriptor();

        if (node.getFrom().isPresent()) {
            TupleAnalyzer analyzer = new TupleAnalyzer(analysis, session, metadata, sqlParser, experimentalSyntaxEnabled);
            fromDescriptor = analyzer.process(node.getFrom().get(), context);
        }

        return fromDescriptor;
    }

    private void analyzeAggregations(QuerySpecification node,
            TupleDescriptor tupleDescriptor,
            List<FieldOrExpression> groupByExpressions,
            List<FieldOrExpression> outputExpressions,
            List<FieldOrExpression> orderByExpressions,
            AnalysisContext context)
    {
        List<FunctionCall> aggregates = extractAggregates(node);

        if (context.isApproximate()) {
            if (Iterables.any(aggregates, FunctionCall::isDistinct)) {
                throw new SemanticException(NOT_SUPPORTED, node, "DISTINCT aggregations not supported for approximate queries");
            }
        }

        // is this an aggregation query?
        if (!aggregates.isEmpty() || !groupByExpressions.isEmpty()) {
            // ensure SELECT, ORDER BY and HAVING are constant with respect to group
            // e.g, these are all valid expressions:
            //     SELECT f(a) GROUP BY a
            //     SELECT f(a + 1) GROUP BY a + 1
            //     SELECT a + sum(b) GROUP BY a
            for (FieldOrExpression fieldOrExpression : Iterables.concat(outputExpressions, orderByExpressions)) {
                verifyAggregations(node, groupByExpressions, tupleDescriptor, fieldOrExpression);
            }

            if (node.getHaving().isPresent()) {
                verifyAggregations(node, groupByExpressions, tupleDescriptor, new FieldOrExpression(node.getHaving().get()));
            }
        }
    }

    private List<FunctionCall> extractAggregates(QuerySpecification node)
    {
        AggregateExtractor extractor = new AggregateExtractor(metadata);
        for (SelectItem item : node.getSelect().getSelectItems()) {
            if (item instanceof SingleColumn) {
                ((SingleColumn) item).getExpression().accept(extractor, null);
            }
        }

        for (SortItem item : node.getOrderBy()) {
            item.getSortKey().accept(extractor, null);
        }

        if (node.getHaving().isPresent()) {
            node.getHaving().get().accept(extractor, null);
        }

        List<FunctionCall> aggregates = extractor.getAggregates();
        analysis.setAggregates(node, aggregates);

        return aggregates;
    }

    private void verifyAggregations(QuerySpecification node, List<FieldOrExpression> groupByExpressions, TupleDescriptor tupleDescriptor, FieldOrExpression fieldOrExpression)
    {
        AggregationAnalyzer analyzer = new AggregationAnalyzer(groupByExpressions, metadata, tupleDescriptor);

        if (fieldOrExpression.isExpression()) {
            analyzer.analyze(fieldOrExpression.getExpression());
        }
        else {
            int fieldIndex = fieldOrExpression.getFieldIndex();
            if (!analyzer.analyze(fieldIndex)) {
                Field field = tupleDescriptor.getFieldByIndex(fieldIndex);

                if (field.getRelationAlias().isPresent()) {
                    if (field.getName().isPresent()) {
                        throw new SemanticException(MUST_BE_AGGREGATE_OR_GROUP_BY, node, "Column '%s.%s' not in GROUP BY clause", field.getRelationAlias().get(), field.getName().get());
                    }
                    else {
                        throw new SemanticException(MUST_BE_AGGREGATE_OR_GROUP_BY, node, "Columns from '%s' not in GROUP BY clause", field.getRelationAlias().get());
                    }
                }
                else {
                    if (field.getName().isPresent()) {
                        throw new SemanticException(MUST_BE_AGGREGATE_OR_GROUP_BY, node, "Column '%s' not in GROUP BY clause", field.getName().get());
                    }
                    else {
                        throw new SemanticException(MUST_BE_AGGREGATE_OR_GROUP_BY, node, "Some columns from FROM clause not in GROUP BY clause");
                    }
                }
            }
        }
    }

    private TupleDescriptor analyzeView(Query query, QualifiedTableName name, String catalog, String schema, Table node)
    {
        try {
            Session viewSession = Session.builder()
                    .setUser(session.getUser())
                    .setSource(session.getSource())
                    .setCatalog(catalog)
                    .setSchema(schema)
                    .setTimeZoneKey(session.getTimeZoneKey())
                    .setLocale(session.getLocale())
                    .setRemoteUserAddress(session.getRemoteUserAddress())
                    .setUserAgent(session.getUserAgent())
                    .setStartTime(session.getStartTime())
                    .build();

            StatementAnalyzer analyzer = new StatementAnalyzer(analysis, metadata, sqlParser, viewSession, experimentalSyntaxEnabled, Optional.empty());
            return analyzer.process(query, new AnalysisContext());
        }
        catch (RuntimeException e) {
            throw new SemanticException(VIEW_ANALYSIS_ERROR, node, "Failed analyzing stored view '%s': %s", name, e.getMessage());
        }
    }

    private Query parseView(String view, QualifiedTableName name, Table node)
    {
        try {
            Statement statement = sqlParser.createStatement(view);
            return checkType(statement, Query.class, "parsed view");
        }
        catch (ParsingException e) {
            throw new SemanticException(VIEW_PARSE_ERROR, node, "Failed parsing stored view '%s': %s", name, e.getMessage());
        }
    }

    private static boolean isViewStale(List<ViewColumn> columns, Collection<Field> fields)
    {
        if (columns.size() != fields.size()) {
            return true;
        }

        List<Field> fieldList = ImmutableList.copyOf(fields);
        for (int i = 0; i < columns.size(); i++) {
            ViewColumn column = columns.get(i);
            Field field = fieldList.get(i);
            if (!column.getName().equals(field.getName().orElse(null)) ||
                    !column.getType().equals(field.getType())) {
                return true;
            }
        }

        return false;
    }

    private ExpressionAnalysis analyzeExpression(Expression expression, TupleDescriptor tupleDescriptor, AnalysisContext context)
    {
        return ExpressionAnalyzer.analyzeExpression(
                session,
                metadata,
                sqlParser,
                tupleDescriptor,
                analysis,
                experimentalSyntaxEnabled,
                context,
                expression);
    }

    public static class DependencyExtractor
    {
        public static Set<QualifiedName> extract(Expression expression)
        {
            ImmutableSet.Builder<QualifiedName> builder = ImmutableSet.builder();

            Visitor visitor = new Visitor();
            visitor.process(expression, builder);

            return builder.build();
        }

        private static class Visitor
                extends DefaultExpressionTraversalVisitor<Void, ImmutableSet.Builder<QualifiedName>>
        {
            @Override
            protected Void visitQualifiedNameReference(QualifiedNameReference node, ImmutableSet.Builder<QualifiedName> builder)
            {
                builder.add(node.getName());
                return null;
            }
        }
    }
}
