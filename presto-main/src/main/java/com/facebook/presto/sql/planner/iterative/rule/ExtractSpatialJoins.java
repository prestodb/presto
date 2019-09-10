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
package com.facebook.presto.sql.planner.iterative.rule;

import com.facebook.presto.Session;
import com.facebook.presto.execution.Lifespan;
import com.facebook.presto.geospatial.KdbTree;
import com.facebook.presto.geospatial.KdbTreeUtils;
import com.facebook.presto.matching.Capture;
import com.facebook.presto.matching.Captures;
import com.facebook.presto.matching.Pattern;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.metadata.QualifiedObjectName;
import com.facebook.presto.metadata.Split;
import com.facebook.presto.metadata.TableLayoutResult;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorPageSource;
import com.facebook.presto.spi.Constraint;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.TableHandle;
import com.facebook.presto.spi.function.OperatorType;
import com.facebook.presto.spi.plan.FilterNode;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.PlanNodeId;
import com.facebook.presto.spi.relation.CallExpression;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.RowExpressionNodeInliner;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.spi.type.ArrayType;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeSignature;
import com.facebook.presto.split.PageSourceManager;
import com.facebook.presto.split.SplitManager;
import com.facebook.presto.split.SplitSource;
import com.facebook.presto.split.SplitSource.SplitBatch;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.planner.VariablesExtractor;
import com.facebook.presto.sql.planner.iterative.Rule;
import com.facebook.presto.sql.planner.iterative.Rule.Context;
import com.facebook.presto.sql.planner.iterative.Rule.Result;
import com.facebook.presto.sql.planner.plan.Assignments;
import com.facebook.presto.sql.planner.plan.JoinNode;
import com.facebook.presto.sql.planner.plan.ProjectNode;
import com.facebook.presto.sql.planner.plan.SpatialJoinNode;
import com.facebook.presto.sql.planner.plan.UnnestNode;
import com.facebook.presto.sql.relational.FunctionResolution;
import com.facebook.presto.sql.relational.StandardExpressions;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import io.airlift.slice.Slices;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.facebook.presto.SystemSessionProperties.getSpatialPartitioningTableName;
import static com.facebook.presto.SystemSessionProperties.isSpatialJoinEnabled;
import static com.facebook.presto.matching.Capture.newCapture;
import static com.facebook.presto.spi.StandardErrorCode.INVALID_SPATIAL_PARTITIONING;
import static com.facebook.presto.spi.connector.ConnectorSplitManager.SplitSchedulingStrategy.UNGROUPED_SCHEDULING;
import static com.facebook.presto.spi.connector.NotPartitionedPartitionHandle.NOT_PARTITIONED;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.IntegerType.INTEGER;
import static com.facebook.presto.spi.type.TypeSignature.parseTypeSignature;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.facebook.presto.sql.planner.plan.AssignmentUtils.identityAsSymbolReference;
import static com.facebook.presto.sql.planner.plan.JoinNode.Type.INNER;
import static com.facebook.presto.sql.planner.plan.JoinNode.Type.LEFT;
import static com.facebook.presto.sql.planner.plan.Patterns.filter;
import static com.facebook.presto.sql.planner.plan.Patterns.join;
import static com.facebook.presto.sql.planner.plan.Patterns.source;
import static com.facebook.presto.sql.relational.Expressions.call;
import static com.facebook.presto.sql.relational.Expressions.constant;
import static com.facebook.presto.sql.relational.StandardExpressions.getLeft;
import static com.facebook.presto.sql.relational.StandardExpressions.getRight;
import static com.facebook.presto.util.SpatialJoinUtils.extractSupportedSpatialComparisons;
import static com.facebook.presto.util.SpatialJoinUtils.extractSupportedSpatialFunctions;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.airlift.concurrent.MoreFutures.getFutureValue;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

/**
 * Applies to broadcast spatial joins, inner and left, expressed via ST_Contains,
 * ST_Intersects and ST_Distance functions.
 * <p>
 * For example:
 * <ul>
 * <li>SELECT ... FROM a, b WHERE ST_Contains(b.geometry, a.geometry)</li>
 * <li>SELECT ... FROM a, b WHERE ST_Intersects(b.geometry, a.geometry)</li>
 * <li>SELECT ... FROM a, b WHERE ST_Distance(b.geometry, a.geometry) <= 300</li>
 * <li>SELECT ... FROM a, b WHERE 15.5 > ST_Distance(b.geometry, a.geometry)</li>
 * </ul>
 * <p>
 * Joins expressed via ST_Contains and ST_Intersects functions must match all of
 * the following criteria:
 * <p>
 * - arguments of the spatial function are non-scalar expressions;
 * - one of the arguments uses symbols from left side of the join, the other from right.
 * <p>
 * Joins expressed via ST_Distance function must use less than or less than or equals operator
 * to compare ST_Distance value with a radius and must match all of the following criteria:
 * <p>
 * - arguments of the spatial function are non-scalar expressions;
 * - one of the arguments uses symbols from left side of the join, the other from right;
 * - radius is either scalar expression or uses symbols only from the right (build) side of the join.
 * <p>
 * For inner join, replaces cross join node and a qualifying filter on top with a single
 * spatial join node.
 * <p>
 * For both inner and left joins, pushes non-trivial expressions of the spatial function
 * arguments and radius into projections on top of join child nodes.
 * <p>
 * Examples:
 * <pre>
 * Point-in-polygon inner join
 *      ST_Contains(ST_GeometryFromText(a.wkt), ST_Point(b.longitude, b.latitude))
 * becomes a spatial join
 *      ST_Contains(st_geometryfromtext, st_point)
 * with st_geometryfromtext -> 'ST_GeometryFromText(a.wkt)' and
 * st_point -> 'ST_Point(b.longitude, b.latitude)' projections on top of child nodes.
 *
 * Distance query
 *      ST_Distance(ST_Point(a.lon, a.lat), ST_Point(b.lon, b.lat)) <= 10 / (111.321 * cos(radians(b.lat)))
 * becomes a spatial join
 *      ST_Distance(st_point_a, st_point_b) <= radius
 * with st_point_a -> 'ST_Point(a.lon, a.lat)', st_point_b -> 'ST_Point(b.lon, b.lat)'
 * and radius -> '10 / (111.321 * cos(radians(b.lat)))' projections on top of child nodes.
 * </pre>
 */
public class ExtractSpatialJoins
{
    private static final TypeSignature GEOMETRY_TYPE_SIGNATURE = parseTypeSignature("Geometry");
    private static final TypeSignature SPHERICAL_GEOGRAPHY_TYPE_SIGNATURE = parseTypeSignature("SphericalGeography");
    private static final String KDB_TREE_TYPENAME = "KdbTree";

    private final Metadata metadata;
    private final SplitManager splitManager;
    private final PageSourceManager pageSourceManager;
    private final SqlParser sqlParser;

    public ExtractSpatialJoins(Metadata metadata, SplitManager splitManager, PageSourceManager pageSourceManager, SqlParser sqlParser)
    {
        this.metadata = requireNonNull(metadata, "metadata is null");
        this.splitManager = requireNonNull(splitManager, "splitManager is null");
        this.pageSourceManager = requireNonNull(pageSourceManager, "pageSourceManager is null");
        this.sqlParser = requireNonNull(sqlParser, "sqlParser is null");
    }

    public Set<Rule<?>> rules()
    {
        return ImmutableSet.of(
                new ExtractSpatialInnerJoin(metadata, splitManager, pageSourceManager, sqlParser),
                new ExtractSpatialLeftJoin(metadata, splitManager, pageSourceManager, sqlParser));
    }

    @VisibleForTesting
    public static final class ExtractSpatialInnerJoin
            implements Rule<FilterNode>
    {
        private static final Capture<JoinNode> JOIN = newCapture();
        private static final Pattern<FilterNode> PATTERN = filter()
                .with(source().matching(join().capturedAs(JOIN).matching(JoinNode::isCrossJoin)));

        private final Metadata metadata;
        private final SplitManager splitManager;
        private final PageSourceManager pageSourceManager;
        private final SqlParser sqlParser;

        public ExtractSpatialInnerJoin(Metadata metadata, SplitManager splitManager, PageSourceManager pageSourceManager, SqlParser sqlParser)
        {
            this.metadata = requireNonNull(metadata, "metadata is null");
            this.splitManager = requireNonNull(splitManager, "splitManager is null");
            this.pageSourceManager = requireNonNull(pageSourceManager, "pageSourceManager is null");
            this.sqlParser = requireNonNull(sqlParser, "sqlParser is null");
        }

        @Override
        public boolean isEnabled(Session session)
        {
            return isSpatialJoinEnabled(session);
        }

        @Override
        public Pattern<FilterNode> getPattern()
        {
            return PATTERN;
        }

        @Override
        public Result apply(FilterNode node, Captures captures, Context context)
        {
            JoinNode joinNode = captures.get(JOIN);
            RowExpression filter = node.getPredicate();
            List<CallExpression> spatialFunctions = extractSupportedSpatialFunctions(filter, metadata.getFunctionManager());
            for (CallExpression spatialFunction : spatialFunctions) {
                Result result = tryCreateSpatialJoin(context, joinNode, filter, node.getId(), node.getOutputVariables(), spatialFunction, Optional.empty(), metadata, splitManager, pageSourceManager);
                if (!result.isEmpty()) {
                    return result;
                }
            }

            List<CallExpression> spatialComparisons = extractSupportedSpatialComparisons(filter, metadata.getFunctionManager());
            for (CallExpression spatialComparison : spatialComparisons) {
                Result result = tryCreateSpatialJoin(context, joinNode, filter, node.getId(), node.getOutputVariables(), spatialComparison, metadata, splitManager, pageSourceManager);
                if (!result.isEmpty()) {
                    return result;
                }
            }

            return Result.empty();
        }
    }

    @VisibleForTesting
    public static final class ExtractSpatialLeftJoin
            implements Rule<JoinNode>
    {
        private static final Pattern<JoinNode> PATTERN = join().matching(node -> node.getCriteria().isEmpty() && node.getFilter().isPresent() && node.getType() == LEFT);

        private final Metadata metadata;
        private final SplitManager splitManager;
        private final PageSourceManager pageSourceManager;
        private final SqlParser sqlParser;

        public ExtractSpatialLeftJoin(Metadata metadata, SplitManager splitManager, PageSourceManager pageSourceManager, SqlParser sqlParser)
        {
            this.metadata = requireNonNull(metadata, "metadata is null");
            this.splitManager = requireNonNull(splitManager, "splitManager is null");
            this.pageSourceManager = requireNonNull(pageSourceManager, "pageSourceManager is null");
            this.sqlParser = requireNonNull(sqlParser, "sqlParser is null");
        }

        @Override
        public boolean isEnabled(Session session)
        {
            return isSpatialJoinEnabled(session);
        }

        @Override
        public Pattern<JoinNode> getPattern()
        {
            return PATTERN;
        }

        @Override
        public Result apply(JoinNode joinNode, Captures captures, Context context)
        {
            RowExpression filter = joinNode.getFilter().get();
            List<CallExpression> spatialFunctions = extractSupportedSpatialFunctions(filter, metadata.getFunctionManager());
            for (CallExpression spatialFunction : spatialFunctions) {
                Result result = tryCreateSpatialJoin(context, joinNode, filter, joinNode.getId(), joinNode.getOutputVariables(), spatialFunction, Optional.empty(), metadata, splitManager, pageSourceManager);
                if (!result.isEmpty()) {
                    return result;
                }
            }

            List<CallExpression> spatialComparisons = extractSupportedSpatialComparisons(filter, metadata.getFunctionManager());
            for (CallExpression spatialComparison : spatialComparisons) {
                Result result = tryCreateSpatialJoin(context, joinNode, filter, joinNode.getId(), joinNode.getOutputVariables(), spatialComparison, metadata, splitManager, pageSourceManager);
                if (!result.isEmpty()) {
                    return result;
                }
            }

            return Result.empty();
        }
    }

    private static OperatorType flip(OperatorType operatorType)
    {
        switch (operatorType) {
            case EQUAL:
                return OperatorType.EQUAL;
            case NOT_EQUAL:
                return OperatorType.NOT_EQUAL;
            case LESS_THAN:
                return OperatorType.GREATER_THAN;
            case LESS_THAN_OR_EQUAL:
                return OperatorType.GREATER_THAN_OR_EQUAL;
            case GREATER_THAN:
                return OperatorType.LESS_THAN;
            case GREATER_THAN_OR_EQUAL:
                return OperatorType.LESS_THAN_OR_EQUAL;
            case IS_DISTINCT_FROM:
                return OperatorType.IS_DISTINCT_FROM;
            default:
                throw new IllegalArgumentException("Unsupported comparison: " + operatorType);
        }
    }

    private static Result tryCreateSpatialJoin(
            Context context,
            JoinNode joinNode,
            RowExpression filter,
            PlanNodeId nodeId,
            List<VariableReferenceExpression> outputVariables,
            CallExpression spatialComparison,
            Metadata metadata,
            SplitManager splitManager,
            PageSourceManager pageSourceManager)
    {
        PlanNode leftNode = joinNode.getLeft();
        PlanNode rightNode = joinNode.getRight();

        List<VariableReferenceExpression> leftVariables = leftNode.getOutputVariables();
        List<VariableReferenceExpression> rightVariables = rightNode.getOutputVariables();
        FunctionResolution functionResolution = new FunctionResolution(metadata.getFunctionManager());
        StandardExpressions standardExpressions = new StandardExpressions(metadata.getFunctionManager());

        RowExpression radius;
        Optional<VariableReferenceExpression> newRadiusVariable;
        RowExpression newComparison;
        Optional<OperatorType> spatialComparisonType = functionResolution.getOperator(spatialComparison.getFunctionHandle());
        if (spatialComparisonType.isPresent() && (spatialComparisonType.get() == OperatorType.LESS_THAN || spatialComparisonType.get() == OperatorType.LESS_THAN_OR_EQUAL)) {
            // ST_Distance(a, b) <= r
            radius = getRight(spatialComparison);
            Set<VariableReferenceExpression> radiusVariables = VariablesExtractor.extractUnique(radius);
            if (radiusVariables.isEmpty() || (rightVariables.containsAll(radiusVariables) && containsNone(leftVariables, radiusVariables))) {
                checkArgument(DOUBLE.equals(radius.getType()), "radius must be double type");
                newRadiusVariable = newVariableIfNecessary(context, radius);
                newComparison = standardExpressions.compare(spatialComparisonType.get(), getLeft(spatialComparison), toRowExpression(newRadiusVariable, radius));
            }
            else {
                return Result.empty();
            }
        }
        else {
            // r >= ST_Distance(a, b)
            radius = getLeft(spatialComparison);
            Set<VariableReferenceExpression> radiusVariables = VariablesExtractor.extractUnique(radius);
            if (radiusVariables.isEmpty() || (rightVariables.containsAll(radiusVariables) && containsNone(leftVariables, radiusVariables))) {
                checkArgument(DOUBLE.equals(radius.getType()), "radius must be double type");
                newRadiusVariable = newVariableIfNecessary(context, radius);
                newComparison = standardExpressions.compare(flip(spatialComparisonType.get()), getRight(spatialComparison), toRowExpression(newRadiusVariable, radius));
            }
            else {
                return Result.empty();
            }
        }

        RowExpression newFilter = RowExpressionNodeInliner.replaceExpression(filter, ImmutableMap.of(spatialComparison, newComparison));
        PlanNode newRightNode = newRadiusVariable.map(variable -> addProjection(context, rightNode, variable, radius)).orElse(rightNode);

        JoinNode newJoinNode = new JoinNode(
                joinNode.getId(),
                joinNode.getType(),
                leftNode,
                newRightNode,
                joinNode.getCriteria(),
                joinNode.getOutputVariables(),
                Optional.of(newFilter),
                joinNode.getLeftHashVariable(),
                joinNode.getRightHashVariable(),
                joinNode.getDistributionType());

        return tryCreateSpatialJoin(context, newJoinNode, newFilter, nodeId, outputVariables, (CallExpression) getLeft(newComparison), Optional.of(getRight(newComparison)), metadata, splitManager, pageSourceManager);
    }

    private static Result tryCreateSpatialJoin(
            Context context,
            JoinNode joinNode,
            RowExpression filter,
            PlanNodeId nodeId,
            List<VariableReferenceExpression> outputVariables,
            CallExpression spatialFunction,
            Optional<RowExpression> radius,
            Metadata metadata,
            SplitManager splitManager,
            PageSourceManager pageSourceManager)
    {
        // TODO Add support for distributed left spatial joins
        Optional<String> spatialPartitioningTableName = joinNode.getType() == INNER ? getSpatialPartitioningTableName(context.getSession()) : Optional.empty();
        Optional<KdbTree> kdbTree = spatialPartitioningTableName.map(tableName -> loadKdbTree(tableName, context.getSession(), metadata, splitManager, pageSourceManager));

        List<RowExpression> arguments = spatialFunction.getArguments();
        verify(arguments.size() == 2);

        RowExpression firstArgument = arguments.get(0);
        RowExpression secondArgument = arguments.get(1);

        Type sphericalGeographyType = metadata.getType(SPHERICAL_GEOGRAPHY_TYPE_SIGNATURE);
        if (firstArgument.getType().equals(sphericalGeographyType)
                || secondArgument.getType().equals(sphericalGeographyType)) {
            return Result.empty();
        }

        Set<VariableReferenceExpression> firstVariables = VariablesExtractor.extractUnique(firstArgument);
        Set<VariableReferenceExpression> secondVariables = VariablesExtractor.extractUnique(secondArgument);

        if (firstVariables.isEmpty() || secondVariables.isEmpty()) {
            return Result.empty();
        }

        Type geometricType = metadata.getType(GEOMETRY_TYPE_SIGNATURE);
        checkArgument(firstArgument.getType().equals(geometricType), "firstArgument must be a Geometry type");
        checkArgument(secondArgument.getType().equals(geometricType), "firstArgument must be a Geometry type");
        Optional<VariableReferenceExpression> newFirstVariable = newVariableIfNecessary(context, firstArgument);
        Optional<VariableReferenceExpression> newSecondVariable = newVariableIfNecessary(context, secondArgument);

        PlanNode leftNode = joinNode.getLeft();
        PlanNode rightNode = joinNode.getRight();

        PlanNode newLeftNode;
        PlanNode newRightNode;

        // Check if the order of arguments of the spatial function matches the order of join sides
        int alignment = checkAlignment(joinNode, firstVariables, secondVariables);
        if (alignment > 0) {
            newLeftNode = newFirstVariable.map(variable -> addProjection(context, leftNode, variable, firstArgument)).orElse(leftNode);
            newRightNode = newSecondVariable.map(variable -> addProjection(context, rightNode, variable, secondArgument)).orElse(rightNode);
        }
        else if (alignment < 0) {
            newLeftNode = newSecondVariable.map(variable -> addProjection(context, leftNode, variable, secondArgument)).orElse(leftNode);
            newRightNode = newFirstVariable.map(variable -> addProjection(context, rightNode, variable, firstArgument)).orElse(rightNode);
        }
        else {
            return Result.empty();
        }

        RowExpression newFirstArgument = toRowExpression(newFirstVariable, firstArgument);
        RowExpression newSecondArgument = toRowExpression(newSecondVariable, secondArgument);

        Optional<VariableReferenceExpression> leftPartitionVariable = Optional.empty();
        Optional<VariableReferenceExpression> rightPartitionVariable = Optional.empty();
        if (kdbTree.isPresent()) {
            leftPartitionVariable = Optional.of(context.getVariableAllocator().newVariable("pid", INTEGER));
            rightPartitionVariable = Optional.of(context.getVariableAllocator().newVariable("pid", INTEGER));

            if (alignment > 0) {
                newLeftNode = addPartitioningNodes(context, metadata, newLeftNode, leftPartitionVariable.get(), kdbTree.get(), newFirstArgument, Optional.empty());
                newRightNode = addPartitioningNodes(context, metadata, newRightNode, rightPartitionVariable.get(), kdbTree.get(), newSecondArgument, radius);
            }
            else {
                newLeftNode = addPartitioningNodes(context, metadata, newLeftNode, leftPartitionVariable.get(), kdbTree.get(), newSecondArgument, Optional.empty());
                newRightNode = addPartitioningNodes(context, metadata, newRightNode, rightPartitionVariable.get(), kdbTree.get(), newFirstArgument, radius);
            }
        }

        RowExpression newSpatialFunction = call(spatialFunction.getDisplayName(), spatialFunction.getFunctionHandle(), spatialFunction.getType(), ImmutableList.of(newFirstArgument, newSecondArgument));
        RowExpression newFilter = RowExpressionNodeInliner.replaceExpression(filter, ImmutableMap.of(spatialFunction, newSpatialFunction));

        return Result.ofPlanNode(new SpatialJoinNode(
                nodeId,
                SpatialJoinNode.Type.fromJoinNodeType(joinNode.getType()),
                newLeftNode,
                newRightNode,
                outputVariables,
                newFilter,
                leftPartitionVariable,
                rightPartitionVariable,
                kdbTree.map(KdbTreeUtils::toJson)));
    }

    private static KdbTree loadKdbTree(String tableName, Session session, Metadata metadata, SplitManager splitManager, PageSourceManager pageSourceManager)
    {
        QualifiedObjectName name = toQualifiedObjectName(tableName, session.getCatalog().get(), session.getSchema().get());
        TableHandle tableHandle = metadata.getTableHandle(session, name)
                .orElseThrow(() -> new PrestoException(INVALID_SPATIAL_PARTITIONING, format("Table not found: %s", name)));
        Map<String, ColumnHandle> columnHandles = metadata.getColumnHandles(session, tableHandle);
        List<ColumnHandle> visibleColumnHandles = columnHandles.values().stream()
                .filter(handle -> !metadata.getColumnMetadata(session, tableHandle, handle).isHidden())
                .collect(toImmutableList());
        checkSpatialPartitioningTable(visibleColumnHandles.size() == 1, "Expected single column for table %s, but found %s columns", name, columnHandles.size());

        ColumnHandle kdbTreeColumn = Iterables.getOnlyElement(visibleColumnHandles);

        TableLayoutResult layout = metadata.getLayout(session, tableHandle, Constraint.alwaysTrue(), Optional.of(ImmutableSet.of(kdbTreeColumn)));
        TableHandle newTableHandle = layout.getLayout().getNewTableHandle();

        Optional<KdbTree> kdbTree = Optional.empty();
        try (SplitSource splitSource = splitManager.getSplits(session, newTableHandle, UNGROUPED_SCHEDULING)) {
            while (!Thread.currentThread().isInterrupted()) {
                SplitBatch splitBatch = getFutureValue(splitSource.getNextBatch(NOT_PARTITIONED, Lifespan.taskWide(), 1000));
                List<Split> splits = splitBatch.getSplits();

                for (Split split : splits) {
                    try (ConnectorPageSource pageSource = pageSourceManager.createPageSource(session, split, newTableHandle, ImmutableList.of(kdbTreeColumn))) {
                        do {
                            getFutureValue(pageSource.isBlocked());
                            Page page = pageSource.getNextPage();
                            if (page != null && page.getPositionCount() > 0) {
                                checkSpatialPartitioningTable(!kdbTree.isPresent(), "Expected exactly one row for table %s, but found more", name);
                                checkSpatialPartitioningTable(page.getPositionCount() == 1, "Expected exactly one row for table %s, but found %s rows", name, page.getPositionCount());
                                String kdbTreeJson = VARCHAR.getSlice(page.getBlock(0), 0).toStringUtf8();
                                try {
                                    kdbTree = Optional.of(KdbTreeUtils.fromJson(kdbTreeJson));
                                }
                                catch (IllegalArgumentException e) {
                                    checkSpatialPartitioningTable(false, "Invalid JSON string for KDB tree: %s", e.getMessage());
                                }
                            }
                        }
                        while (!pageSource.isFinished());
                    }
                    catch (IOException e) {
                        throw new UncheckedIOException(e);
                    }
                }

                if (splitBatch.isLastBatch()) {
                    break;
                }
            }
        }

        checkSpatialPartitioningTable(kdbTree.isPresent(), "Expected exactly one row for table %s, but got none", name);
        return kdbTree.get();
    }

    private static void checkSpatialPartitioningTable(boolean condition, String message, Object... arguments)
    {
        if (!condition) {
            throw new PrestoException(INVALID_SPATIAL_PARTITIONING, format(message, arguments));
        }
    }

    private static QualifiedObjectName toQualifiedObjectName(String name, String catalog, String schema)
    {
        ImmutableList<String> ids = ImmutableList.copyOf(Splitter.on('.').split(name));
        if (ids.size() == 3) {
            return new QualifiedObjectName(ids.get(0), ids.get(1), ids.get(2));
        }

        if (ids.size() == 2) {
            return new QualifiedObjectName(catalog, ids.get(0), ids.get(1));
        }

        if (ids.size() == 1) {
            return new QualifiedObjectName(catalog, schema, ids.get(0));
        }

        throw new PrestoException(INVALID_SPATIAL_PARTITIONING, format("Invalid name: %s", name));
    }

    private static int checkAlignment(JoinNode joinNode, Set<VariableReferenceExpression> maybeLeftVariables, Set<VariableReferenceExpression> maybeRightVariables)
    {
        List<VariableReferenceExpression> leftVariables = joinNode.getLeft().getOutputVariables();
        List<VariableReferenceExpression> rightVariables = joinNode.getRight().getOutputVariables();

        if (leftVariables.containsAll(maybeLeftVariables)
                && containsNone(leftVariables, maybeRightVariables)
                && rightVariables.containsAll(maybeRightVariables)
                && containsNone(rightVariables, maybeLeftVariables)) {
            return 1;
        }

        if (leftVariables.containsAll(maybeRightVariables)
                && containsNone(leftVariables, maybeLeftVariables)
                && rightVariables.containsAll(maybeLeftVariables)
                && containsNone(rightVariables, maybeRightVariables)) {
            return -1;
        }

        return 0;
    }

    private static RowExpression toRowExpression(Optional<VariableReferenceExpression> optionalVariable, RowExpression defaultExpression)
    {
        return optionalVariable.map(RowExpression.class::cast).orElse(defaultExpression);
    }

    private static Optional<VariableReferenceExpression> newVariableIfNecessary(Context context, RowExpression expression)
    {
        if (expression instanceof VariableReferenceExpression) {
            return Optional.empty();
        }
        return Optional.of(context.getVariableAllocator().newVariable(expression));
    }

    private static PlanNode addProjection(Context context, PlanNode node, VariableReferenceExpression variable, RowExpression expression)
    {
        Assignments.Builder projections = Assignments.builder();
        for (VariableReferenceExpression outputVariable : node.getOutputVariables()) {
            projections.put(identityAsSymbolReference(outputVariable));
        }

        projections.put(variable, expression);
        return new ProjectNode(context.getIdAllocator().getNextId(), node, projections.build());
    }

    private static PlanNode addPartitioningNodes(
            Context context,
            Metadata metadata,
            PlanNode node,
            VariableReferenceExpression partitionVariable,
            KdbTree kdbTree,
            RowExpression geometry,
            Optional<RowExpression> radius)
    {
        Assignments.Builder projections = Assignments.builder();
        for (VariableReferenceExpression outputVariable : node.getOutputVariables()) {
            projections.put(identityAsSymbolReference(outputVariable));
        }
        StandardExpressions standardExpressions = new StandardExpressions(metadata.getFunctionManager());

        ImmutableList.Builder<RowExpression> partitioningArguments = ImmutableList.<RowExpression>builder()
                .add(standardExpressions.cast(constant(Slices.utf8Slice(KdbTreeUtils.toJson(kdbTree)), VARCHAR), metadata.getType(TypeSignature.parseTypeSignature(KDB_TREE_TYPENAME))))
                .add(geometry);
        radius.map(partitioningArguments::add);

        CallExpression partitioningFunction = call(metadata.getFunctionManager(), "spatial_partitions", new ArrayType(INTEGER), partitioningArguments.build());
        VariableReferenceExpression partitionsVariable = context.getVariableAllocator().newVariable(partitioningFunction);
        projections.put(partitionsVariable, partitioningFunction);

        return new UnnestNode(
                context.getIdAllocator().getNextId(),
                new ProjectNode(context.getIdAllocator().getNextId(), node, projections.build()),
                node.getOutputVariables(),
                ImmutableMap.of(partitionsVariable, ImmutableList.of(partitionVariable)),
                Optional.empty());
    }

    private static boolean containsNone(Collection<VariableReferenceExpression> values, Collection<VariableReferenceExpression> testValues)
    {
        return values.stream().noneMatch(ImmutableSet.copyOf(testValues)::contains);
    }
}
