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

package com.facebook.presto.sql;

import com.facebook.presto.Session;
import com.facebook.presto.common.predicate.NullableValue;
import com.facebook.presto.common.predicate.TupleDomain;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.metadata.SessionPropertyManager;
import com.facebook.presto.spi.MaterializedViewStatus;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.relation.DomainTranslator;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.spi.security.Identity;
import com.facebook.presto.sql.analyzer.SemanticException;
import com.facebook.presto.sql.planner.LiteralEncoder;
import com.facebook.presto.sql.tree.ArithmeticBinaryExpression;
import com.facebook.presto.sql.tree.BooleanLiteral;
import com.facebook.presto.sql.tree.Cast;
import com.facebook.presto.sql.tree.ComparisonExpression;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.FunctionCall;
import com.facebook.presto.sql.tree.Identifier;
import com.facebook.presto.sql.tree.IsNullPredicate;
import com.facebook.presto.sql.tree.LogicalBinaryExpression;
import com.facebook.presto.sql.tree.QualifiedName;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.Stack;

import static com.facebook.presto.common.predicate.TupleDomain.extractFixedValues;
import static com.facebook.presto.common.type.StandardTypes.HYPER_LOG_LOG;
import static com.facebook.presto.common.type.StandardTypes.VARBINARY;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.NOT_SUPPORTED;
import static com.facebook.presto.sql.tree.ArithmeticBinaryExpression.Operator.DIVIDE;
import static com.facebook.presto.sql.tree.BooleanLiteral.FALSE_LITERAL;
import static com.facebook.presto.sql.tree.ComparisonExpression.Operator.EQUAL;
import static com.facebook.presto.sql.tree.LogicalBinaryExpression.Operator.AND;
import static com.facebook.presto.sql.tree.LogicalBinaryExpression.Operator.OR;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;

public final class MaterializedViewUtils
{
    public static final QualifiedName MIN = QualifiedName.of("MIN");
    public static final QualifiedName MAX = QualifiedName.of("MAX");
    public static final QualifiedName SUM = QualifiedName.of("SUM");
    public static final QualifiedName COUNT = QualifiedName.of("COUNT");
    public static final QualifiedName AVG = QualifiedName.of("AVG");
    public static final QualifiedName CARDINALITY = QualifiedName.of("CARDINALITY");
    public static final QualifiedName MERGE = QualifiedName.of("MERGE");
    public static final QualifiedName APPROX_DISTINCT = QualifiedName.of("APPROX_DISTINCT");
    public static final QualifiedName APPROX_SET = QualifiedName.of("APPROX_SET");

    public static final Set<QualifiedName> ASSOCIATIVE_REWRITE_FUNCTIONS = ImmutableSet.of(MIN, MAX, SUM, COUNT);

    // TODO: Add count to NonAssociativeFunctionHandler, add more functions
    public static final Map<QualifiedName, MaterializedViewUtils.NonAssociativeFunctionHandler> NON_ASSOCIATIVE_REWRITE_FUNCTIONS = ImmutableMap.of(
            AVG, new MaterializedViewUtils.AvgRewriteHandler(),
            APPROX_DISTINCT, new MaterializedViewUtils.ApproxDistinctRewriteHandler());

    public static final Set<QualifiedName> SUPPORTED_FUNCTION_CALLS = Sets.union(ASSOCIATIVE_REWRITE_FUNCTIONS, NON_ASSOCIATIVE_REWRITE_FUNCTIONS.keySet());

    private MaterializedViewUtils() {}

    public static Session buildOwnerSession(Session session, Optional<String> owner, SessionPropertyManager sessionPropertyManager, String catalog, String schema)
    {
        Identity identity = getOwnerIdentity(owner, session);

        return Session.builder(sessionPropertyManager)
                .setQueryId(session.getQueryId())
                .setTransactionId(session.getTransactionId().orElse(null))
                .setIdentity(identity)
                .setSource(session.getSource().orElse(null))
                .setCatalog(catalog)
                .setSchema(schema)
                .setTimeZoneKey(session.getTimeZoneKey())
                .setLocale(session.getLocale())
                .setRemoteUserAddress(session.getRemoteUserAddress().orElse(null))
                .setUserAgent(session.getUserAgent().orElse(null))
                .setClientInfo(session.getClientInfo().orElse(null))
                .setStartTime(session.getStartTime())
                .build();
    }

    public static Identity getOwnerIdentity(Optional<String> owner, Session session)
    {
        if (owner.isPresent() && !owner.get().equals(session.getIdentity().getUser())) {
            return new Identity(owner.get(), Optional.empty());
        }
        return session.getIdentity();
    }

    public static Map<SchemaTableName, Expression> generateBaseTablePredicates(Map<SchemaTableName, MaterializedViewStatus.MaterializedDataPredicates> predicatesFromBaseTables, Metadata metadata)
    {
        Map<SchemaTableName, Expression> baseTablePredicates = new HashMap<>();

        for (SchemaTableName baseTable : predicatesFromBaseTables.keySet()) {
            MaterializedViewStatus.MaterializedDataPredicates predicatesInfo = predicatesFromBaseTables.get(baseTable);
            List<String> partitionKeys = predicatesInfo.getColumnNames();
            ImmutableList<Expression> keyExpressions = partitionKeys.stream().map(Identifier::new).collect(toImmutableList());
            List<TupleDomain<String>> predicateDisjuncts = predicatesInfo.getPredicateDisjuncts();
            Expression disjunct = null;

            for (TupleDomain<String> predicateDisjunct : predicateDisjuncts) {
                Expression conjunct = null;
                Iterator<Expression> keyExpressionsIterator = keyExpressions.stream().iterator();
                Map<String, NullableValue> predicateKeyValue = extractFixedValues(predicateDisjunct)
                        .orElseThrow(() -> new IllegalStateException("predicateKeyValue is not present!"));

                for (String key : partitionKeys) {
                    NullableValue nullableValue = predicateKeyValue.get(key);
                    Expression expression;

                    if (nullableValue.isNull()) {
                        expression = new IsNullPredicate(keyExpressionsIterator.next());
                    }
                    else {
                        LiteralEncoder literalEncoder = new LiteralEncoder(metadata.getBlockEncodingSerde());
                        Expression valueExpression = literalEncoder.toExpression(nullableValue.getValue(), nullableValue.getType(), false);
                        expression = new ComparisonExpression(EQUAL, keyExpressionsIterator.next(), valueExpression);
                    }

                    conjunct = conjunct == null ? expression : new LogicalBinaryExpression(AND, conjunct, expression);
                }

                disjunct = conjunct == null ? disjunct : disjunct == null ? conjunct : new LogicalBinaryExpression(OR, disjunct, conjunct);
            }
            // If no (fresh) partitions are found for table, that means we should not select from it
            if (disjunct == null) {
                disjunct = FALSE_LITERAL;
            }
            baseTablePredicates.put(baseTable, disjunct);
        }

        return baseTablePredicates;
    }

    public static Map<SchemaTableName, Expression> generateFalsePredicates(List<SchemaTableName> baseTables)
    {
        return baseTables.stream().collect(toImmutableMap(table -> table, table -> BooleanLiteral.FALSE_LITERAL));
    }

    // Returns transitive closure of a graph by performing depth first search from each node
    public static <T> Map<T, Set<T>> transitiveClosure(Map<T, Set<T>> graph)
    {
        ImmutableMap.Builder<T, Set<T>> closure = ImmutableMap.builder();
        for (T node : graph.keySet()) {
            // Do depth first search for each node to find its connected component
            Set<T> visited = new HashSet<>();
            Stack<T> stack = new Stack<>();
            stack.push(node);
            while (!stack.empty()) {
                T current = stack.pop();
                if (visited.contains(current)) {
                    continue;
                }
                visited.add(current);
                graph.getOrDefault(current, ImmutableSet.of()).forEach(neighbor -> stack.push(neighbor));
            }
            closure.put(node, visited);
        }
        return closure.build();
    }

    public static TupleDomain<String> getDomainFromFilter(Session session, DomainTranslator domainTranslator, RowExpression rowExpression)
    {
        DomainTranslator.ExtractionResult<String> predicateFromBaseQuery = domainTranslator.fromPredicate(
                session.toConnectorSession(),
                rowExpression,
                (baseFilterExpression, domain) -> baseFilterExpression instanceof VariableReferenceExpression
                        ? Optional.of(((VariableReferenceExpression) baseFilterExpression).getName())
                        : Optional.empty());

        return predicateFromBaseQuery.getTupleDomain();
    }

    /**
     * Rewrites the provided argument if it is non-associative (e.g. it cannot be handled by simply re-applying function to derived columns)
     */
    public static Expression rewriteNonAssociativeFunction(FunctionCall functionCall, Map<Expression, Identifier> baseToViewColumnMap)
    {
        if (!validateNonAssociativeFunctionCallFields(functionCall)) {
            throw new SemanticException(NOT_SUPPORTED, functionCall, "Nonassociative function call rewrite not supported function calls with fields other than name and arguments");
        }

        QualifiedName functionName = functionCall.getName();
        List<Expression> expressions = functionCall.getArguments();

        if (!validateNonAssociativeFunctionCallArguments(functionName, expressions)) {
            throw new SemanticException(NOT_SUPPORTED, functionCall, "Nonassociative function call rewrite not supported without single identifier as argument");
        }

        Identifier baseTableColumn = (Identifier) expressions.get(0);

        return NON_ASSOCIATIVE_REWRITE_FUNCTIONS.get(functionName).rewrite(baseTableColumn, baseToViewColumnMap);
    }

    public static boolean validateNonAssociativeFunctionRewrite(FunctionCall functionCall, Map<Expression, Identifier> baseToViewColumnMap)
    {
        QualifiedName functionName = functionCall.getName();
        List<Expression> expressions = functionCall.getArguments();

        return validateNonAssociativeFunctionCallFields(functionCall)
                && validateNonAssociativeFunctionCallArguments(functionName, expressions)
                && NON_ASSOCIATIVE_REWRITE_FUNCTIONS.get(functionName).validate((Identifier) functionCall.getArguments().get(0), baseToViewColumnMap);
    }

    private static boolean validateNonAssociativeFunctionCallFields(FunctionCall functionCall)
    {
        return !(functionCall.isDistinct() || functionCall.getWindow().isPresent() || functionCall.getOrderBy().isPresent()
                || functionCall.isIgnoreNulls() || functionCall.getFilter().isPresent());
    }

    private static boolean validateNonAssociativeFunctionCallArguments(QualifiedName functionName, List<Expression> expressions)
    {
        return NON_ASSOCIATIVE_REWRITE_FUNCTIONS.containsKey(functionName) && expressions.size() == 1 && expressions.get(0) instanceof Identifier;
    }

    /**
     * Handles rewrites for functions which are non-associative, e.g. cannot be simply re-written like sum(sum_result).
     * Uses base table column name as well as a map of base to view columns to return a rewritten Expression, or
     * Optional.empty() if rewrite is not possible.
     */
    private interface NonAssociativeFunctionHandler
    {
        Expression rewrite(Identifier baseTableColumn, Map<Expression, Identifier> baseToViewColumnMap);

        boolean validate(Identifier baseTableColumn, Map<Expression, Identifier> baseToViewColumnMap);
    }

    private static class AvgRewriteHandler
            implements NonAssociativeFunctionHandler
    {
        @Override
        public Expression rewrite(Identifier baseTableColumn, Map<Expression, Identifier> baseToViewColumnMap)
        {
            Optional<Identifier> sumDerived = getDerivedFromFunctionCall(SUM, baseTableColumn, baseToViewColumnMap);
            Optional<Identifier> countDerived = getDerivedFromFunctionCall(COUNT, baseTableColumn, baseToViewColumnMap);

            if (!sumDerived.isPresent() || !countDerived.isPresent()) {
                throw new SemanticException(NOT_SUPPORTED, baseTableColumn, "Avg rewrite not supported without COUNT and SUM");
            }

            // Rewrite avg as sum(sum_result) / sum(count_result)
            FunctionCall sum = new FunctionCall(SUM, ImmutableList.of(sumDerived.get()));
            FunctionCall count = new FunctionCall(SUM, ImmutableList.of(countDerived.get()));
            return new ArithmeticBinaryExpression(DIVIDE, sum, count);
        }

        @Override
        public boolean validate(Identifier baseTableColumn, Map<Expression, Identifier> baseToViewColumnMap)
        {
            Optional<Identifier> sumDerived = getDerivedFromFunctionCall(SUM, baseTableColumn, baseToViewColumnMap);
            Optional<Identifier> countDerived = getDerivedFromFunctionCall(COUNT, baseTableColumn, baseToViewColumnMap);

            return sumDerived.isPresent() && countDerived.isPresent();
        }

        /**
         * Gets the column from the materialized view derived from provided function name and argument
         */
        private Optional<Identifier> getDerivedFromFunctionCall(QualifiedName functionName, Identifier baseTableColumn, Map<Expression, Identifier> baseToViewColumnMap)
        {
            return Optional.ofNullable(baseToViewColumnMap.get(new FunctionCall(functionName, ImmutableList.of(baseTableColumn))));
        }
    }

    private static class ApproxDistinctRewriteHandler
            implements NonAssociativeFunctionHandler
    {
        @Override
        public Expression rewrite(Identifier baseTableColumn, Map<Expression, Identifier> baseToViewColumnMap)
        {
            Cast cast = new Cast(new FunctionCall(APPROX_SET, ImmutableList.of(baseTableColumn)), VARBINARY);

            if (!baseToViewColumnMap.containsKey(cast)) {
                throw new SemanticException(NOT_SUPPORTED, baseTableColumn, "APPROX_DISTINCT rewrite not supported without approx set in base to view column map");
            }

            Identifier identifier = baseToViewColumnMap.get(cast);
            Cast varbinaryToHll = new Cast(identifier, HYPER_LOG_LOG);

            FunctionCall mergedHll = new FunctionCall(MERGE, ImmutableList.of(varbinaryToHll));
            return new FunctionCall(CARDINALITY, ImmutableList.of(mergedHll));
        }

        @Override
        public boolean validate(Identifier baseTableColumn, Map<Expression, Identifier> baseToViewColumnMap)
        {
            return baseToViewColumnMap.containsKey(new Cast(new FunctionCall(APPROX_SET, ImmutableList.of(baseTableColumn)), VARBINARY));
        }
    }
}
