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
import com.facebook.presto.spi.security.Identity;
import com.facebook.presto.sql.planner.LiteralEncoder;
import com.facebook.presto.sql.tree.BooleanLiteral;
import com.facebook.presto.sql.tree.ComparisonExpression;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.Identifier;
import com.facebook.presto.sql.tree.IsNullPredicate;
import com.facebook.presto.sql.tree.LogicalBinaryExpression;
import com.facebook.presto.sql.tree.QualifiedName;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.Stack;

import static com.facebook.presto.common.predicate.TupleDomain.extractFixedValues;
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
    public static final Set<QualifiedName> SUPPORTED_FUNCTION_CALLS = ImmutableSet.of(MIN, MAX, SUM, COUNT);

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
}
