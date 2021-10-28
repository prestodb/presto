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
import com.facebook.presto.sql.analyzer.Analysis;
import com.facebook.presto.sql.analyzer.MaterializedViewColumnMappingExtractor;
import com.facebook.presto.sql.planner.LiteralEncoder;
import com.facebook.presto.sql.tree.BooleanLiteral;
import com.facebook.presto.sql.tree.ComparisonExpression;
import com.facebook.presto.sql.tree.CreateMaterializedView;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.Identifier;
import com.facebook.presto.sql.tree.IsNullPredicate;
import com.facebook.presto.sql.tree.LogicalBinaryExpression;
import com.facebook.presto.sql.tree.Node;
import com.facebook.presto.sql.tree.Query;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.facebook.presto.common.predicate.TupleDomain.extractFixedValues;
import static com.facebook.presto.metadata.MetadataUtil.toSchemaTableName;
import static com.facebook.presto.spi.ConnectorMaterializedViewDefinition.TableColumn;
import static com.facebook.presto.sql.tree.BooleanLiteral.FALSE_LITERAL;
import static com.facebook.presto.sql.tree.ComparisonExpression.Operator.EQUAL;
import static com.facebook.presto.sql.tree.LogicalBinaryExpression.Operator.AND;
import static com.facebook.presto.sql.tree.LogicalBinaryExpression.Operator.OR;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;

public class MaterializedViewUtils
{
    private MaterializedViewUtils() {}

    /**
     * Compute the 1-to-N column mapping from a materialized view to its base tables.
     * <p>
     * From {@code analysis}, we could derive only one base table column that one materialized view column maps to.
     * In case of 1 materialized view defined on N base tables via join, union, etc, this method helps compute
     * all the N base table columns that one materialized view column maps to.
     * It calls on {@link MaterializedViewColumnMappingExtractor} to get all base table columns mapped by join, union, etc,
     * and then uses them to expand the 1-to-1 column mapping derived from {@code analysis} to the 1-to-N column mapping.
     * <p>
     * For example, given SELECT column_a AS column_x FROM table_a JOIN table_b ON (table_a.column_a = table_b.column_b),
     * the 1-to-1 column mapping from {@code analysis} is column_x -> table_a.column_a. Mapped base table columns are
     * [table_a.column_a, table_b.column_b]. Then it will return a 1-to-N column mapping column_x -> {table_a -> column_a, table_b -> column_b}.
     */
    public static Map<String, Map<SchemaTableName, String>> computeMaterializedViewToBaseTableColumnMappings(Analysis analysis)
    {
        checkState(
                analysis.getStatement() instanceof CreateMaterializedView,
                "Only support the computation of column mappings when analyzing CreateMaterializedView");

        ImmutableMap.Builder<String, Map<SchemaTableName, String>> fullColumnMappings = ImmutableMap.builder();

        Query viewQuery = ((CreateMaterializedView) analysis.getStatement()).getQuery();
        Map<String, TableColumn> originalColumnMappings = getOriginalColumnsFromAnalysis(viewQuery, analysis);

        List<List<TableColumn>> mappedBaseColumns = MaterializedViewColumnMappingExtractor.extractMappedBaseColumns(analysis);

        for (Map.Entry<String, TableColumn> columnMapping : originalColumnMappings.entrySet()) {
            String viewColumn = columnMapping.getKey();
            TableColumn originalBaseColumn = columnMapping.getValue();

            Map<SchemaTableName, String> fullBaseColumns = new HashMap<>();

            mappedBaseColumns.forEach(mappedBaseColumnPair -> {
                if (originalBaseColumn.equals(mappedBaseColumnPair.get(0))) {
                    fullBaseColumns.put(mappedBaseColumnPair.get(1).getTableName(), mappedBaseColumnPair.get(1).getColumnName());
                }
                else if (originalBaseColumn.equals(mappedBaseColumnPair.get(1))) {
                    fullBaseColumns.put(mappedBaseColumnPair.get(0).getTableName(), mappedBaseColumnPair.get(0).getColumnName());
                }
            });

            fullBaseColumns.put(originalBaseColumn.getTableName(), originalBaseColumn.getColumnName());

            fullColumnMappings.put(viewColumn, ImmutableMap.copyOf(fullBaseColumns));
        }

        return fullColumnMappings.build();
    }

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

    private static Map<String, TableColumn> getOriginalColumnsFromAnalysis(Node viewQuery, Analysis analysis)
    {
        return analysis.getOutputDescriptor(viewQuery).getVisibleFields().stream()
                .filter(field -> field.getOriginTable().isPresent() && field.getOriginColumnName().isPresent())
                .collect(toImmutableMap(
                        field -> field.getName().get(),
                        field -> new TableColumn(toSchemaTableName(field.getOriginTable().get()), field.getOriginColumnName().get())));
    }
}
