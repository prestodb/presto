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
package com.facebook.presto.hive.metastore.glue;

import com.facebook.presto.common.predicate.Domain;
import com.facebook.presto.common.predicate.Range;
import com.facebook.presto.common.predicate.ValueSet;
import com.facebook.presto.common.type.DateType;
import com.facebook.presto.common.type.TimestampType;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.hive.metastore.Column;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableSet;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.facebook.presto.hive.metastore.MetastoreUtil.HIVE_DEFAULT_DYNAMIC_PARTITION;
import static com.facebook.presto.hive.metastore.MetastoreUtil.convertRawValueToString;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.Iterators.getOnlyElement;
import static java.lang.String.format;
import static java.util.Locale.ENGLISH;

public final class GlueExpressionUtil
{
    static final int GLUE_EXPRESSION_CHAR_LIMIT = 2048;
    private static final Set<String> QUOTED_TYPES = ImmutableSet.of("string", "char", "varchar", "date", "timestamp", "binary", "varbinary");
    private static final String CONJUNCT_SEPARATOR = " AND ";
    private static final Joiner CONJUNCT_JOINER = Joiner.on(CONJUNCT_SEPARATOR);
    private static final String DISJUNCT_SEPARATOR = " OR ";
    private static final Joiner DISJUNCT_JOINER = Joiner.on(DISJUNCT_SEPARATOR);

    /**
     * AWS Glue uses internally <a href="http://jsqlparser.sourceforge.net/home.php">JSQLParser</a>
     * as SQL statement parser for the expression that can be used to filter the partitions.
     * JSQLParser defines a set of reserved keywords which cannot be used as column names in the filter.
     *
     * @see <a href="https://sourceforge.net/p/jsqlparser/code/HEAD/tree/trunk/src/main/javacc/JSqlParserCC.jj">JSqlParser Grammar</a>
     */
    private static final Set<String> JSQL_PARSER_RESERVED_KEYWORDS = ImmutableSet.of(
            "AS", "BY", "DO", "IS", "IN", "OR", "ON", "ALL", "AND", "ANY", "KEY", "NOT", "SET", "ASC", "TOP", "END", "DESC", "INTO", "NULL", "LIKE", "DROP", "JOIN",
            "LEFT", "FROM", "OPEN", "CASE", "WHEN", "THEN", "ELSE", "SOME", "FULL", "WITH", "TABLE", "WHERE", "USING", "UNION", "GROUP", "BEGIN", "INDEX", "INNER",
            "LIMIT", "OUTER", "ORDER", "RIGHT", "DELETE", "CREATE", "SELECT", "OFFSET", "EXISTS", "HAVING", "INSERT", "UPDATE", "VALUES", "ESCAPE", "PRIMARY",
            "NATURAL", "REPLACE", "BETWEEN", "TRUNCATE", "DISTINCT", "INTERSECT");

    private GlueExpressionUtil() {}

    /**
     * @return a valid glue expression <= {@link  GlueExpressionUtil#GLUE_EXPRESSION_CHAR_LIMIT}. A return value of "" means, for all columns, a valid glue expression could not be created, or
     * {@link Domain#all(Type)} was passed in as an argument
     */
    public static String buildGlueExpression(Map<Column, Domain> partitionPredicates)
    {
        List<String> perColumnExpressions = new ArrayList<>();
        int expressionLength = 0;
        for (Map.Entry<Column, Domain> partitionPredicate : partitionPredicates.entrySet()) {
            String columnName = partitionPredicate.getKey().getName();
            if (JSQL_PARSER_RESERVED_KEYWORDS.contains(columnName.toUpperCase(ENGLISH))) {
                // The column name is a reserved keyword in the grammar of the SQL parser used internally by Glue API
                continue;
            }
            Domain domain = partitionPredicate.getValue();
            if (domain != null && !domain.isAll()) {
                Optional<String> columnExpression = buildGlueExpressionForSingleDomain(columnName, domain);
                if (columnExpression.isPresent()) {
                    int newExpressionLength = expressionLength + columnExpression.get().length();
                    if (expressionLength > 0) {
                        newExpressionLength += CONJUNCT_SEPARATOR.length();
                    }

                    if (newExpressionLength > GLUE_EXPRESSION_CHAR_LIMIT) {
                        continue;
                    }

                    perColumnExpressions.add((columnExpression.get()));
                    expressionLength = newExpressionLength;
                }
            }
        }

        return Joiner.on(CONJUNCT_SEPARATOR).join(perColumnExpressions);
    }

    /**
     * Converts domain to a valid glue expression per
     * https://docs.aws.amazon.com/glue/latest/dg/aws-glue-api-catalog-partitions.html#aws-glue-api-catalog-partitions-GetPartitions
     *
     * @return optional glue-compatible expression. if empty, type of the {@link Domain} cannot be converted to a string
     */
    @VisibleForTesting
    static Optional<String> buildGlueExpressionForSingleDomain(String columnName, Domain domain)
    {
        checkState(!domain.isAll());
        ValueSet valueSet = domain.getValues();

        if (!canConvertSqlTypeToStringForGlue(domain.getType())) {
            return Optional.empty();
        }

        if (domain.getValues().isAll()) {
            return Optional.of(format("(%s <> '%s')", columnName, HIVE_DEFAULT_DYNAMIC_PARTITION));
        }

        // null must be allowed for this case since callers must filter Domain.none() out
        if (domain.getValues().isNone()) {
            return Optional.of(format("(%s = '%s')", columnName, HIVE_DEFAULT_DYNAMIC_PARTITION));
        }

        List<String> disjuncts = new ArrayList<>();
        List<String> singleValues = new ArrayList<>();
        for (Range range : valueSet.getRanges().getOrderedRanges()) {
            checkState(!range.isAll()); // Already checked
            if (range.isSingleValue()) {
                singleValues.add(valueToString(range.getSingleValue(), range.getType()));
            }
            else {
                List<String> rangeConjuncts = new ArrayList<>();
                if (!range.isLowUnbounded()) {
                    rangeConjuncts.add(format(
                            "%s %s %s",
                            columnName,
                            range.isLowInclusive() ? ">=" : ">",
                            valueToString(range.getLowBoundedValue(), range.getType())));
                }
                if (!range.isHighUnbounded()) {
                    rangeConjuncts.add(format(
                            "%s %s %s",
                            columnName,
                            range.isHighInclusive() ? "<=" : "<",
                            valueToString(range.getHighBoundedValue(), range.getType())));
                }
                // If rangeConjuncts is null, then the range was ALL, which should already have been checked for by callers
                checkState(!rangeConjuncts.isEmpty());
                disjuncts.add("(" + CONJUNCT_JOINER.join(rangeConjuncts) + ")");
            }
        }

        if (singleValues.size() == 1) {
            String equalsTest = format("(%s = %s)", columnName, getOnlyElement(singleValues.listIterator()));
            disjuncts.add(equalsTest);
        }
        else if (singleValues.size() > 1) {
            String values = Joiner.on(", ").join(singleValues);
            String inClause = format("(%s in (%s))", columnName, values);

            disjuncts.add(inClause);
        }

        return Optional.of("(" + DISJUNCT_JOINER.join(disjuncts) + ")");
    }

    private static boolean canConvertSqlTypeToStringForGlue(Type type)
    {
        return !(type instanceof TimestampType) && !(type instanceof DateType);
    }

    private static String valueToString(Object value, Type type)
    {
        String s = convertRawValueToString(value, type);

        return isQuotedType(type) ? "'" + s + "'" : s;
    }

    private static boolean isQuotedType(Type type)
    {
        return QUOTED_TYPES.contains(type.getTypeSignature().getBase());
    }
}
