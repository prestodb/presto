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
package com.facebook.presto.pinot;

import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.predicate.Domain;
import com.facebook.presto.spi.predicate.Marker;
import com.facebook.presto.spi.predicate.Range;
import com.facebook.presto.spi.type.VarcharType;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import io.airlift.log.Logger;
import io.airlift.slice.Slice;
import org.apache.pinot.common.utils.CommonConstants;

import java.util.ArrayList;
import java.util.List;
import java.util.StringJoiner;
import java.util.stream.Stream;

import static com.facebook.presto.pinot.PinotUtils.QUOTE;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.joining;

/**
 * This class manages how to generate the query to send to Pinot servers.
 */
public final class PinotQueryBuilder
{
    private static final Logger log = Logger.get(PinotQueryBuilder.class);

    private PinotQueryBuilder()
    {
    }

    /**
     * QUERY_TEMPLATE looks like this:
     * SELECT $fields FROM $tableName $predicate LIMIT $limit.
     * <p>
     * Note $predicate is optional, and we intentionally add a space between $tableName $predicate for readability.
     * When $predicate is absent, there would be 2 spaces between $tableName and LIMIT, which is should not hurt the query itself.
     */
    private static final String QUERY_TEMPLATE = "SELECT %s FROM %s %s LIMIT %d";

    /**
     * Returns the Pinot Query to send for each split.
     *
     * <p>Pinot Query would be constructed based on {$link #QUERY_TEMPLATE} and predicates (WHERE ...).
     *
     * @return the constructed Pinot Query
     */
    static String getPinotQuery(PinotConfig pinotConfig, List<PinotColumnHandle> columnHandles, String pinotFilter, String timeFilter, String tableName, long splitLimit)
    {
        requireNonNull(pinotConfig, "pinotConfig is null");
        StringJoiner fieldsJoiner = new StringJoiner(", ");
        for (PinotColumnHandle columnHandle : columnHandles) {
            // No aggregation pushdown
            fieldsJoiner.add(columnHandle.getColumnName());
        }

        // Add predicates
        StringJoiner predicatesJoiner = new StringJoiner(" AND ");
        if (!pinotFilter.isEmpty()) {
            predicatesJoiner.add(String.format("(%s)", pinotFilter));
        }
        if (!timeFilter.isEmpty()) {
            predicatesJoiner.add(String.format("(%s)", timeFilter));
        }

        // Note pinotPredicate is optional. It would be empty when no predicates are pushed down.
        String pinotPredicate = "";
        if (predicatesJoiner.length() > 0) {
            pinotPredicate = "WHERE " + predicatesJoiner.toString();
        }

        long limit = splitLimit > 0 ? splitLimit : pinotConfig.getLimitAll();

        String finalQuery = String.format(QUERY_TEMPLATE, fieldsJoiner.toString(), tableName, pinotPredicate, limit);
        log.debug("Plan to send PQL : %s", finalQuery);
        return finalQuery;
    }

    /**
     * Get the predicates for a column in string format, for constructing Pinot queries directly
     *
     * @param domain TupleDomain representing the allowed ranges for a column
     * @param columnName Pinot column name
     * @return Predicate in Pinot Query Language for the column. Empty string would be returned if no constraints
     */
    @VisibleForTesting
    static String getColumnPredicate(Domain domain, String columnName)
    {
        List<String> discreteConstraintList = new ArrayList<>();
        List<String> singleValueRangeConstraintList = new ArrayList<>();
        List<String> rangeConstraintList = new ArrayList<>();

        return domain.getValues().getValuesProcessor().transform(
                ranges ->
                {
                    for (Range range : ranges.getOrderedRanges()) {
                        if (range.isSingleValue()) {
                            singleValueRangeConstraintList.add(getMarkerValue(range.getLow()));
                        }
                        else {
                            StringBuilder builder = new StringBuilder();
                            ImmutableList.Builder<String> bounds = ImmutableList.builder();
                            // Get low bound
                            String equationMark = (range.getLow().getBound() == Marker.Bound.EXACTLY) ? "= " : " ";
                            if (!range.getLow().isLowerUnbounded()) {
                                bounds.add(getMarkerValue(range.getLow()) + " <" + equationMark + columnName);
                            }
                            // Get high bound
                            equationMark = (range.getHigh().getBound() == Marker.Bound.EXACTLY) ? "= " : " ";
                            if (!range.getHigh().isUpperUnbounded()) {
                                bounds.add(columnName + " <" + equationMark + getMarkerValue(range.getHigh()));
                            }
                            // Use AND to combine bounds within the same range
                            builder.append("(").append(Joiner.on(" AND ").join(bounds.build())).append(")");
                            rangeConstraintList.add(builder.toString());
                        }
                    }
                    // Multiple ranges on the same column are OR'ed together.
                    String rangeConstraint = Joiner.on(" OR ").join(rangeConstraintList);
                    String discreteConstraint = getDiscretePredicate(true, columnName, singleValueRangeConstraintList);

                    return Stream.of(rangeConstraint, discreteConstraint)
                            .filter(s -> !s.isEmpty())
                            .collect(joining(" OR "));
                },
                discreteValues ->
                {
                    /*
                     * For some types like {@link com.facebook.presto.type.ColorType} that are not orderable, discreteValues would appear here.
                     * For most regular types like boolean, char, number, the discrete values would be converted to singleValues in ranges above,
                     * and would not appear here. So far the column types supported by Pinot all fall in that category.
                     */
                    discreteConstraintList.addAll(discreteValues.getValues().stream().map(Object::toString).collect(toImmutableList()));
                    return getDiscretePredicate(discreteValues.isWhiteList(), columnName, discreteConstraintList);
                },
                allOrNone ->
                {
                    // no-op
                    return "";
                });
    }

    /**
     * Construct the IN predicate for discrete values
     *
     * @param isWhitelist true for IN predicate, false for NOT IN predicate
     * @param columnName name of the column
     * @param discreteConstraintList list of allowed or not allowed values
     * @return Stringified clause with IN or NOT IN
     */
    static String getDiscretePredicate(boolean isWhitelist, String columnName, List<String> discreteConstraintList)
    {
        if (discreteConstraintList.size() == 0) {
            return "";
        }
        return columnName + (isWhitelist ? " " : " NOT ") + "IN (" + Joiner.on(',').join(discreteConstraintList) + ")";
    }

    /**
     * Get the value for the Marker.
     *
     * @param marker marker in the Domain
     * @return Underlying value for the block in the marker. For string, encapsulating quotes will be added.
     */
    private static String getMarkerValue(Marker marker)
    {
        if (marker.getType() instanceof VarcharType) {
            Block highBlock = marker.getValueBlock().get();
            Slice slice = highBlock.getSlice(0, 0, highBlock.getSliceLength(0));
            return QUOTE + slice.toStringUtf8() + QUOTE;
        }
        return marker.getValue().toString();
    }

    static String getTimePredicate(String tableType, String timeColumn, String maxTimeStamp)
    {
        if (CommonConstants.Helix.TableType.OFFLINE.toString().equalsIgnoreCase(tableType)) {
            return String.format("%s < %s", timeColumn, maxTimeStamp);
        }
        if (CommonConstants.Helix.TableType.REALTIME.toString().equalsIgnoreCase(tableType)) {
            return String.format("%s >= %s", timeColumn, maxTimeStamp);
        }
        return null;
    }
}
