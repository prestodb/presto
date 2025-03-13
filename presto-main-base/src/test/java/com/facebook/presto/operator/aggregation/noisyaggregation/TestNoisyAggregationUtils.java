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
package com.facebook.presto.operator.aggregation.noisyaggregation;

import com.facebook.presto.common.type.StandardTypes;

import java.util.ArrayList;
import java.util.List;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

import static com.facebook.presto.common.type.Decimals.MAX_PRECISION;

public class TestNoisyAggregationUtils
{
    public static final BiFunction<Object, Object, Boolean> notEqualDoubleAssertion = (actual, expected) -> !new Double(actual.toString()).equals(new Double(expected.toString()));

    public static final BiFunction<Object, Object, Boolean> equalDoubleAssertion =
            (actual, expected) -> Math.abs(new Double(actual.toString()) - new Double(expected.toString())) <= 1e-12;

    public static final BiFunction<Object, Object, Boolean> equalLongAssertion = (actual, expected) -> new Long(actual.toString()).equals(new Long(expected.toString()));

    public static final double DEFAULT_TEST_STANDARD_DEVIATION = 1.0;

    public static final BiFunction<Object, Object, Boolean> withinSomeStdAssertion = (actual, expected) -> {
        double actualValue = new Double(actual.toString());
        double expectedValue = new Double(expected.toString());
        return expectedValue - 50 * DEFAULT_TEST_STANDARD_DEVIATION <= actualValue && actualValue <= expectedValue + 50 * DEFAULT_TEST_STANDARD_DEVIATION;
    };

    private TestNoisyAggregationUtils()
    {
    }

    public static <T> List<T> createTestValues(int numRows, boolean includeNull, T value, boolean fixedValue)
    {
        ArrayList<T> values = new ArrayList<>();
        for (int i = 0; i < numRows; i++) {
            if (fixedValue) {
                values.add(value);
            }
            else {
                if (value instanceof Double) {
                    values.add((T) Double.valueOf(i));
                }
                else if (value instanceof Integer) {
                    values.add((T) Integer.valueOf(i));
                }
                else if (value instanceof Long) {
                    values.add((T) Long.valueOf(i));
                }
                else if (value instanceof Boolean) {
                    values.add((T) Boolean.valueOf(i % 2 == 0));
                }
            }
        }
        if (includeNull) {
            values.remove(0);
            values.add(null);
        }

        return values;
    }

    /**
     * Build a dataset that can be selected from. This is in the form of:
     * (SELECT
     * CAST(index AS bigint) AS index,
     * CAST(col_bigint AS bigint) as col_bigint,
     * CAST(col_varchar AS varchar) as col_varchar
     * FROM (
     * VALUES
     * (1, 1, '{}'),
     * (NULL, NULL, NULL)
     * ) AS t (index, col_bigint, col_varchar))
     * <p>
     * CASTs is to make sure data type is explicitly provided, not inferred
     */
    public static String buildData(int numRows, boolean includeNullValue, List<String> types)
    {
        int finalNumRows = numRows;
        if (includeNullValue) {
            finalNumRows = numRows - 1;
        }
        // Build CASTs to make sure data type is explicitly provided, not inferred
        StringBuilder sb = new StringBuilder();
        sb.append("(SELECT ");
        sb.append("CAST(index AS bigint) AS index, ");
        for (int i = 0; i < types.size(); i++) {
            String type = types.get(i);
            String typeString = type.equals(StandardTypes.DECIMAL) ? "DECIMAL(" + MAX_PRECISION + ")" : type;
            String column = buildColumnName(type);
            sb.append("CAST(").append(column).append(" AS ").append(typeString).append(") AS ").append(column);
            if (i < types.size() - 1) {
                sb.append(",");
            }
            sb.append(" ");
        }
        sb.append("FROM (VALUES ");
        for (int i = 0; i < finalNumRows; i++) {
            if (i > 0) {
                sb.append(",");
            }
            buildRow(sb, i, types, false);
        }
        if (includeNullValue) {
            sb.append(",");
            buildRow(sb, finalNumRows, types, true);
        }
        sb.append(") AS t (").append("index");
        // build column names
        for (String type : types) {
            sb.append(", ").append(buildColumnName(type));
        }
        sb.append("))");
        return sb.toString();
    }

    public static String buildColumnName(String type)
    {
        return "col_" + type;
    }

    public static void buildRow(StringBuilder sb, int index, List<String> types, boolean isNullRow)
    {
        // index column
        sb.append("(").append(isNullRow ? "NULL" : index);

        // value column(s)
        for (String type : types) {
            sb.append(", ");
            if (isNullRow) {
                sb.append("NULL");
            }
            else {
                switch (type) {
                    case StandardTypes.TINYINT:
                    case StandardTypes.SMALLINT:
                    case StandardTypes.INTEGER:
                    case StandardTypes.BIGINT:
                        sb.append(index);
                        break;
                    case StandardTypes.REAL:
                    case StandardTypes.DOUBLE:
                    case StandardTypes.DECIMAL:
                        sb.append(index).append(".0");
                        break;
                    case StandardTypes.VARCHAR:
                    case StandardTypes.CHAR:
                    case StandardTypes.VARBINARY:
                    case StandardTypes.JSON:
                        sb.append("'{}'");
                        break;
                    case StandardTypes.BOOLEAN:
                        sb.append(index % 2 == 0 ? "true" : "false");
                        break;
                }
            }
        }
        sb.append(")");
    }

    public static double sum(List<Double> values)
    {
        return values.stream().mapToDouble(f -> f == null ? 0 : f).sum();
    }

    public static double sumLong(List<Long> values)
    {
        return values.stream().mapToLong(v -> v == null ? 0 : v).sum();
    }

    public static double countTrue(List<Boolean> values)
    {
        return values.stream().mapToLong(v -> v == null || !v ? 0 : 1).sum();
    }

    public static double avg(List<Double> values)
    {
        return sum(values) / countNonNull(values);
    }

    public static double avgLong(List<Long> values)
    {
        return sumLong(values) / countNonNullLong(values);
    }

    public static double countNonNull(List<Double> values)
    {
        return values.stream().mapToLong(f -> f == null ? 0 : 1).sum();
    }

    public static long countNonNullLong(List<Long> values)
    {
        return values.stream().mapToLong(v -> v == null ? 0 : 1).sum();
    }

    public static List<String> toNullableStringList(List<Long> values)
    {
        return values.stream().map(v -> v == null ? null : String.valueOf(v)).collect(Collectors.toList());
    }
}
