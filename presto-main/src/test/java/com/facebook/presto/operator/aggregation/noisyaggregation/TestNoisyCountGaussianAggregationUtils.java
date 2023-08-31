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
import java.util.Arrays;
import java.util.List;

public class TestNoisyCountGaussianAggregationUtils
{
    private TestNoisyCountGaussianAggregationUtils()
    {
    }
    public static List<Long> createTestValues(int numRows, boolean includeNull)
    {
        ArrayList<Long> values = new ArrayList<>();
        for (int i = 0; i < numRows; i++) {
            values.add((long) i);
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
    public static String buildData(int numRows, boolean includeNullValue)
    {
        int finalNumRows = numRows;
        if (includeNullValue) {
            finalNumRows = numRows - 1;
        }
        List<String> types = Arrays.asList(
                StandardTypes.BIGINT,
                StandardTypes.VARCHAR);
        // Build CASTs to make sure data type is explicitly provided, not inferred
        StringBuilder sb = new StringBuilder();
        sb.append("(SELECT ");
        sb.append("CAST(index AS bigint) AS index, ");
        for (int i = 0; i < types.size(); i++) {
            String type = types.get(i);
            String column = buildColumnName(type);
            sb.append("CAST(").append(column).append(" AS ").append(type).append(") AS ").append(column);
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
                if (type.equals(StandardTypes.TINYINT) || type.equals(StandardTypes.SMALLINT) || type.equals(StandardTypes.INTEGER) || type.equals(StandardTypes.BIGINT)
                        || type.equals(StandardTypes.REAL) || type.equals(StandardTypes.DOUBLE)) {
                    sb.append(index);
                }
                if (type.equals(StandardTypes.REAL) || type.equals(StandardTypes.DOUBLE) || type.equals(StandardTypes.DECIMAL)) {
                    sb.append(index).append(".0");
                }
                else if (type.equals(StandardTypes.VARCHAR) || type.equals(StandardTypes.CHAR) || type.equals(StandardTypes.VARBINARY) || type.equals(StandardTypes.JSON)) {
                    sb.append("'{}'");
                }
            }
        }
        sb.append(")");
    }
}
