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
package com.facebook.presto.rewriter;

import com.facebook.presto.Session;
import com.facebook.presto.common.type.BigintType;
import com.facebook.presto.common.type.DateType;
import com.facebook.presto.common.type.DecimalType;
import com.facebook.presto.common.type.DoubleType;
import com.facebook.presto.common.type.IntegerType;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.VarcharType;
import com.facebook.presto.testing.ExpectedQueryRunner;
import com.facebook.presto.testing.MaterializedResult;
import com.facebook.presto.testing.MaterializedRow;

import java.time.LocalDate;
import java.util.Arrays;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

final class StoredResultExpectedQueryRunner
        implements ExpectedQueryRunner
{
    private final Function<String, String> getQueryResultPath;
    private final Function<String, String> extractQueryName;

    StoredResultExpectedQueryRunner(Function<String, String> getQueryResultPath, Function<String, String> extractQueryName)
    {
        this.getQueryResultPath = getQueryResultPath;
        this.extractQueryName = extractQueryName;
    }

    @Override
    public void close()
    {
    }

    @Override
    public MaterializedResult execute(Session session, String sql, List<? extends Type> resultTypes)
    {
        String queryName = extractQueryName.apply(sql);
        String queryResultPath = getQueryResultPath.apply(queryName);
        String rawResult = AbstractOptPlusTestFramework.read(queryResultPath);
        String[] rawStringRows = rawResult.split("\n");
        // parse first Row e.g. --types: DECIMAL|VARCHAR|VARCHAR|BIGINT|VARCHAR
        String csvDelimiter = "\\|";
        String csvHeader = rawStringRows[0];
        int typesIndex = csvHeader.indexOf("types: ");
        String[] types = new String[] {};
        if (typesIndex > 0) {
            types = csvHeader.substring(typesIndex + 7).split(csvDelimiter);
        }
        List<? extends Type> prestoTypes = Arrays.stream(types).map(type -> getPrestoType(type)).collect(Collectors.toList());
        return MaterializedResult.resultBuilder(session, prestoTypes).rows(
                IntStream.range(1, rawStringRows.length)
                        .mapToObj(i -> rawStringRows[i].split(csvDelimiter))
                        .map(elements -> getMaterializedRowApplyingPrestoTypes(elements, prestoTypes))
                        .collect(Collectors.toList())).build();
    }

    private MaterializedRow getMaterializedRowApplyingPrestoTypes(String[] rawStringRow, List<? extends Type> types)
    {
        if (types.size() != rawStringRow.length) {
            // assume all strings.
            return new MaterializedRow(MaterializedResult.DEFAULT_PRECISION,
                    IntStream.range(0, rawStringRow.length).mapToObj(i -> rawStringRow[i]).collect(Collectors.toList()));
        }
        return new MaterializedRow(MaterializedResult.DEFAULT_PRECISION,
                IntStream.range(0, rawStringRow.length).mapToObj(i -> getJavaTypedObjectFromRawString(rawStringRow[i], types.get(i))).collect(Collectors.toList()));
    }

    private Object getJavaTypedObjectFromRawString(String rawString, Type type)
    {
        if (null == rawString || rawString.equals("null")) {
            return null;
        }
        if (type.equals(VarcharType.VARCHAR)) {
            return rawString;
        }
        else if (type.equals(IntegerType.INTEGER)) {
            return Integer.decode(rawString);
        }
        else if (type.equals(BigintType.BIGINT)) {
            // Long represents BigInt in presto.
            return Long.decode(rawString);
        }
        else if (type.equals(DoubleType.DOUBLE)) {
            return Double.valueOf(rawString);
        }
        else if (type.equals(DecimalType.createDecimalType())) {
            return Double.valueOf(rawString);
        }
        else if (type.equals(DateType.DATE)) {
            String[] dateValues = rawString.split("-");
            return LocalDate.of(Integer.parseInt(dateValues[0]), Integer.parseInt(dateValues[1]), Integer.parseInt(dateValues[2]));
        }
        return rawString;
    }

    private Type getPrestoType(String type)
    {
        switch (type) {
            case "VARCHAR":
                return VarcharType.VARCHAR;
            case "BIGINT":
                return BigintType.BIGINT;
            case "DOUBLE":
                return DoubleType.DOUBLE;
            case "DECIMAL":
                return DecimalType.createDecimalType();
            case "DATE":
                return DateType.DATE;
            case "INTEGER":
                return IntegerType.INTEGER;
        }
        return null;
    }
}
