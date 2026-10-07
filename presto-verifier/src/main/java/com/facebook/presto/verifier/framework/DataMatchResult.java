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
package com.facebook.presto.verifier.framework;

import com.facebook.presto.verifier.checksum.ChecksumResult;
import com.facebook.presto.verifier.checksum.ColumnMatchResult;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.MapDifference;
import com.google.common.collect.Maps;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;

import static com.facebook.presto.verifier.framework.DataMatchResult.MatchType.COLUMN_MISMATCH;
import static com.facebook.presto.verifier.framework.DataMatchResult.MatchType.MATCH;
import static com.facebook.presto.verifier.framework.DataMatchResult.MatchType.ROW_COUNT_MISMATCH;
import static com.facebook.presto.verifier.framework.DataMatchResult.MatchType.SCHEMA_MISMATCH;
import static com.facebook.presto.verifier.framework.DataMatchResult.MatchType.SNAPSHOT_DOES_NOT_EXIST;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.lang.String.format;
import static java.lang.String.join;
import static java.util.Objects.requireNonNull;

public class DataMatchResult
        implements MatchResult
{
    public enum DataType
    {
        DATA,
        PARTITION_DATA,
        BUCKET_DATA,
    }
    public enum MatchType
    {
        MATCH,
        SCHEMA_MISMATCH,
        ROW_COUNT_MISMATCH,
        COLUMN_MISMATCH,
        PARTITION_COUNT_MISMATCH,
        BUCKET_COUNT_MISMATCH,
        SNAPSHOT_DOES_NOT_EXIST,
    }

    private final DataType dataType;
    private final MatchType matchType;
    private final Optional<ChecksumResult> controlChecksum;
    private final Optional<ChecksumResult> testChecksum;
    private final OptionalLong controlRowCount;
    private final OptionalLong testRowCount;
    private final List<ColumnMatchResult<?>> mismatchedColumns;
    // Populated for SCHEMA_MISMATCH only; the two schemas being compared, in declaration order.
    private final List<Column> controlColumns;
    private final List<Column> testColumns;

    public DataMatchResult(
            DataType dataType,
            MatchType matchType,
            Optional<ChecksumResult> controlChecksum,
            Optional<ChecksumResult> testChecksum,
            OptionalLong controlRowCount,
            OptionalLong testRowCount,
            List<ColumnMatchResult<?>> mismatchedColumns)
    {
        this(dataType, matchType, controlChecksum, testChecksum, controlRowCount, testRowCount, mismatchedColumns, ImmutableList.of(), ImmutableList.of());
    }

    private DataMatchResult(
            DataType dataType,
            MatchType matchType,
            Optional<ChecksumResult> controlChecksum,
            Optional<ChecksumResult> testChecksum,
            OptionalLong controlRowCount,
            OptionalLong testRowCount,
            List<ColumnMatchResult<?>> mismatchedColumns,
            List<Column> controlColumns,
            List<Column> testColumns)
    {
        this.dataType = requireNonNull(dataType, "data type is null");
        this.matchType = requireNonNull(matchType, "match type is null");
        this.controlChecksum = requireNonNull(controlChecksum, "controlChecksum is null");
        this.testChecksum = requireNonNull(testChecksum, "testChecksum is null");
        this.controlRowCount = requireNonNull(controlRowCount, "controlRowCount is null");
        this.testRowCount = requireNonNull(testRowCount, "testRowCount is null");
        this.mismatchedColumns = ImmutableList.copyOf(mismatchedColumns);
        this.controlColumns = ImmutableList.copyOf(requireNonNull(controlColumns, "controlColumns is null"));
        this.testColumns = ImmutableList.copyOf(requireNonNull(testColumns, "testColumns is null"));
    }

    public static DataMatchResult schemaMismatch(DataType dataType, List<Column> controlColumns, List<Column> testColumns)
    {
        return new DataMatchResult(
                dataType,
                SCHEMA_MISMATCH,
                Optional.empty(),
                Optional.empty(),
                OptionalLong.empty(),
                OptionalLong.empty(),
                ImmutableList.of(),
                controlColumns,
                testColumns);
    }

    @Override
    public boolean isMatched()
    {
        return matchType == MATCH;
    }

    @Override
    public String getDataType()
    {
        return dataType.name();
    }

    @Override
    public String getMatchTypeName()
    {
        return matchType.name();
    }

    @Override
    public boolean isMismatchPossiblyCausedByNonDeterminism()
    {
        return matchType == ROW_COUNT_MISMATCH || matchType == COLUMN_MISMATCH;
    }

    @Override
    public boolean isMismatchPossiblyCausedByReuseOutdatedTable()
    {
        return matchType == SCHEMA_MISMATCH || matchType == ROW_COUNT_MISMATCH || matchType == COLUMN_MISMATCH;
    }

    public MatchType getMatchType()
    {
        return matchType;
    }

    public ChecksumResult getControlChecksum()
    {
        checkState(controlChecksum.isPresent(), "controlChecksum is missing");
        return controlChecksum.get();
    }

    public ChecksumResult getTestChecksum()
    {
        checkState(testChecksum.isPresent(), "testChecksum is missing");
        return testChecksum.get();
    }

    public List<ColumnMatchResult<?>> getMismatchedColumns()
    {
        return mismatchedColumns;
    }

    public String getReport()
    {
        StringBuilder message = new StringBuilder()
                .append(matchType.name().replace("_", " "))
                .append('\n');
        if (matchType == SCHEMA_MISMATCH) {
            return message.append(getSchemaDifference()).toString();
        }
        if (matchType == SNAPSHOT_DOES_NOT_EXIST) {
            return message.toString();
        }

        checkState(controlRowCount.isPresent(), "controlRowCount is missing");
        checkState(testRowCount.isPresent(), "testRowCount is missing");
        message.append(format("Control %s rows, Test %s rows%n", controlRowCount.getAsLong(), testRowCount.getAsLong()));
        if (matchType == ROW_COUNT_MISMATCH) {
            return message.toString();
        }

        message.append("Mismatched Columns:\n");
        mismatchedColumns.forEach(columnMismatch ->
                message.append(format(
                        "  %s (%s)%s\n    control\t(%s)\n    test\t(%s)\n",
                        columnMismatch.getColumn().getName(),
                        columnMismatch.getColumn().getType().getDisplayName(),
                        columnMismatch.getMessage().map(columnMessage -> " " + columnMessage).orElse(""),
                        columnMismatch.getControlChecksum(),
                        columnMismatch.getTestChecksum())));
        return message.toString();
    }

    // Describes how the control and test schemas differ. Columns are keyed by name, which is unique
    // because both schemas come from a CREATE TABLE AS. Emits one line per column so that failures
    // can be aggregated across a verifier run.
    private String getSchemaDifference()
    {
        MapDifference<String, String> difference = Maps.difference(toTypesByName(controlColumns), toTypesByName(testColumns));

        StringBuilder message = new StringBuilder();
        if (!difference.entriesDiffering().isEmpty()) {
            message.append("Differing Columns:\n");
            difference.entriesDiffering().forEach((name, types) ->
                    message.append(format("  %s: %s -> %s\n", name, types.leftValue(), types.rightValue())));
        }
        appendColumns(message, "Only in Control", difference.entriesOnlyOnLeft());
        appendColumns(message, "Only in Test", difference.entriesOnlyOnRight());

        // Identical names and types in both schemas means the columns are merely ordered differently.
        if (message.length() == 0 && !controlColumns.isEmpty()) {
            appendColumnOrderDifference(message);
        }
        return message.toString();
    }

    private static void appendColumns(StringBuilder message, String header, Map<String, String> columns)
    {
        if (columns.isEmpty()) {
            return;
        }
        message.append(header).append(":\n");
        columns.forEach((name, type) -> message.append(format("  %s: %s\n", name, type)));
    }

    // Reports the shortest window of positions containing the reordering, by dropping the prefix
    // and suffix the two schemas agree on. Positions are 1-based.
    private void appendColumnOrderDifference(StringBuilder message)
    {
        // Equal type maps but unequal lengths means a name repeats, which collapsed in the map.
        if (controlColumns.size() != testColumns.size()) {
            message.append(format("Column Count Differs: control %s, test %s\n", controlColumns.size(), testColumns.size()));
            return;
        }

        List<String> controlNames = toNames(controlColumns);
        List<String> testNames = toNames(testColumns);
        int size = controlNames.size();

        int start = 0;
        while (start < size && controlNames.get(start).equals(testNames.get(start))) {
            start++;
        }
        int end = size;
        while (end > start && controlNames.get(end - 1).equals(testNames.get(end - 1))) {
            end--;
        }

        message.append(format("Column Order Differs (positions %s-%s):\n", start + 1, end))
                .append(format("  control: %s\n", join(", ", controlNames.subList(start, end))))
                .append(format("  test: %s\n", join(", ", testNames.subList(start, end))));
    }

    // Not an ImmutableMap: reporting a failure must not itself fail should a schema ever carry
    // duplicate column names.
    private static Map<String, String> toTypesByName(List<Column> columns)
    {
        Map<String, String> types = new LinkedHashMap<>();
        columns.forEach(column -> types.put(column.getName(), column.getType().getDisplayName()));
        return types;
    }

    private static List<String> toNames(List<Column> columns)
    {
        return columns.stream().map(Column::getName).collect(toImmutableList());
    }
}
