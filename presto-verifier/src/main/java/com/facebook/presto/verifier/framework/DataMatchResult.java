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

import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;

import static com.facebook.presto.verifier.framework.DataMatchResult.MatchType.COLUMN_MISMATCH;
import static com.facebook.presto.verifier.framework.DataMatchResult.MatchType.MATCH;
import static com.facebook.presto.verifier.framework.DataMatchResult.MatchType.ROW_COUNT_MISMATCH;
import static com.facebook.presto.verifier.framework.DataMatchResult.MatchType.SCHEMA_MISMATCH;
import static com.facebook.presto.verifier.framework.DataMatchResult.MatchType.SNAPSHOT_DOES_NOT_EXIST;
import static com.google.common.base.Preconditions.checkState;
import static java.lang.String.format;
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
    private final OptionalLong controlRowCount;
    private final OptionalLong testRowCount;
    private final List<ColumnMatchResult<?>> mismatchedColumns;

    public DataMatchResult(
            DataType dataType,
            MatchType matchType,
            Optional<ChecksumResult> controlChecksum,
            OptionalLong controlRowCount,
            OptionalLong testRowCount,
            List<ColumnMatchResult<?>> mismatchedColumns)
    {
        this.dataType = requireNonNull(dataType, "data type is null");
        this.matchType = requireNonNull(matchType, "match type is null");
        this.controlChecksum = requireNonNull(controlChecksum, "controlChecksum is null");
        this.controlRowCount = requireNonNull(controlRowCount, "controlRowCount is null");
        this.testRowCount = requireNonNull(testRowCount, "testRowCount is null");
        this.mismatchedColumns = ImmutableList.copyOf(mismatchedColumns);
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

    public List<ColumnMatchResult<?>> getMismatchedColumns()
    {
        return mismatchedColumns;
    }

    public String getReport()
    {
        StringBuilder message = new StringBuilder()
                .append(matchType.name().replace("_", " "))
                .append('\n');
        if (matchType == SCHEMA_MISMATCH || matchType == SNAPSHOT_DOES_NOT_EXIST) {
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
}
