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

package io.prestosql.plugin.tpcds.statistics;

import com.google.common.collect.ImmutableList;
import com.teradata.tpcds.Results;
import com.teradata.tpcds.Session;
import com.teradata.tpcds.Table;
import com.teradata.tpcds.column.Column;
import com.teradata.tpcds.column.ColumnType;
import io.prestosql.plugin.tpcds.TpcdsRecordSet;
import io.prestosql.spi.connector.RecordCursor;

import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static java.lang.String.format;

class TableStatisticsRecorder
{
    public TableStatisticsData recordStatistics(Table table, double scaleFactor)
    {
        Session session = Session.getDefaultSession()
                .withScale(scaleFactor)
                .withParallelism(1)
                .withNoSexism(false);

        List<Column> columns = ImmutableList.copyOf(table.getColumns());
        RecordCursor recordCursor = new TpcdsRecordSet(Results.constructResults(table, session), columns)
                .cursor();

        List<ColumnStatisticsRecorder> statisticsRecorders = createStatisticsRecorders(columns);
        long rowCount = 0;

        while (recordCursor.advanceNextPosition()) {
            rowCount++;
            for (int columnId = 0; columnId < columns.size(); columnId++) {
                Comparable<?> value = getPrestoValue(recordCursor, columns, columnId);
                statisticsRecorders.get(columnId).record(value);
            }
        }

        Map<String, ColumnStatisticsData> columnSampleStatistics = IntStream.range(0, columns.size())
                .boxed()
                .collect(toImmutableMap(
                        i -> columns.get(i).getName(),
                        i -> statisticsRecorders.get(i).getRecording()));
        return new TableStatisticsData(rowCount, columnSampleStatistics);
    }

    private List<ColumnStatisticsRecorder> createStatisticsRecorders(List<Column> columns)
    {
        return columns.stream()
                .map(column -> new ColumnStatisticsRecorder(column.getType()))
                .collect(toImmutableList());
    }

    private Comparable<?> getPrestoValue(RecordCursor recordCursor, List<Column> columns, int columnId)
    {
        if (recordCursor.isNull(columnId)) {
            return null;
        }

        Column column = columns.get(columnId);
        ColumnType.Base baseType = column.getType().getBase();
        switch (baseType) {
            case IDENTIFIER:
            case INTEGER:
            case DATE:
            case TIME:
            case DECIMAL:
                return recordCursor.getLong(columnId);
            case VARCHAR:
            case CHAR:
                return recordCursor.getSlice(columnId).toStringAscii();
        }
        throw new UnsupportedOperationException(format("Unsupported TPCDS base type [%s]", baseType));
    }
}
