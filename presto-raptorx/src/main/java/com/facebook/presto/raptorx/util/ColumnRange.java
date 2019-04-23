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
package com.facebook.presto.raptorx.util;

import com.facebook.presto.raptorx.metadata.ColumnInfo;
import com.facebook.presto.spi.type.Type;

import java.util.List;
import java.util.stream.Stream;

import static com.facebook.presto.raptorx.metadata.IndexWriter.chunkIndexTable;
import static com.facebook.presto.raptorx.metadata.IndexWriter.maxColumn;
import static com.facebook.presto.raptorx.metadata.IndexWriter.minColumn;
import static java.lang.String.format;
import static java.util.stream.Collectors.joining;

public class ColumnRange
{
    private Type columnType;
    private Object value;

    public ColumnRange(Type type, Object val)
    {
        columnType = type;
        value = val;
    }

    public Type getColumnType()
    {
        return columnType;
    }

    public Object getVal()
    {
        return value;
    }

    public static String getColumnRangesMetadataSqlQuery(long commitId, long tableId, List<ColumnInfo> raptorColumns)
    {
        String columns = raptorColumns.stream()
                .flatMap(column -> Stream.of(
                        format("min(%s)", minColumn(column.getColumnId())),
                        format("max(%s)", maxColumn(column.getColumnId()))))
                .collect(joining(", "));
        return format("SELECT %s FROM %s \n" +
                        "  WHERE start_commit_id <= %d\n" +
                        "  AND (end_commit_id > %d OR end_commit_id IS NULL)\n",
                columns, chunkIndexTable(tableId), commitId, commitId);
    }
}
