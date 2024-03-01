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
package com.facebook.presto.druid.segment;

import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.druid.DruidColumnHandle;
import com.facebook.presto.druid.column.ColumnReader;
import com.facebook.presto.druid.column.SimpleReadableOffset;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.PrestoException;
import com.google.common.collect.ImmutableMap;
import org.apache.druid.segment.ColumnValueSelector;
import org.apache.druid.segment.QueryableIndex;
import org.apache.druid.segment.column.BaseColumn;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static com.facebook.presto.druid.DruidErrorCode.DRUID_SEGMENT_LOAD_ERROR;
import static com.facebook.presto.druid.column.ColumnReader.createColumnReader;
import static java.lang.Math.min;
import static java.lang.Math.toIntExact;

public class DruidSegmentReader
        implements SegmentReader
{
    private static final int BATCH_SIZE = 1024;

    private final Map<String, ColumnReader> columnValueSelectors;
    private final long totalRowCount;

    private QueryableIndex queryableIndex;
    private long currentPosition;
    private int currentBatchSize;

    public DruidSegmentReader(SegmentIndexSource segmentIndexSource, List<ColumnHandle> columns)
    {
        try {
            queryableIndex = segmentIndexSource.loadIndex(columns);
            totalRowCount = queryableIndex.getNumRows();
            ImmutableMap.Builder<String, ColumnReader> selectorsBuilder = ImmutableMap.builder();
            for (ColumnHandle column : columns) {
                DruidColumnHandle druidColumn = (DruidColumnHandle) column;
                String columnName = druidColumn.getColumnName();
                Type type = druidColumn.getColumnType();
                BaseColumn baseColumn = queryableIndex.getColumnHolder(columnName).getColumn();
                ColumnValueSelector<?> valueSelector = baseColumn.makeColumnValueSelector(new SimpleReadableOffset());
                selectorsBuilder.put(columnName, createColumnReader(type, valueSelector));
            }
            columnValueSelectors = selectorsBuilder.build();
        }
        catch (IOException e) {
            throw new PrestoException(DRUID_SEGMENT_LOAD_ERROR, "failed to load druid segment");
        }
    }

    @Override
    public int nextBatch()
    {
        // TODO: dynamic batch sizing
        currentBatchSize = toIntExact(min(BATCH_SIZE, totalRowCount - currentPosition));
        currentPosition += currentBatchSize;
        return currentBatchSize;
    }

    @Override
    public Block readBlock(Type type, String columnName)
    {
        return columnValueSelectors.get(columnName).readBlock(type, currentBatchSize);
    }
}
