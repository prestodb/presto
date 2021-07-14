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
package com.facebook.presto.tablestore;

import com.alicloud.openservices.tablestore.model.iterator.ParallelScanRowIterator;
import com.alicloud.openservices.tablestore.model.iterator.SearchRowIterator;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.RecordCursor;

import java.util.List;

public class TablestoreRecordCursor4SearchIndex
        extends TablestoreRowBasedRecordCursor
        implements RecordCursor
{
    public TablestoreRecordCursor4SearchIndex(TablestoreFacade tablestoreFacade, ConnectorSession session,
            TablestoreSplit split, List<TablestoreColumnHandle> columnHandles)
    {
        super(tablestoreFacade, session, split, columnHandles);
    }

    @Override
    public WrappedRowIterator fetchFirst()
    {
        return tablestoreFacade.rowIterator4SearchIndex(session, split, columnHandles);
    }

    @Override
    public long getCompletedBytes()
    {
        checkClosed();
        if (rowIterator instanceof CountStarRowIterator) {
            return ((CountStarRowIterator) rowIterator).getCompletedBytes();
        }
        else if (rowIterator instanceof ParallelScanRowIterator) {
            return ((ParallelScanRowIterator) rowIterator).getCompletedBytes();
        }
        else {
            return ((SearchRowIterator) rowIterator).getCompletedBytes();
        }
    }
}
