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
package com.facebook.presto.raptor;

import com.facebook.presto.raptor.storage.RowSink;
import com.facebook.presto.raptor.storage.RowSinkProvider;
import com.facebook.presto.raptor.storage.TupleBuffer;

public class CappedRowSink
        implements RowSink
{
    private final RowSinkProvider rowSinkProvider;
    private final int rowsPerShard;

    private int currentRowCount;
    private RowSink delegate;

    private CappedRowSink(RowSinkProvider rowSinkProvider, int rowsPerShard)
    {
        this.rowSinkProvider = rowSinkProvider;
        this.rowsPerShard = rowsPerShard;

        this.delegate = rowSinkProvider.getRowSink();
        this.currentRowCount = 0;
    }

    public static RowSink from(RowSinkProvider rowSinkProvider, int rowsPerShard)
    {
        return new CappedRowSink(rowSinkProvider, rowsPerShard);
    }

    @Override
    public void appendTuple(TupleBuffer tupleBuffer)
    {
        createRowSinkIfNeeded();
        delegate.appendTuple(tupleBuffer);
        currentRowCount++;
    }

    @Override
    public void close()
    {
        delegate.close();
    }

    private void createRowSinkIfNeeded()
    {
        if (currentRowCount >= rowsPerShard) {
            delegate.close();
            delegate = rowSinkProvider.getRowSink();
            currentRowCount = 0;
        }
    }
}
