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

import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorHandleResolver;
import com.facebook.presto.spi.ConnectorInsertTableHandle;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;

public class TablestoreHandleResolver
        implements ConnectorHandleResolver
{
    @Override
    public Class<? extends ConnectorTableHandle> getTableHandleClass()
    {
        return TablestoreTableHandle.class;
    }

    @Override
    public Class<? extends ConnectorTableLayoutHandle> getTableLayoutHandleClass()
    {
        return TablestoreTableLayoutHandle.class;
    }

    @Override
    public Class<? extends ColumnHandle> getColumnHandleClass()
    {
        return TablestoreColumnHandle.class;
    }

    @Override
    public Class<? extends ConnectorTransactionHandle> getTransactionHandleClass()
    {
        return TablestoreTransactionHandle.class;
    }

    @Override
    public Class<? extends ConnectorSplit> getSplitClass()
    {
        return TablestoreSplit.class;
    }

    public Class<? extends ConnectorInsertTableHandle> getInsertTableHandleClass()
    {
        return TablestoreInsertTableHandle.class;
    }
}
