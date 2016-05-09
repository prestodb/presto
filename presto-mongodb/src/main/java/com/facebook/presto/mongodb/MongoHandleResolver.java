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
package com.facebook.presto.mongodb;

import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorHandleResolver;
import com.facebook.presto.spi.ConnectorInsertTableHandle;
import com.facebook.presto.spi.ConnectorOutputTableHandle;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;

public class MongoHandleResolver
    implements ConnectorHandleResolver
{
    public MongoHandleResolver()
    {
    }

    @Override
    public Class<? extends ConnectorTableHandle> getTableHandleClass()
    {
        return MongoTableHandle.class;
    }

    @Override
    public Class<? extends ColumnHandle> getColumnHandleClass()
    {
        return MongoColumnHandle.class;
    }

    @Override
    public Class<? extends ConnectorSplit> getSplitClass()
    {
        return MongoSplit.class;
    }

    public Class<? extends ConnectorOutputTableHandle> getOutputTableHandleClass()
    {
        return MongoOutputTableHandle.class;
    }

    public Class<? extends ConnectorInsertTableHandle> getInsertTableHandleClass()
    {
        return MongoInsertTableHandle.class;
    }

    public Class<? extends ConnectorTableLayoutHandle> getTableLayoutHandleClass()
    {
        return MongoTableLayoutHandle.class;
    }
}
