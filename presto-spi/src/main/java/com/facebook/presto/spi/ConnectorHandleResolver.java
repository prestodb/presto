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
package com.facebook.presto.spi;

public interface ConnectorHandleResolver
{
    boolean canHandle(ConnectorTableHandle tableHandle);

    default boolean canHandle(ConnectorTableLayoutHandle handle)
    {
        return false;
    }

    boolean canHandle(ColumnHandle columnHandle);

    boolean canHandle(ConnectorSplit split);

    default boolean canHandle(ConnectorIndexHandle indexHandle)
    {
        return false;
    }

    default boolean canHandle(ConnectorOutputTableHandle tableHandle)
    {
        return false;
    }

    default boolean canHandle(ConnectorInsertTableHandle tableHandle)
    {
        return false;
    }

    Class<? extends ConnectorTableHandle> getTableHandleClass();

    default Class<? extends ConnectorTableLayoutHandle> getTableLayoutHandleClass()
    {
        throw new UnsupportedOperationException();
    }

    Class<? extends ColumnHandle> getColumnHandleClass();

    Class<? extends ConnectorSplit> getSplitClass();

    default Class<? extends ConnectorIndexHandle> getIndexHandleClass()
    {
        throw new UnsupportedOperationException();
    }

    default Class<? extends ConnectorOutputTableHandle> getOutputTableHandleClass()
    {
        throw new UnsupportedOperationException();
    }

    default Class<? extends ConnectorInsertTableHandle> getInsertTableHandleClass()
    {
        throw new UnsupportedOperationException();
    }
}
