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
package com.facebook.presto.tpch;

import com.facebook.presto.spi.IndexHandle;

import static com.google.common.base.Preconditions.checkNotNull;

public class TpchIndexHandleResolver
        extends TpchHandleResolver
{
    private final String connectorId;

    public TpchIndexHandleResolver(String connectorId)
    {
        super(connectorId);
        this.connectorId = checkNotNull(connectorId, "connectorId is null");
    }

    @Override
    public boolean canHandle(IndexHandle indexHandle)
    {
        return indexHandle instanceof TpchIndexHandle && ((TpchIndexHandle) indexHandle).getConnectorId().equals(connectorId);
    }

    @Override
    public Class<? extends IndexHandle> getIndexHandleClass()
    {
        return TpchIndexHandle.class;
    }
}
