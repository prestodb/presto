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
package com.facebook.presto.metadata;

import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorResolvedIndex;
import com.facebook.presto.spi.TupleDomain;

import static java.util.Objects.requireNonNull;

public final class ResolvedIndex
{
    private final IndexHandle indexHandle;
    private final TupleDomain<ColumnHandle> undeterminedTupleDomain;

    public ResolvedIndex(String connectorId, ConnectorResolvedIndex index)
    {
        requireNonNull(connectorId, "connectorId is null");
        requireNonNull(index, "index is null");

        indexHandle = new IndexHandle(connectorId, index.getIndexHandle());
        undeterminedTupleDomain = index.getUnresolvedTupleDomain();
    }

    public IndexHandle getIndexHandle()
    {
        return indexHandle;
    }

    public TupleDomain<ColumnHandle> getUnresolvedTupleDomain()
    {
        return undeterminedTupleDomain;
    }
}
