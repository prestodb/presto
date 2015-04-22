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

import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorPartition;
import com.facebook.presto.spi.TupleDomain;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkNotNull;

public class RaptorPartition
        implements ConnectorPartition
{
    private final long tableId;
    private final TupleDomain<ColumnHandle> effectivePredicate;

    public RaptorPartition(long tableId, TupleDomain<ColumnHandle> effectivePredicate)
    {
        this.tableId = tableId;
        this.effectivePredicate = checkNotNull(effectivePredicate, "effectivePredicate is null");
    }

    @Override
    public String getPartitionId()
    {
        return String.valueOf(tableId);
    }

    @Override
    public TupleDomain<ColumnHandle> getTupleDomain()
    {
        return TupleDomain.all();
    }

    public TupleDomain<ColumnHandle> getEffectivePredicate()
    {
        return effectivePredicate;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("tableId", tableId)
                .toString();
    }
}
