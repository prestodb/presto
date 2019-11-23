/**
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
package com.victoriametrics.presto

import com.facebook.presto.spi.ColumnHandle
import com.facebook.presto.spi.ConnectorTableLayout
import com.facebook.presto.spi.ConnectorTableLayoutHandle
import com.facebook.presto.spi.ConnectorTablePartitioning
import com.facebook.presto.spi.Constraint
import com.facebook.presto.spi.DiscretePredicates
import com.facebook.presto.spi.LocalProperty
import com.facebook.presto.spi.predicate.TupleDomain
import com.victoriametrics.presto.model.VmTableLayoutHandle
import java.util.*

/**
 * Differs from [VmTableHandle]
 */
class VmTableLayout(
    tableLayoutHandle: VmTableLayoutHandle,
    constraint: Constraint<ColumnHandle>,
    desiredColumns: Optional<MutableSet<ColumnHandle>>
) : ConnectorTableLayout(tableLayoutHandle) {
    override fun getPredicate(): TupleDomain<ColumnHandle> {
        return super.getPredicate()
    }

    override fun getStreamPartitioningColumns(): Optional<MutableSet<ColumnHandle>> {
        return super.getStreamPartitioningColumns()
    }

    override fun getDiscretePredicates(): Optional<DiscretePredicates> {
        return super.getDiscretePredicates()
    }

    override fun getLocalProperties(): MutableList<LocalProperty<ColumnHandle>> {
        return super.getLocalProperties()
    }

    override fun getColumns(): Optional<MutableList<ColumnHandle>> {
        return super.getColumns()
    }

    override fun getHandle(): ConnectorTableLayoutHandle {
        return super.getHandle()
    }

    override fun getTablePartitioning(): Optional<ConnectorTablePartitioning> {
        return super.getTablePartitioning()
    }
}

