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
package com.facebook.presto.sql.planner;

import com.facebook.presto.common.predicate.TupleDomain;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.plan.TableScanNode;
import com.facebook.presto.spi.relation.VariableReferenceExpression;

import java.util.Map;

public class LocalDynamicFiltersCollector
{
    /**
     * May contains domains for dynamic filters for different table scans
     * (e.g. in case of co-located joins).
     */
    private TupleDomain<VariableReferenceExpression> predicate;

    LocalDynamicFiltersCollector()
    {
        this.predicate = TupleDomain.all();
    }

    synchronized void intersect(TupleDomain<VariableReferenceExpression> predicate)
    {
        this.predicate = this.predicate.intersect(predicate);
    }

    synchronized TupleDomain<ColumnHandle> get(TableScanNode tableScan)
    {
        Map<VariableReferenceExpression, ColumnHandle> assignments = tableScan.getAssignments();
        // Skips symbols irrelevant to this table scan node.
        return predicate.transform(assignments::get);
    }
}
