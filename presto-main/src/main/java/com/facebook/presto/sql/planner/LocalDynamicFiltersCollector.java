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
import com.facebook.presto.spi.relation.VariableReferenceExpression;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

@ThreadSafe
public class LocalDynamicFiltersCollector
{
    /**
     * May contains domains for dynamic filters for different table scans
     * (e.g. in case of co-located joins).
     */
    @GuardedBy ("this")
    private TupleDomain<VariableReferenceExpression> predicate;

    public LocalDynamicFiltersCollector()
    {
        this.predicate = TupleDomain.all();
    }

    public synchronized TupleDomain<VariableReferenceExpression> getPredicate()
    {
        return predicate;
    }

    public synchronized void intersect(TupleDomain<VariableReferenceExpression> predicate)
    {
        this.predicate = this.predicate.intersect(predicate);
    }
}
