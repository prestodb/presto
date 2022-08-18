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
package com.facebook.presto.common.plan;

import java.util.List;

import static java.util.Arrays.asList;
import static java.util.Collections.unmodifiableList;

public enum PlanCanonicalizationStrategy
{
    /**
     * DEFAULT strategy is used to canonicalize plans with minimal changes.
     *
     * We remove any unimportant information like source location, make the variable
     * names consistent, and order them.
     * For example:
     * `SELECT * FROM table WHERE id IN (1, 2)` will be equivalent to `SELECT * FROM table WHERE id IN (2, 1)`
     *
     * This is used in context of fragment result caching
     */
    DEFAULT,
    /**
     * CONNECTOR strategy will canonicalize plan according to DEFAULT strategy, and additionally
     * canoncialize `TableScanNode` by giving a connector specific implementation.
     *
     * With this approach, we call ConnectorTableLayoutHandle.getIdentifier() for all `TableScanNode`.
     * Each connector can have a specific implementation to canonicalize table layout handles however they want.
     *
     * For example, Hive connector removes constants from constraints on partition keys:
     * `SELECT * FROM table WHERE ds = '2020-01-01'` will be equivalent to `SELECT * FROM table WHERE ds = '2020-01-02'`
     * where `ds` is a partition column in `table`
     *
     * This is used in context of history based optimizations.
     */
    CONNECTOR,
    /**
     * REMOVE_SAFE_CONSTANTS strategy is used to canonicalize plan with
     * CONNECTOR strategy and will additionally remove constants from plan
     * which are not bound to have impact on plan statistics.
     *
     * This includes removing constants from ProjectNode, but keeps constants
     * in FilterNode since they can have major impact on plan statistics.
     *
     * For example:
     * `SELECT *, 1 FROM table` will be equivalent to `SELECT *, 2 FROM table`
     * `SELECT * FROM table WHERE id > 1` will NOT be equivalent to `SELECT * FROM table WHERE id > 1000`
     *
     * This is used in context of history based optimizations.
     */
    REMOVE_SAFE_CONSTANTS;

    /**
     * Creates a list of PlanCanonicalizationStrategy to be used for history based optimizations.
     * Output is ordered by decreasing accuracy of statistics, at benefit of more coverage.
     */
    public static List<PlanCanonicalizationStrategy> historyBasedPlanCanonicalizationStrategyList()
    {
        return unmodifiableList(asList(CONNECTOR, REMOVE_SAFE_CONSTANTS));
    }
}
