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
package com.facebook.presto.spi.plan;

/**
 * Defines a suite of plan node-specific methods for the computation of logical properties.
 * Supplies a default implementation that produces an empty set of logical properties, and additionally,
 * a suite of plan-node specific overrides of the default implementation. The implementations leverage
 * property propagation builders supplied by LogicalPropertiesImpl. The LogicalPropertiesProvider
 * mechanism enables a plan node to receive its logical property compute capabilities via dependency injection.
 * This is needed because the computation of logical properties requires analysis of plan node's argument expressions,
 * and the code the performs this analysis must be built in presto-main as this is where expression related classes are visible.
 * The property computation implementation is dynamically injected into the presto-spi and presto-main plan node method's responsible
 * for computing logical properties. This interface is defined in presto-spi where it is visible to all plan nodes. The
 * implementation LogicalPropertiesImpl is provided in presto-main as per the reasons described above.
 */
public interface LogicalPropertiesProvider
{
    LogicalProperties getDefaultProperties();

    LogicalProperties getTableScanProperties(TableScanNode tableScanNode);

    LogicalProperties getProjectProperties(ProjectNode projectNode);

    LogicalProperties getFilterProperties(FilterNode filterNode);

    LogicalProperties getJoinProperties(PlanNode joinNode);

    LogicalProperties getSemiJoinProperties(PlanNode semiJNode);

    LogicalProperties getSortProperties(PlanNode sortNode);

    LogicalProperties getAggregationProperties(AggregationNode aggregationNode);

    LogicalProperties getLimitProperties(LimitNode limitNode);

    LogicalProperties getTopNProperties(TopNNode limitNode);

    LogicalProperties getValuesProperties(ValuesNode valuesNode);

    LogicalProperties getDistinctLimitProperties(DistinctLimitNode limitNode);

    LogicalProperties getAssignUniqueIdProperties(PlanNode assignUniqueId);
}
