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
package com.facebook.presto.spark.planner;

import com.facebook.presto.execution.scheduler.TableWriteInfo;

import static java.util.Objects.requireNonNull;

public class PreparedPlan
{
    private final SubPlanWithTaskSources plan;
    private final TableWriteInfo tableWriteInfo;

    public PreparedPlan(SubPlanWithTaskSources plan, TableWriteInfo tableWriteInfo)
    {
        this.plan = requireNonNull(plan, "plan is null");
        this.tableWriteInfo = requireNonNull(tableWriteInfo, "tableWriteInfo is null");
    }

    public SubPlanWithTaskSources getPlan()
    {
        return plan;
    }

    public TableWriteInfo getTableWriteInfo()
    {
        return tableWriteInfo;
    }
}
