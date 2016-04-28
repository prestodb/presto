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
package com.facebook.presto.sql.planner.sanity;

import com.facebook.presto.sql.planner.plan.PlanNode;
import com.google.common.collect.ImmutableList;

import java.util.List;

/**
 * It is going to be executed at the end of logical planner, to verify its correctness
 */
public final class PlanSanityChecker
{
    private static final List<Checker> CHECKERS = ImmutableList.of(
            new ValidateDependenciesChecker(),
            new NoSubqueryExpressionLeftChecker(),
            new NoApplyNodeLeftChecker());

    private PlanSanityChecker() {}

    public static void validate(PlanNode planNode)
    {
        CHECKERS.forEach(checker -> checker.validate(planNode));
    }

    public interface Checker
    {
        void validate(PlanNode plan);
    }
}
