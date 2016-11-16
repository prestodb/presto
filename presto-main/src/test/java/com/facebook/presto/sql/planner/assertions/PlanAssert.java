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
package com.facebook.presto.sql.planner.assertions;

import com.facebook.presto.Session;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.sql.planner.Plan;

import static com.facebook.presto.sql.planner.PlanPrinter.textLogicalPlan;
import static java.lang.String.format;
import static org.testng.Assert.assertTrue;

public final class PlanAssert
{
    private PlanAssert() {}

    public static void assertPlan(Session session, Metadata metadata, Plan actual, PlanMatchPattern pattern)
    {
        MatchResult matches = actual.getRoot().accept(new PlanMatchingVisitor(session, metadata), pattern);
        if (!matches.isMatch()) {
            String logicalPlan = textLogicalPlan(actual.getRoot(), actual.getTypes(), metadata, session);
            assertTrue(matches.isMatch(), format("Plan does not match:\n %s\n, to pattern:\n%s", logicalPlan, pattern));
        }
    }
}
