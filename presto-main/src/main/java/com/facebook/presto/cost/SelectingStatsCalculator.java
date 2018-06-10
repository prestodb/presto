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
package com.facebook.presto.cost;

import com.facebook.presto.Session;
import com.facebook.presto.SystemSessionProperties;
import com.facebook.presto.sql.planner.TypeProvider;
import com.facebook.presto.sql.planner.iterative.Lookup;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.google.inject.BindingAnnotation;

import javax.inject.Inject;

import java.lang.annotation.Retention;
import java.lang.annotation.Target;

import static java.lang.annotation.ElementType.METHOD;
import static java.lang.annotation.ElementType.PARAMETER;
import static java.lang.annotation.RetentionPolicy.RUNTIME;
import static java.util.Objects.requireNonNull;

public class SelectingStatsCalculator
        implements StatsCalculator
{
    private final StatsCalculator oldStatsCalculator;
    private final StatsCalculator newStatsCalculator;

    @Inject
    public SelectingStatsCalculator(@Old StatsCalculator oldStatsCalculator, @New StatsCalculator newStatsCalculator)
    {
        this.oldStatsCalculator = requireNonNull(oldStatsCalculator, "oldStatsCalculator can not be null");
        this.newStatsCalculator = requireNonNull(newStatsCalculator, "newStatsCalculator can not be null");
    }

    @Override
    public PlanNodeStatsEstimate calculateStats(PlanNode node, StatsProvider sourceStats, Lookup lookup, Session session, TypeProvider types)
    {
        if (SystemSessionProperties.isEnableNewStatsCalculator(session)) {
            return newStatsCalculator.calculateStats(node, sourceStats, lookup, session, types);
        }
        else {
            return oldStatsCalculator.calculateStats(node, sourceStats, lookup, session, types);
        }
    }

    @BindingAnnotation
    @Target({PARAMETER, METHOD})
    @Retention(RUNTIME)
    public @interface Old {}

    @BindingAnnotation
    @Target({PARAMETER, METHOD})
    @Retention(RUNTIME)
    public @interface New {}
}
