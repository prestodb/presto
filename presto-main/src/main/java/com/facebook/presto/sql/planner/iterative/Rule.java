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
package com.facebook.presto.sql.planner.iterative;

import com.facebook.presto.Session;
import com.facebook.presto.matching.Captures;
import com.facebook.presto.matching.Pattern;
import com.facebook.presto.sql.planner.PlanNodeIdAllocator;
import com.facebook.presto.sql.planner.SymbolAllocator;
import com.facebook.presto.sql.planner.plan.PlanNode;

import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public interface Rule<T>
{
    /**
     * Returns a pattern to which plan nodes this rule applies.
     */
    Pattern<T> getPattern();

    default boolean isEnabled(Session session)
    {
        return true;
    }

    Result apply(T node, Captures captures, Context context);

    interface Context
    {
        Lookup getLookup();

        PlanNodeIdAllocator getIdAllocator();

        SymbolAllocator getSymbolAllocator();

        Session getSession();
    }

    class Result
    {
        @Deprecated
        public static Result fromOptional(Optional<PlanNode> transformedPlan)
        {
            return new Result(transformedPlan, Optional.empty());
        }

        public static Result empty()
        {
            return new Result(Optional.empty(), Optional.empty());
        }

        public static Result replace(PlanNode transformedPlan)
        {
            return new Result(Optional.of(transformedPlan), Optional.empty());
        }

        public static Result set(Trait trait)
        {
            return new Result(Optional.empty(), Optional.of(trait));
        }

        private final Optional<PlanNode> transformedPlan;
        private final Optional<Trait> trait;

        private Result(Optional<PlanNode> transformedPlan, Optional<Trait> trait)
        {
            checkArgument(!(transformedPlan.isPresent() && trait.isPresent()), "Either transformed plan or trait can be set");
            this.transformedPlan = requireNonNull(transformedPlan, "transformedPlan is null");
            this.trait = requireNonNull(trait, "trait is null");
        }

        public Optional<PlanNode> getTransformedPlan()
        {
            return transformedPlan;
        }

        public Optional<Trait> getTrait()
        {
            return trait;
        }

        public boolean isPresent()
        {
            return transformedPlan.isPresent() || trait.isPresent();
        }
    }
}
