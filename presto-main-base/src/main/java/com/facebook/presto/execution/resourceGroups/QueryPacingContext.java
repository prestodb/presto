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
package com.facebook.presto.execution.resourceGroups;

/**
 * Context for query admission pacing. Provides a single interface for
 * global rate limiting and running query tracking to prevent worker overload.
 * <p>
 * This interface consolidates the pacing-related callbacks that are shared
 * across all resource groups, keeping resource group objects smaller.
 */
public interface QueryPacingContext
{
    /**
     * A no-op implementation that allows all queries and tracks nothing.
     */
    QueryPacingContext NOOP = new QueryPacingContext()
    {
        @Override
        public boolean tryAcquireAdmissionSlot()
        {
            return true;
        }

        @Override
        public void onQueryStarted()
        {
        }

        @Override
        public void onQueryFinished()
        {
        }
    };

    /**
     * Attempts to acquire an admission slot for starting a new query.
     * Enforces global rate limiting when running queries exceed threshold.
     *
     * @return true if query can be admitted, false if rate limit exceeded
     */
    boolean tryAcquireAdmissionSlot();

    /**
     * Called when a query starts running. Used to track global running query count.
     */
    void onQueryStarted();

    /**
     * Called when a query finishes (success or failure). Used to track global running query count.
     */
    void onQueryFinished();
}
