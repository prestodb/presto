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
package com.facebook.presto.cassandra;

import static java.util.Objects.requireNonNull;

/**
 * Retry policy types for Cassandra driver 4.x.
 * In driver 4.x, retry policies are configured by class name rather than instances.
 */
public enum RetryPolicyType
{
    DEFAULT("DefaultRetryPolicy"),
    BACKOFF("com.facebook.presto.cassandra.BackoffRetryPolicy"),
    /**
     * Downgrading consistency retry policy that always downgrades to LOCAL_ONE.
     * This provides a simplified, predictable downgrading behavior compared to
     * the driver's built-in ConsistencyDowngradingRetryPolicy.
     * <p>
     * <b>Warning:</b> This may break consistency invariants. Use only if you
     * understand the consequences of downgrading to LOCAL_ONE.
     */
    DOWNGRADING_CONSISTENCY("com.facebook.presto.cassandra.LocalOneDowngradingRetryPolicy"),
    FALLTHROUGH("DefaultRetryPolicy"); // Driver 4.x doesn't have FallthroughRetryPolicy, use default

    private final String policyClass;

    RetryPolicyType(String policyClass)
    {
        this.policyClass = requireNonNull(policyClass, "policyClass is null");
    }

    /**
     * Get the retry policy class name for driver 4.x configuration.
     * This is used in the programmatic configuration builder.
     */
    public String getPolicyClass()
    {
        return policyClass;
    }
}
