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

public enum RetryPolicyType
{
    // Built-in policies are referenced by their short class name: driver 4.x resolves an unqualified
    // retry-policy class name relative to com.datastax.oss.driver.internal.core.retry, so we avoid
    // importing those internal (non-API) types directly. The custom BackoffRetryPolicy is referenced
    // by its fully qualified name (a dotted name is treated as absolute by the driver).
    DEFAULT("DefaultRetryPolicy"),
    BACKOFF(BackoffRetryPolicy.class.getName()),
    DOWNGRADING_CONSISTENCY("ConsistencyDowngradingRetryPolicy"),
    FALLTHROUGH("DefaultRetryPolicy"); // Fallthrough is similar to default in 4.x

    private final String policyClassName;

    RetryPolicyType(String policyClassName)
    {
        this.policyClassName = requireNonNull(policyClassName, "policyClassName is null");
    }

    public String getPolicyClassName()
    {
        return policyClassName;
    }
}
