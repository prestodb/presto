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

import com.datastax.oss.driver.api.core.retry.RetryPolicy;
import com.datastax.oss.driver.internal.core.retry.ConsistencyDowngradingRetryPolicy;
import com.datastax.oss.driver.internal.core.retry.DefaultRetryPolicy;

import static java.util.Objects.requireNonNull;

public enum RetryPolicyType
{
    DEFAULT(DefaultRetryPolicy.class),
    BACKOFF(BackoffRetryPolicy.class),
    DOWNGRADING_CONSISTENCY(ConsistencyDowngradingRetryPolicy.class),
    FALLTHROUGH(DefaultRetryPolicy.class); // Fallthrough is similar to default in 4.x

    private final Class<? extends RetryPolicy> policyClass;

    RetryPolicyType(Class<? extends RetryPolicy> policyClass)
    {
        this.policyClass = requireNonNull(policyClass, "policyClass is null");
    }

    public Class<? extends RetryPolicy> getPolicyClass()
    {
        return policyClass;
    }
}
