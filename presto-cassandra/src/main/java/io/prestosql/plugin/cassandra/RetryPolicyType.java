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
package io.prestosql.plugin.cassandra;

import com.datastax.driver.core.policies.DefaultRetryPolicy;
import com.datastax.driver.core.policies.DowngradingConsistencyRetryPolicy;
import com.datastax.driver.core.policies.FallthroughRetryPolicy;
import com.datastax.driver.core.policies.RetryPolicy;

import static java.util.Objects.requireNonNull;

public enum RetryPolicyType
{
    DEFAULT(DefaultRetryPolicy.INSTANCE),
    BACKOFF(BackoffRetryPolicy.INSTANCE),
    DOWNGRADING_CONSISTENCY(DowngradingConsistencyRetryPolicy.INSTANCE),
    FALLTHROUGH(FallthroughRetryPolicy.INSTANCE);

    private final RetryPolicy policy;

    RetryPolicyType(RetryPolicy policy)
    {
        this.policy = requireNonNull(policy, "policy is null");
    }

    public RetryPolicy getPolicy()
    {
        return policy;
    }
}
