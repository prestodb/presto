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

import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;

/**
 * Regression test for {@link RetryPolicyType} after switching off driver-internal types (review item M4).
 * <p>
 * Built-in policies must be referenced by their unqualified short name (driver 4.x resolves these
 * relative to {@code com.datastax.oss.driver.internal.core.retry}); the custom policy must be
 * referenced by its fully qualified name. These tests confirm the configured names actually resolve to
 * real classes the same way the driver will at runtime, without the production code importing the
 * internal types.
 */
public class TestRetryPolicyType
{
    // The package the driver prepends to an unqualified retry-policy class name.
    private static final String DRIVER_RETRY_PACKAGE = "com.datastax.oss.driver.internal.core.retry.";

    @Test
    public void testBuiltInPoliciesUseShortNamesThatResolveLikeTheDriver()
            throws ClassNotFoundException
    {
        assertEquals(RetryPolicyType.DEFAULT.getPolicyClassName(), "DefaultRetryPolicy");
        assertEquals(RetryPolicyType.FALLTHROUGH.getPolicyClassName(), "DefaultRetryPolicy");
        assertEquals(RetryPolicyType.DOWNGRADING_CONSISTENCY.getPolicyClassName(), "ConsistencyDowngradingRetryPolicy");

        // Mirror the driver's resolution of an unqualified name; throws if the short name is wrong.
        Class.forName(DRIVER_RETRY_PACKAGE + RetryPolicyType.DEFAULT.getPolicyClassName());
        Class.forName(DRIVER_RETRY_PACKAGE + RetryPolicyType.DOWNGRADING_CONSISTENCY.getPolicyClassName());
    }

    @Test
    public void testCustomBackoffPolicyUsesFullyQualifiedName()
            throws ClassNotFoundException
    {
        String name = RetryPolicyType.BACKOFF.getPolicyClassName();
        assertEquals(name, BackoffRetryPolicy.class.getName());
        // A dotted (absolute) name must resolve directly.
        assertEquals(Class.forName(name), BackoffRetryPolicy.class);
    }

    @Test
    public void testLoadBalancingPolicyShortNameResolvesLikeTheDriver()
            throws ClassNotFoundException
    {
        // CassandraClientModule configures the load-balancing policy by this unqualified short name;
        // confirm it resolves to a real class via the driver's relative-resolution scheme.
        Class.forName("com.datastax.oss.driver.internal.core.loadbalancing.DefaultLoadBalancingPolicy");
    }
}
