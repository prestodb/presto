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

import com.datastax.oss.driver.api.core.DefaultConsistencyLevel;
import com.datastax.oss.driver.api.core.context.DriverContext;
import com.datastax.oss.driver.api.core.retry.RetryDecision;
import com.datastax.oss.driver.api.core.retry.RetryPolicy;
import org.testng.annotations.Test;

import java.lang.reflect.Constructor;
import java.lang.reflect.Modifier;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

/**
 * Regression test for {@link BackoffRetryPolicy} loadability under driver 4.x.
 * <p>
 * Driver 4.x instantiates retry policies referenced by {@code advanced.retry-policy.class}
 * (wired here via {@link RetryPolicyType}) through reflection, requiring a public
 * {@code (DriverContext, String)} constructor. A previous version exposed only a private no-arg
 * constructor, so selecting {@code cassandra.retry-policy=BACKOFF} failed at session-build time.
 * These tests instantiate the policy exactly the way the driver does.
 */
public class TestBackoffRetryPolicy
{
    @Test
    public void testHasDriverRequiredPublicConstructor()
            throws Exception
    {
        Constructor<BackoffRetryPolicy> constructor =
                BackoffRetryPolicy.class.getConstructor(DriverContext.class, String.class);
        assertTrue(Modifier.isPublic(constructor.getModifiers()),
                "driver 4.x requires a public (DriverContext, String) constructor");

        RetryPolicy policy = constructor.newInstance(null, "default");
        assertNotNull(policy);
        // Sanity-check a non-blocking decision path stays functional.
        assertEquals(policy.onReadTimeout(null, DefaultConsistencyLevel.ONE, 0, 0, false, 0),
                RetryDecision.RETHROW);
    }

    @Test
    public void testRetryPolicyTypeBackoffIsInstantiableByDriver()
            throws Exception
    {
        // Resolve the configured class name and instantiate it via the same reflective path the driver
        // uses to build the policy from config; must not throw.
        Class<?> policyClass = Class.forName(RetryPolicyType.BACKOFF.getPolicyClassName());
        RetryPolicy policy = (RetryPolicy) policyClass
                .getConstructor(DriverContext.class, String.class)
                .newInstance(null, "default");
        assertNotNull(policy);
    }
}
