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

package com.facebook.presto.security;

import com.facebook.presto.spi.security.Identity;
import com.facebook.presto.spi.security.SystemAccessControl;
import org.testng.annotations.Test;

import java.security.Principal;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

public class TestRefreshSystemAccessControl
{
    @Test
    public void testDefaultNoRefresh() throws Exception
    {
        Map<String, String> config = new HashMap<>();
        assertNull(RefreshingSystemAccessControl.optionallyRefresh(config, previous -> null));
    }

    @Test
    public void testCallDelegate() throws Exception
    {
        Map<String, String> config = new HashMap<>();
        config.put(RefreshingSystemAccessControl.REFRESH_ENABLED, "true");
        final AccessControlForTesting mock = new AccessControlForTesting();
        SystemAccessControl ref = RefreshingSystemAccessControl.optionallyRefresh(config, previous -> mock);
        assertNotEquals(mock, ref);
        ref.checkCanSetUser(null, null);
        assertTrue(mock.calledCheckCanSetUser);
    }

    @Test
    public void testRefreshAfterPeriod() throws Exception
    {
        Map<String, String> config = new HashMap<>();
        config.put(RefreshingSystemAccessControl.REFRESH_ENABLED, "true");
        config.put(RefreshingSystemAccessControl.REFRESH_PERIOD_SEC, "1");
        RefreshingSystemAccessControl ref = (RefreshingSystemAccessControl) RefreshingSystemAccessControl
                .optionallyRefresh(config, previous -> new AccessControlForTesting());
        Instant start = Instant.now();
        SystemAccessControl mock = ref.getDelegateForTesting().get();
        // wait until we get a new mock or its been 10 seconds
        while (ref.getDelegateForTesting().get() == mock &&
                !Duration.ofSeconds(10).minus(Duration.between(start, Instant.now())).isNegative()) {
            Thread.sleep(1000);
        }
        assertNotEquals(ref.getDelegateForTesting().get(), mock, "Did not load a new reference");
    }

    private static class AccessControlForTesting
            implements SystemAccessControl
    {
        private boolean calledCheckCanSetUser;

        @Override
        public void checkCanSetUser(Optional<Principal> principal, String userName)
        {
            calledCheckCanSetUser = true;
        }

        @Override
        public void checkCanSetSystemSessionProperty(Identity identity, String propertyName)
        {
        }
    }
}
