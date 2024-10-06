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
package com.facebook.presto.iceberg.rest;

import com.facebook.airlift.configuration.testing.ConfigAssertions;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.util.Map;

import static com.facebook.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static com.facebook.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static com.facebook.presto.iceberg.rest.AuthenticationType.OAUTH2;
import static com.facebook.presto.iceberg.rest.SessionType.USER;

public class TestIcebergRestConfig
{
    @Test
    public void testDefaults()
    {
        assertRecordedDefaults(ConfigAssertions.recordDefaults(IcebergRestConfig.class)
                .setServerUri(null)
                .setAuthenticationType(null)
                .setAuthenticationServerUri(null)
                .setCredential(null)
                .setToken(null)
                .setSessionType(null));
    }

    @Test
    public void testExplicitPropertyMappings()
    {
        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put("iceberg.rest.uri", "http://localhost:xxx")
                .put("iceberg.rest.auth.type", "OAUTH2")
                .put("iceberg.rest.auth.oauth2.uri", "http://localhost:yyy")
                .put("iceberg.rest.auth.oauth2.credential", "key:secret")
                .put("iceberg.rest.auth.oauth2.token", "SXVLUXUhIExFQ0tFUiEK")
                .put("iceberg.rest.session.type", "USER")
                .build();

        IcebergRestConfig expected = new IcebergRestConfig()
                .setServerUri("http://localhost:xxx")
                .setAuthenticationType(OAUTH2)
                .setAuthenticationServerUri("http://localhost:yyy")
                .setCredential("key:secret")
                .setToken("SXVLUXUhIExFQ0tFUiEK")
                .setSessionType(USER);

        assertFullMapping(properties, expected);
    }
}
