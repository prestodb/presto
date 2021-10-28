
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
package com.facebook.presto.password.file;

import com.google.common.collect.ImmutableMap;
import io.airlift.units.Duration;
import org.testng.annotations.Test;

import java.io.File;
import java.util.Map;

import static com.facebook.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static com.facebook.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static com.facebook.airlift.configuration.testing.ConfigAssertions.recordDefaults;
import static java.util.concurrent.TimeUnit.SECONDS;

public class TestFileConfig
{
    @Test
    public void testDefault()
    {
        assertRecordedDefaults(recordDefaults(FileConfig.class)
                .setPasswordFile(null)
                .setRefreshPeriod(new Duration(5, SECONDS))
                .setAuthTokenCacheMaxSize(1000));
    }

    @Test
    public void testExplicitConfig()
    {
        Map<String, String> properties = new ImmutableMap.Builder<String, String>()
                .put("file.password-file", "/test/password")
                .put("file.refresh-period", "42s")
                .put("file.auth-token-cache.max-size", "1234")
                .build();

        FileConfig expected = new FileConfig()
                .setPasswordFile(new File("/test/password"))
                .setRefreshPeriod(new Duration(42, SECONDS))
                .setAuthTokenCacheMaxSize(1234);

        assertFullMapping(properties, expected);
    }
}
