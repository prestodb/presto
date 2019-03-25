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
package com.facebook.presto.verifier.framework;

import com.google.common.collect.ImmutableMap;
import io.airlift.units.Duration;
import org.testng.annotations.Test;

import java.util.Map;

import static io.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static io.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static io.airlift.configuration.testing.ConfigAssertions.recordDefaults;
import static java.util.concurrent.TimeUnit.HOURS;
import static java.util.concurrent.TimeUnit.MINUTES;

public class TestVerifierConfig
{
    @Test
    public void testDefault()
    {
        assertRecordedDefaults(recordDefaults(VerifierConfig.class)
                .setControlJdbcUrl(null)
                .setTestJdbcUrl(null)
                .setControlTimeout(new Duration(10, MINUTES))
                .setTestTimeout(new Duration(30, MINUTES))
                .setMetadataTimeout(new Duration(3, MINUTES))
                .setChecksumTimeout(new Duration(20, MINUTES))
                .setRelativeErrorMargin(1e-4));
    }

    @Test
    public void testExplicitPropertyMappings()
    {
        Map<String, String> properties = new ImmutableMap.Builder<String, String>()
                .put("control.jdbc-url", "jdbc:presto://proxy.presto.fbinfra.net")
                .put("test.jdbc-url", "jdbc:presto://proxy.presto.fbinfra.net")
                .put("control.timeout", "1h")
                .put("test.timeout", "2h")
                .put("metadata.timeout", "3h")
                .put("checksum.timeout", "4h")
                .put("relative-error-margin", "2e-5")
                .build();
        VerifierConfig expected = new VerifierConfig()
                .setControlJdbcUrl("jdbc:presto://proxy.presto.fbinfra.net")
                .setTestJdbcUrl("jdbc:presto://proxy.presto.fbinfra.net")
                .setControlTimeout(new Duration(1, HOURS))
                .setTestTimeout(new Duration(2, HOURS))
                .setMetadataTimeout(new Duration(3, HOURS))
                .setChecksumTimeout(new Duration(4, HOURS))
                .setRelativeErrorMargin(2e-5);

        assertFullMapping(properties, expected);
    }
}
