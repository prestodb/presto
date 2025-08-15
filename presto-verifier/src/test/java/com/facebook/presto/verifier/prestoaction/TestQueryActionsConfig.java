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
package com.facebook.presto.verifier.prestoaction;

import com.google.common.collect.ImmutableMap;
import io.airlift.units.Duration;
import org.testng.annotations.Test;

import java.util.Map;

import static com.facebook.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static com.facebook.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static com.facebook.airlift.configuration.testing.ConfigAssertions.recordDefaults;
import static java.util.concurrent.TimeUnit.HOURS;
import static java.util.concurrent.TimeUnit.MINUTES;

public class TestQueryActionsConfig
{
    @Test
    public void testDefault()
    {
        assertRecordedDefaults(recordDefaults(QueryActionsConfig.class)
                .setControlQueryActionType(JdbcPrestoAction.QUERY_ACTION_TYPE)
                .setTestQueryActionType(JdbcPrestoAction.QUERY_ACTION_TYPE)
                .setRunHelperQueriesOnControl(true)
                .setMetadataTimeout(new Duration(3, MINUTES))
                .setChecksumTimeout(new Duration(30, MINUTES)));
    }

    @Test
    public void testExplicitPropertyMappings()
    {
        Map<String, String> properties = new ImmutableMap.Builder<String, String>()
                .put("control.query-action-type", "control-action")
                .put("test.query-action-type", "test-action")
                .put("run-helper-queries-on-control", "false")
                .put("metadata-timeout", "1h")
                .put("checksum-timeout", "3h")
                .build();
        QueryActionsConfig expected = new QueryActionsConfig()
                .setControlQueryActionType("control-action")
                .setTestQueryActionType("test-action")
                .setRunHelperQueriesOnControl(false)
                .setMetadataTimeout(new Duration(1, HOURS))
                .setChecksumTimeout(new Duration(3, HOURS));

        assertFullMapping(properties, expected);
    }
}
