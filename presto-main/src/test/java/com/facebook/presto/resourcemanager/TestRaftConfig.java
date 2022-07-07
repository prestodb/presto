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
package com.facebook.presto.resourcemanager;

import com.facebook.airlift.configuration.testing.ConfigAssertions;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.util.Map;

import static com.facebook.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static com.facebook.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;

public class TestRaftConfig
{
    @Test
    public void testDefaults()
    {
        assertRecordedDefaults(ConfigAssertions.recordDefaults(RaftConfig.class)
                .setEnabled(false)
                .setGroupId(null)
                .setPort(0)
                .setStorageDir(null));
    }

    @Test
    public void testExplicitPropertyMappings()
    {
        Map<String, String> properties = new ImmutableMap.Builder<String, String>()
                .put("raft.storageDir", "/tmp/raft-server")
                .put("raft.groupId", "testRaftGroupId1")
                .put("raft.port", "6000")
                .put("raft.isEnabled", "true")
                .build();

        RaftConfig expected = new RaftConfig()
                .setEnabled(true)
                .setGroupId("testRaftGroupId1")
                .setPort(6000)
                .setStorageDir("/tmp/raft-server");

        assertFullMapping(properties, expected);
    }
}
