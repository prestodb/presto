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
package com.facebook.presto.release.git;

import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.util.Map;

import static com.facebook.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static com.facebook.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static com.facebook.airlift.configuration.testing.ConfigAssertions.recordDefaults;

public class TestFileRepositoryConfig
{
    @Test
    public void testDefault()
    {
        assertRecordedDefaults(recordDefaults(FileRepositoryConfig.class)
                .setUpstreamName("upstream")
                .setOriginName("origin")
                .setDirectory(null)
                .setCheckDirectoryName(true));
    }

    @Test
    public void testExplicitPropertyMappings()
    {
        Map<String, String> properties = new ImmutableMap.Builder<String, String>()
                .put("git.upstream-name", "u")
                .put("git.origin-name", "o")
                .put("git.directory", "/tmp/presto")
                .put("git.check-directory-name", "false")
                .build();
        FileRepositoryConfig expected = new FileRepositoryConfig()
                .setUpstreamName("u")
                .setOriginName("o")
                .setDirectory("/tmp/presto")
                .setCheckDirectoryName(false);

        assertFullMapping(properties, expected);
    }
}
