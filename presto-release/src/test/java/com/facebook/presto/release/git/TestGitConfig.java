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

import java.io.File;
import java.util.Map;

import static com.facebook.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static com.facebook.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static com.facebook.airlift.configuration.testing.ConfigAssertions.recordDefaults;

public class TestGitConfig
{
    @Test
    public void testDefault()
    {
        assertRecordedDefaults(recordDefaults(GitConfig.class)
                .setExecutable("git")
                .setSshKeyFile(null));
    }

    @Test
    public void testExplicitPropertyMappings()
    {
        Map<String, String> properties = new ImmutableMap.Builder<String, String>()
                .put("git.executable", "/bin/git")
                .put("git.ssh-key-file", "~/.ssh/id_rsa")
                .build();
        GitConfig expected = new GitConfig()
                .setExecutable("/bin/git")
                .setSshKeyFile(new File("~/.ssh/id_rsa"));

        assertFullMapping(properties, expected);
    }
}
