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
package com.facebook.presto.secretsManager;

import com.facebook.presto.spi.secretsManager.SecretsManager;
import com.google.common.collect.ImmutableMap;

import java.util.Map;
import java.util.UUID;

public class TestingSecretsManager
        implements SecretsManager
{
    public static final String TESTADM = "testadm";

    @Override
    public Map<String, Map<String, String>> fetchSecrets()
    {
        Map<String, String> properties = new ImmutableMap.Builder<String, String>()
                .put("test-username", TESTADM)
                .put("test-password", TESTADM).build();

        Map<String, Map<String, String>> secrets = ImmutableMap.of(UUID.randomUUID().toString(), properties);
        return secrets;
    }
}
