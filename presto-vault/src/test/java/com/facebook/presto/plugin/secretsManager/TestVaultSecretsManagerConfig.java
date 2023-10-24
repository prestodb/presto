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
package com.facebook.presto.plugin.secretsManager;

import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.util.Map;

import static com.facebook.airlift.configuration.testing.ConfigAssertions.assertFullMapping;

public class TestVaultSecretsManagerConfig
{
    public static final String VAULT_ADDR = "test://vault-url";
    public static final String VAULT_TOKEN = "test-vault-token";
    public static final String VAULT_SECRET_KEYS = "testAccessKey";

    @Test
    public void testExplicitPropertyMappings()
    {
        Map<String, String> properties = new ImmutableMap.Builder<String, String>()
                .put("vault_address", VAULT_ADDR)
                .put("vault_token", VAULT_TOKEN)
                .put("vault_secret_keys", VAULT_SECRET_KEYS).build();

        VaultSecretsManagerConfig expected = new VaultSecretsManagerConfig()
                .setSecretsKeys(VAULT_SECRET_KEYS)
                .setVaultAddress(VAULT_ADDR)
                .setVaultToken(VAULT_TOKEN);
        assertFullMapping(properties, expected);
    }

    public static boolean isEnabled()
    {
        return System.getenv("VAULT_ADDR") != null && System.getenv("VAULT_TOKEN") != null && System.getenv("VAULT_SECRET_KEYS") != null;
    }
}
