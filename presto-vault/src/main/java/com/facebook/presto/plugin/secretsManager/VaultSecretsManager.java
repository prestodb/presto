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

import com.bettercloud.vault.Vault;
import com.bettercloud.vault.VaultConfig;
import com.bettercloud.vault.VaultException;
import com.bettercloud.vault.response.LogicalResponse;
import com.facebook.airlift.log.Logger;
import com.facebook.presto.spi.secretsManager.SecretsManager;

import javax.inject.Inject;

import java.util.HashMap;
import java.util.Map;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class VaultSecretsManager
        implements SecretsManager
{
    private static final Logger log = Logger.get(VaultSecretsManager.class);
    private final VaultSecretsManagerConfig config;

    @Inject
    VaultSecretsManager(VaultSecretsManagerConfig config)
    {
        this.config = requireNonNull(config, "Required Configuration is null");
    }

    @Override
    public Map<String, Map<String, String>> fetchSecrets()
    {
        checkArgument(config.getVaultAddress() != null, "Secrets Url not configured");
        checkArgument(config.getVaultToken() != null, "Secrets access key not configured");
        checkArgument(config.getSecretsKeys() != null, "Secrets ID not configured");

        log.info("Loading Secrets from Vault");

        final VaultConfig vaultConfig;
        final Vault vault;
        LogicalResponse response;
        Map<String, Map<String, String>> finalMap = new HashMap<>();
        try {
            vaultConfig = new VaultConfig()
                    .address(config.getVaultAddress())
                    .token(config.getVaultToken())
                    .build();

            vault = new Vault(vaultConfig);
            for (String secretID : config.getSecretsKeys().split(",")) {
                response = vault.withRetries(5, 1000)
                        .logical()
                        .read("secret/" + secretID);
                finalMap.put(secretID, response.getData());
            }
        }
        catch (VaultException e) {
            throw new RuntimeException(e);
        }
        return finalMap;
    }
}
