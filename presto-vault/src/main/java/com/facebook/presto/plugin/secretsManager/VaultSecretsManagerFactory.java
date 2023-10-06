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

import com.facebook.airlift.bootstrap.Bootstrap;
import com.facebook.airlift.log.Logger;
import com.facebook.presto.spi.secretsManager.SecretsManager;
import com.facebook.presto.spi.secretsManager.SecretsManagerFactory;
import com.google.inject.Injector;
import com.google.inject.Scopes;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static com.facebook.airlift.configuration.ConfigBinder.configBinder;
import static com.google.common.base.Throwables.throwIfUnchecked;

public class VaultSecretsManagerFactory
        implements SecretsManagerFactory
{
    private final String name;

    private static final Logger log = Logger.get(VaultSecretsManagerFactory.class);

    public VaultSecretsManagerFactory(String name)
    {
        this.name = name;
    }

    @Override
    public String getName()
    {
        return name;
    }

    @Override
    public SecretsManager create()
    {
        final Map<String, String> config = new HashMap<>();
        try {
            config.put("vault_address", Optional.ofNullable(System.getenv("VAULT_ADDR")).orElseThrow(
                    () -> new Exception("vault_address is not set in the environment")));
            config.put("vault_token", Optional.ofNullable(System.getenv("VAULT_TOKEN")).orElseThrow(
                    () -> new Exception("vault_token is not set in the environment")));
            config.put("vault_secret_keys", Optional.ofNullable(System.getenv("VAULT_SECRET_KEYS")).orElseThrow(
                    () -> new Exception("vault_secret_keys is not set in the environment")));
        }
        catch (Exception e) {
            log.error(e.getMessage());
            throw new RuntimeException(e);
        }

        try {
            Bootstrap app = new Bootstrap(
                    binder -> {
                        binder.bind(VaultSecretsManager.class).in(Scopes.SINGLETON);
                        configBinder(binder).bindConfig(VaultSecretsManagerConfig.class);
                    });

            Injector injector = app
                    .doNotInitializeLogging()
                    .quiet() // Do not log configuration
                    .setRequiredConfigurationProperties(config)
                    .initialize();

            return injector.getInstance(VaultSecretsManager.class);
        }
        catch (Exception e) {
            throwIfUnchecked(e);
            throw new RuntimeException(e);
        }
    }
}
