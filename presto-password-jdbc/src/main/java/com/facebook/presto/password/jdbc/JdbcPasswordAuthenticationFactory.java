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
package com.facebook.presto.password.jdbc;

import com.facebook.presto.spi.security.PasswordAuthenticator;
import com.facebook.presto.spi.security.PasswordAuthenticatorFactory;
import com.google.inject.Injector;
import com.google.inject.Scopes;
import io.airlift.bootstrap.Bootstrap;

import java.util.Map;

import static com.google.common.base.Throwables.throwIfUnchecked;
import static io.airlift.configuration.ConfigBinder.configBinder;

public class JdbcPasswordAuthenticationFactory
        implements PasswordAuthenticatorFactory
{
    @Override
    public String getName()
    {
        return "jdbc";
    }

    @Override
    public PasswordAuthenticator create(Map<String, String> config)
    {
        try {
            Bootstrap app = new Bootstrap(
                    binder -> {
                        configBinder(binder).bindConfig(JdbcConfig.class);
                        binder.bind(Datastore.class).to(PostgresDatastore.class);
                        binder.bind(JdbcPasswordAuthenticator.class).in(Scopes.SINGLETON);
                    });

            Injector injector = app
                    .strictConfig()
                    .doNotInitializeLogging()
                    .setRequiredConfigurationProperties(config)
                    .initialize();

            return injector.getInstance(JdbcPasswordAuthenticator.class);
        }
        catch (Exception e) {
            throwIfUnchecked(e);
            throw new RuntimeException(e);
        }
    }
}
