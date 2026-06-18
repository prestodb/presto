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
package com.facebook.presto.password.file;

import com.facebook.airlift.bootstrap.Bootstrap;
import com.facebook.presto.spi.security.PasswordAuthenticator;
import com.facebook.presto.spi.security.PasswordAuthenticatorFactory;
import com.google.inject.Injector;
import com.google.inject.Scopes;

import java.util.Map;

import static com.facebook.airlift.configuration.ConfigBinder.configBinder;
import static com.google.common.base.Throwables.throwIfUnchecked;

public class FileAuthenticatorFactory
        implements PasswordAuthenticatorFactory
{
    @Override
    public String getName()
    {
        return "file";
    }

    @Override
    public PasswordAuthenticator create(Map<String, String> config)
    {
        try {
            Bootstrap app = new Bootstrap(
                    binder -> {
                        configBinder(binder).bindConfig(FileConfig.class);
                        binder.bind(FileAuthenticator.class).in(Scopes.SINGLETON);
                    });

            Injector injector = app
                    .doNotInitializeLogging()
                    .setRequiredConfigurationProperties(config)
                    .initialize();

            return injector.getInstance(FileAuthenticator.class);
        }
        catch (Exception e) {
            throwIfUnchecked(e);
            throw new RuntimeException(e);
        }
    }
}
