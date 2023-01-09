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
package com.facebook.presto.pinot.auth;

import com.facebook.presto.pinot.PinotConfig;
import com.facebook.presto.pinot.PinotSessionProperties;
import com.facebook.presto.pinot.auth.none.PinotEmptyAuthenticationProvider;
import com.facebook.presto.pinot.auth.password.PinotPasswordAuthenticationProvider;
import com.facebook.presto.spi.ConnectorSession;

import javax.inject.Inject;

import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class PinotBrokerAuthenticationProvider
        implements PinotAuthenticationProvider
{
    private final PinotAuthenticationProvider delegate;

    @Inject
    public PinotBrokerAuthenticationProvider(PinotConfig pinotConfig)
    {
        String type = pinotConfig.getBrokerAuthenticationType();
        switch (type) {
            case "NONE":
                this.delegate = PinotEmptyAuthenticationProvider.instance();
                break;
            case "PASSWORD":
                try {
                    this.delegate = new PinotPasswordAuthenticationProvider(
                            pinotConfig.getBrokerAuthenticationUser(),
                            pinotConfig.getBrokerAuthenticationPassword(),
                            PinotSessionProperties.class.getMethod("getBrokerAuthenticationUser", ConnectorSession.class),
                            PinotSessionProperties.class.getMethod("getBrokerAuthenticationPassword", ConnectorSession.class));
                }
                catch (NoSuchMethodException e) {
                    throw new RuntimeException("Failed to create Broker auth provider", e);
                }
                break;
            default:
                throw new RuntimeException("Unknown authentication type - " + type);
        }
    }

    private PinotBrokerAuthenticationProvider(PinotAuthenticationProvider delegate)
    {
        this.delegate = requireNonNull(delegate, "Delegate broker authentication provider is required");
    }

    @Override
    public Optional<String> getAuthenticationToken()
    {
        return delegate.getAuthenticationToken();
    }

    @Override
    public Optional<String> getAuthenticationToken(ConnectorSession session)
    {
        return delegate.getAuthenticationToken(session);
    }

    public static PinotBrokerAuthenticationProvider create(PinotAuthenticationProvider delegate)
    {
        return new PinotBrokerAuthenticationProvider(delegate);
    }
}
