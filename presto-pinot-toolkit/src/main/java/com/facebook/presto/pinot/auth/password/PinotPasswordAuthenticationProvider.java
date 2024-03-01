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
package com.facebook.presto.pinot.auth.password;

import com.facebook.airlift.log.Logger;
import com.facebook.presto.pinot.PinotSessionProperties;
import com.facebook.presto.pinot.auth.PinotAuthenticationProvider;
import com.facebook.presto.spi.ConnectorSession;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Base64;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class PinotPasswordAuthenticationProvider
        implements PinotAuthenticationProvider
{
    private static final Logger log = Logger.get(PinotPasswordAuthenticationProvider.class);

    private final Optional<String> authToken;
    private final Method getAuthenticationUserMethod;
    private final Method getAuthenticationPasswordMethod;

    public PinotPasswordAuthenticationProvider(String user, String password, Method getControllerAuthenticationUser, Method getControllerAuthenticationPassword)
    {
        requireNonNull(user, "user is null");
        requireNonNull(password, "password is null");
        this.authToken = Optional.of(encode(user, password));
        this.getAuthenticationUserMethod = getControllerAuthenticationUser;
        this.getAuthenticationPasswordMethod = getControllerAuthenticationPassword;
    }

    @Override
    public Optional<String> getAuthenticationToken()
    {
        return authToken;
    }

    @Override
    public Optional<String> getAuthenticationToken(ConnectorSession connectorSession)
    {
        if (connectorSession != null) {
            try {
                String user = (String) getAuthenticationUserMethod.invoke(PinotSessionProperties.class, connectorSession);
                String password = (String) getAuthenticationPasswordMethod.invoke(PinotSessionProperties.class, connectorSession);
                return Optional.of(encode(user, password));
            }
            catch (IllegalAccessException | InvocationTargetException e) {
                log.warn("Failed to extract auth from session configs, use empty auth");
                return Optional.empty();
            }
        }
        return authToken;
    }

    private String encode(String username, String password)
    {
        String valueToEncode = username + ":" + password;
        return Base64.getEncoder().encodeToString(valueToEncode.getBytes());
    }
}
