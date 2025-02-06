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
package com.facebook.presto.rewriter.password;

import com.facebook.presto.spi.security.PasswordAuthenticator;
import com.facebook.presto.spi.security.PasswordAuthenticatorFactory;

import java.util.Map;

import static java.util.Objects.requireNonNull;

public class PluginPasswordAuthenticatorFactory
        implements PasswordAuthenticatorFactory
{
    @Override
    public String getName()
    {
        return "OPTPLUS_PASSWORD_AUTHENTICATOR";
    }

    @Override
    public PasswordAuthenticator create(Map<String, String> config)
    {
        String optplusUser = requireNonNull(config.get("optplus.username"), "optplus.username, entry missing in password-authenticator.properties");
        String optplusPass = requireNonNull(config.get("optplus.password"), "optplus.password, entry missing in password-authenticator.properties");
        return new PluginPasswordAuthenticator(optplusUser, optplusPass);
    }
}
