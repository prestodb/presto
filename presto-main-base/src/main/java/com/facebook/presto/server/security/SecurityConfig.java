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
package com.facebook.presto.server.security;

import com.facebook.airlift.configuration.Config;
import com.facebook.airlift.configuration.ConfigDescription;
import com.facebook.airlift.configuration.DefunctConfig;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import jakarta.validation.constraints.NotNull;

import java.util.List;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.Streams.stream;

@DefunctConfig("http.server.authentication.enabled")
public class SecurityConfig
{
    private static final Splitter SPLITTER = Splitter.on(',').trimResults().omitEmptyStrings();

    private List<AuthenticationType> authenticationTypes = ImmutableList.of();
    private boolean allowForwardedHttps;
    private boolean authorizedIdentitySelectionEnabled;

    public enum AuthenticationType
    {
        CERTIFICATE,
        KERBEROS,
        PASSWORD,
        JWT,
        CUSTOM,
        OAUTH2
    }

    @NotNull
    public List<AuthenticationType> getAuthenticationTypes()
    {
        return authenticationTypes;
    }

    public SecurityConfig setAuthenticationTypes(List<AuthenticationType> authenticationTypes)
    {
        this.authenticationTypes = ImmutableList.copyOf(authenticationTypes);
        return this;
    }

    @Config("http-server.authentication.type")
    @ConfigDescription("Authentication types (supported types: CERTIFICATE, KERBEROS, PASSWORD, JWT, CUSTOM, OAUTH2)")
    public SecurityConfig setAuthenticationTypes(String types)
    {
        if (types == null) {
            authenticationTypes = null;
            return this;
        }

        authenticationTypes = stream(SPLITTER.split(types))
                .map(AuthenticationType::valueOf)
                .collect(toImmutableList());
        return this;
    }

    public boolean getAllowForwardedHttps()
    {
        return allowForwardedHttps;
    }

    @Config("http-server.authentication.allow-forwarded-https")
    @ConfigDescription("Allow forwarded HTTPS requests")
    public SecurityConfig setAllowForwardedHttps(boolean allowForwardedHttps)
    {
        this.allowForwardedHttps = allowForwardedHttps;
        return this;
    }

    @Config("permissions.authorized-identity-selection-enabled")
    @ConfigDescription("Authorized identity selection enabled")
    public SecurityConfig setAuthorizedIdentitySelectionEnabled(boolean authorizedIdentitySelectionEnabled)
    {
        this.authorizedIdentitySelectionEnabled = authorizedIdentitySelectionEnabled;
        return this;
    }

    public boolean isAuthorizedIdentitySelectionEnabled()
    {
        return authorizedIdentitySelectionEnabled;
    }
}
