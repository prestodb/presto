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

import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableSet;
import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import io.airlift.configuration.DefunctConfig;

import javax.validation.constraints.NotNull;

import java.util.Set;

import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static com.google.common.collect.Streams.stream;

@DefunctConfig("http.server.authentication.enabled")
public class SecurityConfig
{
    private static final Splitter SPLITTER = Splitter.on(',').trimResults().omitEmptyStrings();

    private Set<AuthenticationType> authenticationTypes = ImmutableSet.of();

    public enum AuthenticationType
    {
        CERTIFICATE,
        KERBEROS,
        LDAP
    }

    @NotNull
    public Set<AuthenticationType> getAuthenticationTypes()
    {
        return authenticationTypes;
    }

    public SecurityConfig setAuthenticationTypes(Set<AuthenticationType> authenticationTypes)
    {
        this.authenticationTypes = ImmutableSet.copyOf(authenticationTypes);
        return this;
    }

    @Config("http-server.authentication.type")
    @ConfigDescription("Authentication types (supported types: CERTIFICATE, KERBEROS, LDAP)")
    public SecurityConfig setAuthenticationTypes(String types)
    {
        if (types == null) {
            authenticationTypes = null;
            return this;
        }

        authenticationTypes = stream(SPLITTER.split(types))
                .map(AuthenticationType::valueOf)
                .collect(toImmutableSet());
        return this;
    }
}
