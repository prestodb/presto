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

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import io.airlift.configuration.DefunctConfig;

import javax.validation.constraints.NotNull;

@DefunctConfig("http.server.authentication.enabled")
public class SecurityConfig
{
    private AuthenticationType authenticationType = AuthenticationType.NONE;

    public enum AuthenticationType
    {
        NONE,
        KERBEROS,
        LDAP
    }

    @NotNull
    public AuthenticationType getAuthenticationType()
    {
        return authenticationType;
    }

    @Config("http-server.authentication.type")
    @ConfigDescription("Authentication type (supported types: NONE, KERBEROS, LDAP)")
    public SecurityConfig setAuthenticationType(AuthenticationType authenticationType)
    {
        this.authenticationType = authenticationType;
        return this;
    }
}
