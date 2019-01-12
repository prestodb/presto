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
package io.prestosql.server;

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigSecuritySensitive;

public class InternalCommunicationConfig
{
    public static final String INTERNAL_COMMUNICATION_KERBEROS_ENABLED = "internal-communication.kerberos.enabled";

    private boolean httpsRequired;
    private String keyStorePath;
    private String keyStorePassword;
    private boolean kerberosEnabled;
    private boolean kerberosUseCanonicalHostname = true;

    public boolean isHttpsRequired()
    {
        return httpsRequired;
    }

    @Config("internal-communication.https.required")
    public InternalCommunicationConfig setHttpsRequired(boolean httpsRequired)
    {
        this.httpsRequired = httpsRequired;
        return this;
    }

    public String getKeyStorePath()
    {
        return keyStorePath;
    }

    @Config("internal-communication.https.keystore.path")
    public InternalCommunicationConfig setKeyStorePath(String keyStorePath)
    {
        this.keyStorePath = keyStorePath;
        return this;
    }

    public String getKeyStorePassword()
    {
        return keyStorePassword;
    }

    @Config("internal-communication.https.keystore.key")
    @ConfigSecuritySensitive
    public InternalCommunicationConfig setKeyStorePassword(String keyStorePassword)
    {
        this.keyStorePassword = keyStorePassword;
        return this;
    }

    public boolean isKerberosEnabled()
    {
        return kerberosEnabled;
    }

    @Config(INTERNAL_COMMUNICATION_KERBEROS_ENABLED)
    public InternalCommunicationConfig setKerberosEnabled(boolean kerberosEnabled)
    {
        this.kerberosEnabled = kerberosEnabled;
        return this;
    }

    public boolean isKerberosUseCanonicalHostname()
    {
        return kerberosUseCanonicalHostname;
    }

    @Config("internal-communication.kerberos.use-canonical-hostname")
    public InternalCommunicationConfig setKerberosUseCanonicalHostname(boolean kerberosUseCanonicalHostname)
    {
        this.kerberosUseCanonicalHostname = kerberosUseCanonicalHostname;
        return this;
    }
}
