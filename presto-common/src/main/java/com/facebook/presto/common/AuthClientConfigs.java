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
package com.facebook.presto.common;

import java.util.Optional;

public class AuthClientConfigs
{
    private final String nodeId;
    private final String keyStorePath;
    private final String keyStorePassword;
    private final String trustStorePath;
    private final String trustStorePassword;
    private final Optional<String> excludeCipherSuites;
    private final Optional<String> includedCipherSuites;
    private final boolean internalJwtEnabled;
    private final Optional<String> sharedSecret;

    public static AuthClientConfigs defaultAuthClientConfigs(String nodeId)
    {
        return new AuthClientConfigs(
                nodeId,
                "",
                "",
                "",
                "",
                Optional.empty(),
                Optional.empty(),
                false,
                Optional.empty());
    }

    public AuthClientConfigs(
            String nodeId,
            String keyStorePath,
            String keyStorePassword,
            String trustStorePath,
            String trustStorePassword,
            Optional<String> excludeCipherSuites,
            Optional<String> includedCipherSuites,
            boolean internalJwtEnabled,
            Optional<String> sharedSecret)
    {
        this.nodeId = nodeId;
        this.keyStorePath = keyStorePath;
        this.keyStorePassword = keyStorePassword;
        this.trustStorePath = trustStorePath;
        this.trustStorePassword = trustStorePassword;
        this.excludeCipherSuites = excludeCipherSuites;
        this.includedCipherSuites = includedCipherSuites;
        this.internalJwtEnabled = internalJwtEnabled;
        this.sharedSecret = sharedSecret;
    }

    public String getNodeId()
    {
        return nodeId;
    }

    public String getKeyStorePath()
    {
        return keyStorePath;
    }

    public String getKeyStorePassword()
    {
        return keyStorePassword;
    }

    public String getTrustStorePath()
    {
        return trustStorePath;
    }

    public String getTrustStorePassword()
    {
        return trustStorePassword;
    }

    public Optional<String> getExcludeCipherSuites()
    {
        return excludeCipherSuites;
    }

    public Optional<String> getIncludedCipherSuites()
    {
        return includedCipherSuites;
    }

    public boolean isInternalJwtEnabled()
    {
        return internalJwtEnabled;
    }

    public Optional<String> getSharedSecret()
    {
        return sharedSecret;
    }
}
