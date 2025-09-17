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
package com.facebook.presto.server;

import com.facebook.airlift.configuration.Config;
import com.facebook.airlift.configuration.ConfigDescription;
import com.facebook.airlift.configuration.ConfigSecuritySensitive;
import com.facebook.airlift.units.DataSize;
import com.facebook.drift.transport.netty.codec.Protocol;
import jakarta.validation.constraints.AssertTrue;
import jakarta.validation.constraints.NotNull;

import java.util.Optional;

import static com.facebook.airlift.units.DataSize.Unit.MEGABYTE;

public class InternalCommunicationConfig
{
    public static final String INTERNAL_COMMUNICATION_KERBEROS_ENABLED = "internal-communication.kerberos.enabled";

    private boolean httpsRequired;
    private String keyStorePath;
    private String keyStorePassword;
    private String trustStorePath;
    private String trustStorePassword;
    private Optional<String> excludeCipherSuites = Optional.empty();
    private Optional<String> includedCipherSuites = Optional.empty();
    private boolean kerberosEnabled;
    private boolean kerberosUseCanonicalHostname = true;
    private boolean binaryTransportEnabled;
    private boolean thriftTransportEnabled;
    private boolean taskInfoThriftTransportEnabled;
    private boolean taskUpdateRequestThriftSerdeEnabled;
    private boolean taskInfoResponseThriftSerdeEnabled;
    private Protocol thriftProtocol = Protocol.BINARY;
    private DataSize maxTaskUpdateSize = new DataSize(16, MEGABYTE);
    private CommunicationProtocol taskCommunicationProtocol = CommunicationProtocol.HTTP;
    private CommunicationProtocol serverInfoCommunicationProtocol = CommunicationProtocol.HTTP;
    private boolean memoizeDeadNodesEnabled;
    private String sharedSecret;
    private long nodeStatsRefreshIntervalMillis = 1_000;
    private long nodeDiscoveryPollingIntervalMillis = 5_000;

    private boolean internalJwtEnabled;

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

    public String getTrustStorePath()
    {
        return trustStorePath;
    }

    @Config("internal-communication.https.trust-store-path")
    public InternalCommunicationConfig setTrustStorePath(String trustStorePath)
    {
        this.trustStorePath = trustStorePath;
        return this;
    }

    public String getTrustStorePassword()
    {
        return trustStorePassword;
    }

    @Config("internal-communication.https.trust-store-password")
    @ConfigSecuritySensitive
    public InternalCommunicationConfig setTrustStorePassword(String trustStorePassword)
    {
        this.trustStorePassword = trustStorePassword;
        return this;
    }

    public Optional<String> getIncludedCipherSuites()
    {
        return includedCipherSuites;
    }

    @Config("internal-communication.https.included-cipher")
    public InternalCommunicationConfig setIncludedCipherSuites(String includedCipherSuites)
    {
        this.includedCipherSuites = Optional.ofNullable(includedCipherSuites);
        return this;
    }

    public Optional<String> getExcludeCipherSuites()
    {
        return excludeCipherSuites;
    }

    @Config("internal-communication.https.excluded-cipher")
    public InternalCommunicationConfig setExcludeCipherSuites(String excludeCipherSuites)
    {
        this.excludeCipherSuites = Optional.ofNullable(excludeCipherSuites);
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

    public boolean isBinaryTransportEnabled()
    {
        return binaryTransportEnabled;
    }

    @Config("experimental.internal-communication.binary-transport-enabled")
    @ConfigDescription("Enables smile encoding support for coordinator-to-worker communication")
    public InternalCommunicationConfig setBinaryTransportEnabled(boolean binaryTransportEnabled)
    {
        this.binaryTransportEnabled = binaryTransportEnabled;
        return this;
    }

    public boolean isThriftTransportEnabled()
    {
        return thriftTransportEnabled;
    }

    @Config("experimental.internal-communication.thrift-transport-enabled")
    @ConfigDescription("Enables thrift encoding support for internal communication")
    public InternalCommunicationConfig setThriftTransportEnabled(boolean thriftTransportEnabled)
    {
        this.thriftTransportEnabled = thriftTransportEnabled;
        return this;
    }

    public boolean isTaskUpdateRequestThriftSerdeEnabled()
    {
        return taskUpdateRequestThriftSerdeEnabled;
    }

    @Config("experimental.internal-communication.task-update-request-thrift-serde-enabled")
    @ConfigDescription("Enables thrift encoding support for Task Update Request")
    public InternalCommunicationConfig setTaskUpdateRequestThriftSerdeEnabled(boolean taskUpdateRequestThriftSerdeEnabled)
    {
        this.taskUpdateRequestThriftSerdeEnabled = taskUpdateRequestThriftSerdeEnabled;
        return this;
    }

    public boolean isTaskInfoResponseThriftSerdeEnabled()
    {
        return taskInfoResponseThriftSerdeEnabled;
    }

    @Config("experimental.internal-communication.task-info-response-thrift-serde-enabled")
    @ConfigDescription("Enables thrift encoding support for Task Info Response")
    public InternalCommunicationConfig setTaskInfoResponseThriftSerdeEnabled(boolean taskInfoResponseThriftSerdeEnabled)
    {
        this.taskInfoResponseThriftSerdeEnabled = taskInfoResponseThriftSerdeEnabled;
        return this;
    }

    public boolean isTaskInfoThriftTransportEnabled()
    {
        return taskInfoThriftTransportEnabled;
    }

    @Config("experimental.internal-communication.task-info-thrift-transport-enabled")
    @ConfigDescription("Enables thrift encoding support for Task Info")
    public InternalCommunicationConfig setTaskInfoThriftTransportEnabled(boolean taskInfoThriftTransportEnabled)
    {
        this.taskInfoThriftTransportEnabled = taskInfoThriftTransportEnabled;
        return this;
    }

    public Protocol getThriftProtocol()
    {
        return thriftProtocol;
    }

    @Config("experimental.internal-communication.thrift-transport-protocol")
    @ConfigDescription("Thrift encoding type for internal communication")
    public InternalCommunicationConfig setThriftProtocol(Protocol thriftProtocol)
    {
        this.thriftProtocol = thriftProtocol;
        return this;
    }

    public DataSize getMaxTaskUpdateSize()
    {
        return maxTaskUpdateSize;
    }

    @Config("experimental.internal-communication.max-task-update-size")
    @ConfigDescription("Enables limit on the size of the task update")
    public InternalCommunicationConfig setMaxTaskUpdateSize(DataSize maxTaskUpdateSize)
    {
        this.maxTaskUpdateSize = maxTaskUpdateSize;
        return this;
    }

    public enum CommunicationProtocol
    {
        HTTP,
        THRIFT
    }

    public CommunicationProtocol getTaskCommunicationProtocol()
    {
        return taskCommunicationProtocol;
    }

    @Config("internal-communication.task-communication-protocol")
    @ConfigDescription("Set task communication protocol")
    public InternalCommunicationConfig setTaskCommunicationProtocol(CommunicationProtocol taskCommunicationProtocol)
    {
        this.taskCommunicationProtocol = taskCommunicationProtocol;
        return this;
    }

    public CommunicationProtocol getServerInfoCommunicationProtocol()
    {
        return serverInfoCommunicationProtocol;
    }

    @Config("internal-communication.server-info-communication-protocol")
    @ConfigDescription("Set server info communication protocol to broadcast state info")
    public InternalCommunicationConfig setServerInfoCommunicationProtocol(CommunicationProtocol serverInfoCommunicationProtocol)
    {
        this.serverInfoCommunicationProtocol = serverInfoCommunicationProtocol;
        return this;
    }

    public boolean isMemoizeDeadNodesEnabled()
    {
        return memoizeDeadNodesEnabled;
    }

    @Config("internal-communication.memoize-dead-nodes-enabled")
    @ConfigDescription("Enables memoizing dead nodes in discovery node manger")
    public InternalCommunicationConfig setMemoizeDeadNodesEnabled(boolean memoizeDeadNodesEnabled)
    {
        this.memoizeDeadNodesEnabled = memoizeDeadNodesEnabled;
        return this;
    }

    @NotNull
    public Optional<String> getSharedSecret()
    {
        return Optional.ofNullable(sharedSecret);
    }

    @ConfigSecuritySensitive
    @Config("internal-communication.shared-secret")
    public InternalCommunicationConfig setSharedSecret(String sharedSecret)
    {
        this.sharedSecret = sharedSecret;
        return this;
    }

    public long getNodeStatsRefreshIntervalMillis()
    {
        return nodeStatsRefreshIntervalMillis;
    }

    @Config("internal-communication.node-stats-refresh-interval-millis")
    @ConfigDescription("Interval in milliseconds for refreshing node statistics")
    public InternalCommunicationConfig setNodeStatsRefreshIntervalMillis(long nodeStatsRefreshIntervalMillis)
    {
        this.nodeStatsRefreshIntervalMillis = nodeStatsRefreshIntervalMillis;
        return this;
    }

    public long getNodeDiscoveryPollingIntervalMillis()
    {
        return nodeDiscoveryPollingIntervalMillis;
    }

    @Config("internal-communication.node-discovery-polling-interval-millis")
    @ConfigDescription("Interval in milliseconds for polling node discovery and refreshing node states")
    public InternalCommunicationConfig setNodeDiscoveryPollingIntervalMillis(long nodeDiscoveryPollingIntervalMillis)
    {
        this.nodeDiscoveryPollingIntervalMillis = nodeDiscoveryPollingIntervalMillis;
        return this;
    }

    public boolean isInternalJwtEnabled()
    {
        return internalJwtEnabled;
    }

    @Config("internal-communication.jwt.enabled")
    public InternalCommunicationConfig setInternalJwtEnabled(boolean internalJwtEnabled)
    {
        this.internalJwtEnabled = internalJwtEnabled;
        return this;
    }

    @AssertTrue(message = "When internal JWT(internal-communication.jwt.enabled) authentication is enabled, a shared secret(internal-communication.shared-secret) is required")
    public boolean isRequiredSharedSecretSet()
    {
        return !isInternalJwtEnabled() || getSharedSecret().isPresent();
    }
}
