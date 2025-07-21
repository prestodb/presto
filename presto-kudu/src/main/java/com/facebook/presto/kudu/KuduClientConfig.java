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
package com.facebook.presto.kudu;

import com.facebook.airlift.configuration.Config;
import com.facebook.airlift.units.Duration;
import com.facebook.airlift.units.MaxDuration;
import com.facebook.airlift.units.MinDuration;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Size;

import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Configuration read from etc/catalog/kudu.properties
 */
public class KuduClientConfig
{
    private static final Splitter SPLITTER = Splitter.on(',').trimResults().omitEmptyStrings();

    private List<String> masterAddresses;
    private Duration defaultAdminOperationTimeout = new Duration(30, TimeUnit.SECONDS);
    private Duration defaultOperationTimeout = new Duration(30, TimeUnit.SECONDS);
    private Duration defaultSocketReadTimeout = new Duration(10, TimeUnit.SECONDS);
    private boolean disableStatistics;
    private boolean schemaEmulationEnabled;
    private String schemaEmulationPrefix = "presto::";
    private boolean kerberosAuthEnabled;
    private String kerberosPrincipal;
    private String kerberosKeytab;
    private boolean kerberosAuthDebugEnabled;

    @NotNull
    @Size(min = 1)
    public List<String> getMasterAddresses()
    {
        return masterAddresses;
    }

    @Config("kudu.client.master-addresses")
    public KuduClientConfig setMasterAddresses(String commaSeparatedList)
    {
        this.masterAddresses = SPLITTER.splitToList(commaSeparatedList);
        return this;
    }

    public KuduClientConfig setMasterAddresses(String... contactPoints)
    {
        this.masterAddresses = ImmutableList.copyOf(contactPoints);
        return this;
    }

    @Config("kudu.client.default-admin-operation-timeout")
    public KuduClientConfig setDefaultAdminOperationTimeout(Duration timeout)
    {
        this.defaultAdminOperationTimeout = timeout;
        return this;
    }

    @MinDuration("1s")
    @MaxDuration("1h")
    public Duration getDefaultAdminOperationTimeout()
    {
        return defaultAdminOperationTimeout;
    }

    @Config("kudu.client.default-operation-timeout")
    public KuduClientConfig setDefaultOperationTimeout(Duration timeout)
    {
        this.defaultOperationTimeout = timeout;
        return this;
    }

    @MinDuration("1s")
    @MaxDuration("1h")
    public Duration getDefaultOperationTimeout()
    {
        return defaultOperationTimeout;
    }

    @Config("kudu.client.default-socket-read-timeout")
    public KuduClientConfig setDefaultSocketReadTimeout(Duration timeout)
    {
        this.defaultSocketReadTimeout = timeout;
        return this;
    }

    @MinDuration("1s")
    @MaxDuration("1h")
    public Duration getDefaultSocketReadTimeout()
    {
        return defaultSocketReadTimeout;
    }

    public boolean isDisableStatistics()
    {
        return this.disableStatistics;
    }

    @Config("kudu.client.disable-statistics")
    public KuduClientConfig setDisableStatistics(boolean disableStatistics)
    {
        this.disableStatistics = disableStatistics;
        return this;
    }

    public String getSchemaEmulationPrefix()
    {
        return schemaEmulationPrefix;
    }

    @Config("kudu.schema-emulation.prefix")
    public KuduClientConfig setSchemaEmulationPrefix(String prefix)
    {
        this.schemaEmulationPrefix = prefix;
        return this;
    }

    public boolean isSchemaEmulationEnabled()
    {
        return schemaEmulationEnabled;
    }

    @Config("kudu.schema-emulation.enabled")
    public KuduClientConfig setSchemaEmulationEnabled(boolean enabled)
    {
        this.schemaEmulationEnabled = enabled;
        return this;
    }

    public boolean isKerberosAuthEnabled()
    {
        return kerberosAuthEnabled;
    }

    @Config("kudu.kerberos-auth.enabled")
    public KuduClientConfig setKerberosAuthEnabled(boolean enabled)
    {
        this.kerberosAuthEnabled = enabled;
        return this;
    }

    public String getKerberosPrincipal()
    {
        return kerberosPrincipal;
    }

    @Config("kudu.kerberos-auth.principal")
    public KuduClientConfig setKerberosPrincipal(String principal)
    {
        this.kerberosPrincipal = principal;
        return this;
    }

    public String getKerberosKeytab()
    {
        return kerberosKeytab;
    }

    @Config("kudu.kerberos-auth.keytab")
    public KuduClientConfig setKerberosKeytab(String keytab)
    {
        this.kerberosKeytab = keytab;
        return this;
    }

    public boolean isKerberosAuthDebugEnabled()
    {
        return kerberosAuthDebugEnabled;
    }

    @Config("kudu.kerberos-auth.debug.enabled")
    public KuduClientConfig setKerberosAuthDebugEnabled(boolean enabled)
    {
        this.kerberosAuthDebugEnabled = enabled;
        return this;
    }
}
