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
package com.facebook.presto.druid;

import com.facebook.airlift.configuration.Config;
import com.facebook.airlift.configuration.ConfigDescription;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;

import javax.annotation.Nullable;
import javax.validation.constraints.NotNull;

import java.util.List;

public class DruidConfig
{
    private String coordinatorUrl;
    private String brokerUrl;
    private String schema = "druid";
    private boolean pushdown;
    private List<String> hadoopConfiguration = ImmutableList.of();
    private DruidAuthenticationType druidAuthenticationType = DruidAuthenticationType.NONE;
    private String basicAuthenticationUsername;
    private String basicAuthenticationPassword;

    public enum DruidAuthenticationType
    {
        NONE,
        BASIC,
        KERBEROS
    }

    @NotNull
    public String getDruidCoordinatorUrl()
    {
        return coordinatorUrl;
    }

    @Config("druid.coordinator-url")
    @ConfigDescription("druid coordinator Url")
    public DruidConfig setDruidCoordinatorUrl(String coordinatorUrl)
    {
        this.coordinatorUrl = coordinatorUrl;
        return this;
    }

    @NotNull
    public String getDruidBrokerUrl()
    {
        return brokerUrl;
    }

    @Config("druid.broker-url")
    @ConfigDescription("druid broker Url")
    public DruidConfig setDruidBrokerUrl(String brokerUrl)
    {
        this.brokerUrl = brokerUrl;
        return this;
    }

    @NotNull
    public String getDruidSchema()
    {
        return schema;
    }

    @Config("druid.schema-name")
    @ConfigDescription("druid schema name")
    public DruidConfig setDruidSchema(String schema)
    {
        this.schema = schema;
        return this;
    }

    public boolean isComputePushdownEnabled()
    {
        return pushdown;
    }

    @Config("druid.compute-pushdown-enabled")
    @ConfigDescription("pushdown query processing to druid")
    public DruidConfig setComputePushdownEnabled(boolean pushdown)
    {
        this.pushdown = pushdown;
        return this;
    }

    @NotNull
    public List<String> getHadoopConfiguration()
    {
        return hadoopConfiguration;
    }

    @Config("druid.hadoop.config.resources")
    public DruidConfig setHadoopConfiguration(String files)
    {
        if (files != null) {
            this.hadoopConfiguration = Splitter.on(',').trimResults().omitEmptyStrings().splitToList(files);
        }
        return this;
    }

    public DruidConfig setHadoopConfiguration(List<String> files)
    {
        if (files != null) {
            this.hadoopConfiguration = ImmutableList.copyOf(files);
        }
        return this;
    }

    @NotNull
    public DruidAuthenticationType getDruidAuthenticationType()
    {
        return druidAuthenticationType;
    }

    @Config("druid.authentication.type")
    public DruidConfig setDruidAuthenticationType(DruidAuthenticationType druidAuthenticationType)
    {
        if (druidAuthenticationType != null) {
            this.druidAuthenticationType = druidAuthenticationType;
        }
        return this;
    }

    @Nullable
    public String getBasicAuthenticationUsername()
    {
        return basicAuthenticationUsername;
    }

    @Config("druid.basic.authentication.username")
    public DruidConfig setBasicAuthenticationUsername(String basicAuthenticationUsername)
    {
        this.basicAuthenticationUsername = basicAuthenticationUsername;
        return this;
    }

    @Nullable
    public String getBasicAuthenticationPassword()
    {
        return basicAuthenticationPassword;
    }

    @Config("druid.basic.authentication.password")
    public DruidConfig setBasicAuthenticationPassword(String basicAuthenticationPassword)
    {
        this.basicAuthenticationPassword = basicAuthenticationPassword;
        return this;
    }
}
