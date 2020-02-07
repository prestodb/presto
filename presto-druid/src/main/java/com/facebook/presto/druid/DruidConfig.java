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

import javax.validation.constraints.NotNull;

public class DruidConfig
{
    private String coordinatorUrl;
    private String brokerUrl;
    private String schema = "druid";

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
}
