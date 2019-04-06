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
package com.facebook.presto.dynamicCatalog;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Map;

public class CatalogVo
{
    @JsonProperty("catalogName")
    public String catalogName;
    @JsonProperty("connectorName")
    public String connectorName;
    @JsonProperty("properties")
    public Map<String, String> properties;

    public CatalogVo(String catalogName, String connectorName, Map<String, String> properties)
    {
        this.catalogName = catalogName;
        this.connectorName = connectorName;
        this.properties = properties;
    }

    public CatalogVo()
    {
    }

    public String getCatalogName()
    {
        return catalogName;
    }

    public void setCatalogName(String catalogName)
    {
        this.catalogName = catalogName;
    }

    public String getConnectorName()
    {
        return connectorName;
    }

    public void setConnectorName(String connectorName)
    {
        this.connectorName = connectorName;
    }

    public Map<String, String> getProperties()
    {
        return properties;
    }

    public void setProperties(Map<String, String> properties)
    {
        this.properties = properties;
    }

    @Override
    public String toString()
    {
        return "CatalogVo{" +
                "catalogName='" + catalogName + '\'' +
                ", connectorName='" + connectorName + '\'' +
                ", properties=" + properties +
                '}';
    }
}
