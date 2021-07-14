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
package com.facebook.presto.tablestore;

import com.facebook.airlift.configuration.Config;

public class TablestoreConfig
{
    private String endpoint;
    private String accessKeyId;
    private String accessKeySecret;
    private String instance;
    private int clientRetrySeconds = 120;
    private int connectTimeoutSeconds = 10;
    private int socketTimeoutSeconds = 50;
    private int syncClientWaitTimeoutSeconds = 60;

    public String getEndpoint()
    {
        return endpoint;
    }

    public String getAccessKeyId()
    {
        return accessKeyId;
    }

    public String getAccessKeySecret()
    {
        return accessKeySecret;
    }

    public String getInstance()
    {
        return instance;
    }

    public int getClientRetrySeconds()
    {
        return clientRetrySeconds;
    }

    public int getConnectTimeoutSeconds()
    {
        return connectTimeoutSeconds;
    }

    public int getSocketTimeoutSeconds()
    {
        return socketTimeoutSeconds;
    }

    public int getSyncClientWaitTimeoutSeconds()
    {
        return syncClientWaitTimeoutSeconds;
    }

    @Config("tablestore.endpoint")
    public void setEndpoint(String endpoint)
    {
        this.endpoint = endpoint;
    }

    @Config("tablestore.access-key-id")
    public void setAccessKeyId(String accessKeyId)
    {
        this.accessKeyId = accessKeyId;
    }

    @Config("tablestore.access-key-secret")
    public void setAccessKeySecret(String acccessKeySecret)
    {
        this.accessKeySecret = acccessKeySecret;
    }

    @Config("tablestore.instance")
    public void setInstance(String instance)
    {
        this.instance = instance;
    }

    @Config("tablestore.client-retry-seconds")
    public void setClientRetrySeconds(int clientRetrySeconds)
    {
        this.clientRetrySeconds = clientRetrySeconds;
    }

    @Config("tablestore.connect-timeout-seconds")
    public void setConnectTimeoutSeconds(int connectTimeoutSeconds)
    {
        this.connectTimeoutSeconds = connectTimeoutSeconds;
    }

    @Config("tablestore.socket-timeout-seconds")
    public void setSocketTimeoutSeconds(int socketTimeoutSeconds)
    {
        this.socketTimeoutSeconds = socketTimeoutSeconds;
    }

    @Config("tablestore.sync-client-wait-timeout-seconds")
    public void setSyncClientWaitTimeoutSeconds(int syncClientWaitTimeoutSeconds)
    {
        this.syncClientWaitTimeoutSeconds = syncClientWaitTimeoutSeconds;
    }
}
