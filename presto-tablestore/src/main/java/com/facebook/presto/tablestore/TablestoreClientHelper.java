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

import com.alicloud.openservices.tablestore.ClientConfiguration;
import com.alicloud.openservices.tablestore.SyncClient;
import com.alicloud.openservices.tablestore.core.auth.DefaultCredentials;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ExecutorService;

public class TablestoreClientHelper
{
    private TablestoreClientHelper()
    {
    }

    public class Credentials
    {
        private String accessKeyId;
        private String accessKeySecret;

        public Credentials(String accessKeyId, String accessKeySecret)
        {
            this.accessKeyId = accessKeyId;
            this.accessKeySecret = accessKeySecret;
        }

        public String getAccessKeyId()
        {
            return accessKeyId;
        }

        public void setAccessKeyId(String accessKeyId)
        {
            this.accessKeyId = accessKeyId;
        }

        public String getAccessKeySecret()
        {
            return accessKeySecret;
        }

        public void setAccessKeySecret(String accessKeySecret)
        {
            this.accessKeySecret = accessKeySecret;
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            Credentials that = (Credentials) o;
            return Objects.equals(accessKeyId, that.accessKeyId)
                    && Objects.equals(accessKeySecret, that.accessKeySecret);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(accessKeyId, accessKeySecret);
        }
    }

    public interface CredentialProvider
    {
        Credentials get();
    }

    protected static SyncClient createSyncClient(TablestoreConfig config, Optional<ExecutorService> callback)
    {
        ClientConfiguration configuration = new ClientConfiguration();
        configuration.setRetryStrategy(new TimeoutRetryStrategy(config.getClientRetrySeconds()));
        configuration.setConnectionTimeoutInMillisecond(config.getConnectTimeoutSeconds() * 1000);
        configuration.setSocketTimeoutInMillisecond(config.getSocketTimeoutSeconds() * 1000);
        configuration.setSyncClientWaitFutureTimeoutInMillis(config.getSyncClientWaitTimeoutSeconds() * 1000);

        SyncClient client = new SyncClient(config.getEndpoint(), config.getAccessKeyId(), config.getAccessKeySecret(), config.getInstance(), configuration);
        client.setPrepareCallback(() -> {
            client.setCredentials(new DefaultCredentials(config.getAccessKeyId(), config.getAccessKeySecret()));
        });

        Map<String, String> extraHeaders = new HashMap<>();
        client.setExtraHeaders(extraHeaders);

        return client;
    }
}
