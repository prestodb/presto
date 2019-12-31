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

import com.facebook.airlift.log.Logger;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.kudu.client.KuduClient;

import java.io.IOException;
import java.security.PrivilegedExceptionAction;

public class KuduKerberosUtil
{
    private static final Logger log = Logger.get(KuduKerberosUtil.class);

    private KuduKerberosUtil()
    {
        //not allowed to be called to initialize instance
    }

    /**
     * Initialize kerberos authentication
     */
    static void initKerberosENV(String principal, String keytab)
    {
        try {
            org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration();
            conf.set("hadoop.security.authentication", "Kerberos");
            UserGroupInformation.setConfiguration(conf);
            UserGroupInformation.loginUserFromKeytab(principal, keytab);
            log.warn("getting connection from kudu with kerberos");
            log.warn("----------current user: " + UserGroupInformation.getCurrentUser() + "----------");
            log.warn("----------login user: " + UserGroupInformation.getLoginUser() + "----------");
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }

    static KuduClient getKuduClient(KuduClientConfig config)
    {
        KuduClient client = null;
        try {
            client = UserGroupInformation.getLoginUser().doAs(
                    new PrivilegedExceptionAction<KuduClient>() {
                        @Override
                        public KuduClient run() throws Exception
                        {
                            KuduClient.KuduClientBuilder builder = new KuduClient.KuduClientBuilder(config.getMasterAddresses());
                            builder.defaultAdminOperationTimeoutMs(config.getDefaultAdminOperationTimeout().toMillis());
                            builder.defaultOperationTimeoutMs(config.getDefaultOperationTimeout().toMillis());
                            builder.defaultSocketReadTimeoutMs(config.getDefaultSocketReadTimeout().toMillis());
                            if (config.isDisableStatistics()) {
                                builder.disableStatistics();
                            }
                            return builder.build();
                        }
                    });
        }
        catch (IOException | InterruptedException e) {
            log.error(e.getMessage());
        }
        return client;
    }
}
