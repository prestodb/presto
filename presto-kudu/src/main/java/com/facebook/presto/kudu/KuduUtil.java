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
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.kudu.client.KuduClient;

import java.io.IOException;
import java.security.PrivilegedExceptionAction;

public class KuduUtil
{
    private static final Logger log = Logger.get(KuduUtil.class);

    private KuduUtil()
    {
        // not allowed to be called to initialize instance
    }

    /**
     * Initialize kerberos authentication
     */
    static void initKerberosENV(String principal, String keytab, boolean debugEnabled)
    {
        try {
            Configuration conf = new Configuration();
            conf.set("hadoop.security.authentication", "kerberos");
            if (debugEnabled) {
                System.setProperty("sun.security.krb5.debug", "true");
            }
            UserGroupInformation.setConfiguration(conf);
            UserGroupInformation.loginUserFromKeytab(principal, keytab);
            log.info("Connecting to kudu with kerberos authentication");
            log.info("Current user: " + UserGroupInformation.getCurrentUser());
            log.info("Login user: " + UserGroupInformation.getLoginUser());
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    static KuduClient createKuduKerberosClient(KuduClientConfig config)
    {
        KuduClient client = null;
        try {
            reTryKerberos(true);
            client = UserGroupInformation.getLoginUser().doAs(
                    (PrivilegedExceptionAction<KuduClient>) () -> createKuduClient(config));
            return client;
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    static KuduClient createKuduClient(KuduClientConfig config)
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

    static void reTryKerberos(boolean enabled)
    {
        if (enabled) {
            log.debug("Try relogin kerberos at first!");
            try {
                if (UserGroupInformation.isLoginKeytabBased()) {
                    UserGroupInformation.getLoginUser().reloginFromKeytab();
                }
                else if (UserGroupInformation.isLoginTicketBased()) {
                    UserGroupInformation.getLoginUser().reloginFromTicketCache();
                }
            }
            catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }
}
