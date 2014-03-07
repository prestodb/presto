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
package com.facebook.presto.hive.util;

import io.airlift.log.Logger;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;

import java.io.IOException;
import java.security.PrivilegedAction;
import java.security.PrivilegedExceptionAction;

/**
 * Kerberos authentication
 */
public class SecurityUtils {

    private static final Logger log = Logger.get(SecurityUtils.class);

    /**
     * kerberos login
     *
     * @param principalConf etc. hadoop-data/_HOST@SANKUAI.COM
     * @param keytabFile    keytab file
     * @return the login UserGroupInformation
     *
     * @throws IOException
     */
    public static UserGroupInformation login(String principalConf, String keytabFile) throws IOException {
        if (UserGroupInformation.isSecurityEnabled()) {
            String kerberosName = SecurityUtil.getServerPrincipal(principalConf, "0.0.0.0");
            return UserGroupInformation.loginUserFromKeytabAndReturnUGI(kerberosName, keytabFile);
        }
        return null;
    }

    /**
     * Run the given action as the user.
     *
     * @param ugi
     * @param action
     * @param <T>
     * @return
     * @see org.apache.hadoop.security.UserGroupInformation#doAs(java.security.PrivilegedExceptionAction)
     */
    public static <T> T doAs(UserGroupInformation ugi, PrivilegedExceptionAction<T> action) throws Exception {
        return ugi != null ? ugi.doAs(action) : action.run();
    }

    /**
     * Run the given action as the user.
     *
     * @param ugi
     * @param action
     * @param <T>
     * @return
     * @see org.apache.hadoop.security.UserGroupInformation#doAs(java.security.PrivilegedAction)
     */
    public static <T> T doAs(UserGroupInformation ugi, PrivilegedAction<T> action) {
        return ugi != null ? ugi.doAs(action) : action.run();
    }

}
