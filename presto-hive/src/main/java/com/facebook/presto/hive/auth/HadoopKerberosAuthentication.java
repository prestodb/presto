/*
 * Copyright 2016, Teradata Corp. All rights reserved.
 */

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
package com.facebook.presto.hive.auth;

import com.google.common.base.Throwables;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.security.PrivilegedAction;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;
import static java.util.Optional.empty;
import static org.apache.hadoop.security.SecurityUtil.getServerPrincipal;

public class HadoopKerberosAuthentication
{
    private final String principal;
    private final String keytab;
    private final Configuration configuration;

    private Optional<UserGroupInformation> authenticatedUser = empty();

    public HadoopKerberosAuthentication(String principal, String keytab, Configuration configuration)
    {
        this.principal = requireNonNull(principal, "principal is null");
        this.keytab = requireNonNull(keytab, "keytab is null");
        this.configuration = requireNonNull(configuration, "configuration is null");
        this.configuration.set("hadoop.security.authentication", "kerberos");
    }

    public void authenticate()
    {
        checkKeytabIsReadable();
        try {
            String localResolvedHostName = InetAddress.getLocalHost().getCanonicalHostName();
            String prestoPrincipal = getServerPrincipal(
                    principal,
                    localResolvedHostName
            );
            UserGroupInformation ugi;
            synchronized (UserGroupInformation.class) {
                UserGroupInformation.setConfiguration(configuration);
                ugi = UserGroupInformation
                        .loginUserFromKeytabAndReturnUGI(prestoPrincipal, keytab);
            }
            checkState(ugi.isFromKeytab(), "failed to login user '%s' with keytab '%s'",
                    prestoPrincipal, keytab);
            this.authenticatedUser = Optional.of(ugi);
        }
        catch (IOException e) {
            throw Throwables.propagate(e);
        }
    }

    public UserGroupInformation getUserGroupInformation()
    {
        checkState(authenticatedUser.isPresent(), "user is not yet authenticated");
        UserGroupInformation ugi = authenticatedUser.get();
        try {
            ugi.checkTGTAndReloginFromKeytab();
            return ugi;
        }
        catch (IOException e) {
            throw Throwables.propagate(e);
        }
    }

    public <T> T doAs(PrivilegedAction<T> action)
    {
        return getUserGroupInformation()
                .doAs(action);
    }

    public void doAs(Runnable action)
    {
        getUserGroupInformation()
                .doAs((PrivilegedAction<Object>) () -> {
                    action.run();
                    return null;
                });
    }

    public <T> T doAs(String user, PrivilegedAction<T> action)
    {
        return UserGroupInformation
                .createProxyUser(user, getUserGroupInformation())
                .doAs(action);
    }

    public void doAs(String user, Runnable action)
    {
        UserGroupInformation
                .createProxyUser(user, getUserGroupInformation())
                .doAs((PrivilegedAction<Object>) () -> {
                    action.run();
                    return null;
                });
    }

    private void checkKeytabIsReadable()
    {
        File prestoKeytab = new File(keytab);
        checkState(prestoKeytab.exists() && prestoKeytab.canRead(),
                "keytab file at %s does not exist or it is not readable", keytab);
    }
}
