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
package com.facebook.presto.hive.metastore;

import com.facebook.presto.hive.HdfsConfiguration;
import com.facebook.presto.hive.HiveClientConfig;
import com.facebook.presto.hive.auth.HadoopAuthentication;
import com.facebook.presto.hive.auth.HadoopKerberosAuthentication;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import com.google.inject.PrivateModule;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import com.google.inject.Singleton;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.thrift.client.TUGIAssumingTransport;
import org.apache.hadoop.security.SaslRpcServer;
import org.apache.thrift.transport.TSaslClientTransport;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;

import javax.security.sasl.Sasl;

import java.io.IOException;
import java.util.Map;

import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;
import static org.apache.hadoop.security.SaslRpcServer.AuthMethod.KERBEROS;
import static org.apache.hadoop.security.SecurityUtil.getServerPrincipal;

public class HiveMetastoreAuthenticationKerberos
        implements HiveMetastoreAuthentication
{
    private final String hiveMetastorePrincipal;
    private final HadoopAuthentication authentication;

    @Inject
    public HiveMetastoreAuthenticationKerberos(HiveClientConfig hiveClientConfig, HadoopAuthentication authentication)
    {
        this(hiveClientConfig.getHiveMetastorePrincipal(), authentication);
    }

    public HiveMetastoreAuthenticationKerberos(String hiveMetastorePrincipal, HadoopAuthentication authentication)
    {
        this.hiveMetastorePrincipal = requireNonNull(hiveMetastorePrincipal, "hiveMetastorePrincipal is null");
        this.authentication = requireNonNull(authentication, "hadoopAuthentication is null");
    }

    @Override
    public TTransport createAuthenticatedTransport(TTransport rawTransport, String hiveMetastoreHost)
            throws TTransportException
    {
        try {
            String serverPrincipal = getServerPrincipal(hiveMetastorePrincipal, hiveMetastoreHost);
            String[] names = SaslRpcServer.splitKerberosName(serverPrincipal);
            checkState(names.length == 3,
                    "Kerberos principal name does NOT have the expected hostname part: %s", serverPrincipal);

            Map<String, String> saslProps = ImmutableMap.of(
                    Sasl.QOP, "auth",
                    Sasl.SERVER_AUTH, "true"
            );

            TTransport saslTransport = new TSaslClientTransport(
                    KERBEROS.getMechanismName(),
                    null,
                    names[0],
                    names[1],
                    saslProps,
                    null,
                    rawTransport);

            return new TUGIAssumingTransport(saslTransport, authentication.getUserGroupInformation());
        }
        catch (IOException e) {
            throw Throwables.propagate(e);
        }
    }

    public static class Module
            extends PrivateModule
    {
        @Override
        public void configure()
        {
            bind(HiveMetastoreAuthentication.class)
                    .to(HiveMetastoreAuthenticationKerberos.class)
                    .in(Scopes.SINGLETON);
            expose(HiveMetastoreAuthentication.class);
        }

        @Inject
        @Provides
        @Singleton
        HadoopAuthentication createHadoopAuthentication(HiveClientConfig hiveClientConfig,
                HdfsConfiguration hdfsConfiguration)
        {
            String principal = hiveClientConfig.getHiveMetastorePrestoPrincipal();
            String keytab = hiveClientConfig.getHiveMetastorePrestoKeytab();
            Configuration configuration = hdfsConfiguration.getDefaultConfiguration();
            HadoopAuthentication authentication = new HadoopKerberosAuthentication(principal, keytab, configuration);
            authentication.authenticate();
            return authentication;
        }
    }
}
