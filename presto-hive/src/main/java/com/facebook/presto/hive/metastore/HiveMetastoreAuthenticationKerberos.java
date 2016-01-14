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

import com.facebook.presto.hive.HiveClientConfig;
import com.facebook.presto.hive.auth.HadoopKerberosAuthentication;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.inject.Binder;
import com.google.inject.Inject;
import com.google.inject.Scopes;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.thrift.client.TUGIAssumingTransport;
import org.apache.hadoop.security.SaslRpcServer;
import org.apache.thrift.transport.TSaslClientTransport;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static com.google.common.base.MoreObjects.firstNonNull;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;
import static org.apache.hadoop.hive.metastore.MetaStoreUtils.getMetaStoreSaslProperties;
import static org.apache.hadoop.security.SaslRpcServer.AuthMethod.KERBEROS;
import static org.apache.hadoop.security.SecurityUtil.getServerPrincipal;

public class HiveMetastoreAuthenticationKerberos
        implements HiveMetastoreAuthentication
{
    public static class Module
            implements com.google.inject.Module
    {
        @Override
        public void configure(Binder binder)
        {
            binder.bind(HiveMetastoreAuthentication.class)
                    .to(HiveMetastoreAuthenticationKerberos.class)
                    .in(Scopes.SINGLETON);
        }
    }

    private final String hiveMetastorePrincipal;
    private final HadoopKerberosAuthentication metastoreUserAuthentication;
    private final HiveConf hiveConf;

    @Inject
    public HiveMetastoreAuthenticationKerberos(HiveClientConfig hiveClientConfig)
    {
        this(hiveClientConfig.getHiveMetastorePrincipal(),
                hiveClientConfig.getHiveMetastorePrestoPrincipal(),
                hiveClientConfig.getHiveMetastorePrestoKeytab(),
                firstNonNull(hiveClientConfig.getResourceConfigFiles(), ImmutableList.of()));
    }

    public HiveMetastoreAuthenticationKerberos(String hiveMetastorePrincipal,
            String hiveMetastorePrestoPrincipal,
            String hiveMetastorePrestoKeytab,
            List<String> configurationFiles)
    {
        requireNonNull(hiveMetastorePrestoPrincipal, "hiveMetastorePrestoPrincipal is null");
        requireNonNull(hiveMetastorePrestoKeytab, "hiveMetastorePrestoKeytab is null");
        requireNonNull(configurationFiles, "configurationFiles is null");
        Configuration configuration = createConfiguration(configurationFiles);
        this.hiveConf = new HiveConf(configuration, HiveMetastoreAuthenticationKerberos.class);
        this.hiveMetastorePrincipal = requireNonNull(hiveMetastorePrincipal, "hiveMetastorePrincipal is null");
        this.metastoreUserAuthentication = new HadoopKerberosAuthentication(
                hiveMetastorePrestoPrincipal, hiveMetastorePrestoKeytab, configuration
        );
        this.metastoreUserAuthentication.authenticate();
    }

    private static Configuration createConfiguration(List<String> configurationFiles)
    {
        Configuration configuration = new Configuration();
        configurationFiles.forEach(filePath -> configuration.addResource(new Path(filePath)));
        return configuration;
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
            Map<String, String> saslProps = getMetaStoreSaslProperties(hiveConf);

            TTransport saslTransport = new TSaslClientTransport(
                    KERBEROS.getMechanismName(),
                    null,
                    names[0],
                    names[1],
                    saslProps,
                    null,
                    rawTransport);

            return new TUGIAssumingTransport(saslTransport, metastoreUserAuthentication.getUserGroupInformation());
        }
        catch (IOException e) {
            throw Throwables.propagate(e);
        }
    }
}
