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
package com.facebook.presto.hive.authentication;

import com.facebook.presto.hive.ForHdfs;
import com.facebook.presto.hive.ForHiveMetastore;
import com.facebook.presto.hive.HiveClientConfig;
import com.google.inject.Binder;
import com.google.inject.Key;
import com.google.inject.Module;
import com.google.inject.Provides;
import com.google.inject.Singleton;

import javax.inject.Inject;

import static com.google.inject.Scopes.SINGLETON;

public final class AuthenticationModules
{
    private AuthenticationModules() {}

    public static Module noHiveMetastoreAuthenticationModule()
    {
        return binder -> binder
                .bind(HiveMetastoreAuthentication.class)
                .to(NoHiveMetastoreAuthentication.class)
                .in(SINGLETON);
    }

    public static Module kerberosHiveMetastoreAuthenticationModule()
    {
        return new Module()
        {
            @Override
            public void configure(Binder binder)
            {
                binder.bind(HiveMetastoreAuthentication.class)
                        .to(KerberosHiveMetastoreAuthentication.class)
                        .in(SINGLETON);
            }

            @Provides
            @Singleton
            @ForHiveMetastore
            HadoopAuthentication createHadoopAuthentication(HiveClientConfig hiveClientConfig)
            {
                String principal = hiveClientConfig.getHiveMetastoreClientPrincipal();
                String keytabLocation = hiveClientConfig.getHiveMetastoreClientKeytab();
                return createCachingKerberosHadoopAuthentication(principal, keytabLocation);
            }
        };
    }

    public static Module noHdfsAuthenticationModule()
    {
        return binder -> binder
                .bind(HdfsAuthentication.class)
                .to(NoHdfsAuthentication.class)
                .in(SINGLETON);
    }

    public static Module simpleImpersonatingHdfsAuthenticationModule()
    {
        return binder -> {
            binder.bind(Key.get(HadoopAuthentication.class, ForHdfs.class))
                    .to(SimpleHadoopAuthentication.class);
            binder.bind(HdfsAuthentication.class)
                    .to(ImpersonatingHdfsAuthentication.class)
                    .in(SINGLETON);
        };
    }

    public static Module kerberosHdfsAuthenticationModule()
    {
        return new Module()
        {
            @Override
            public void configure(Binder binder)
            {
                binder.bind(HdfsAuthentication.class)
                        .to(DirectHdfsAuthentication.class)
                        .in(SINGLETON);
            }

            @Inject
            @Provides
            @Singleton
            @ForHdfs
            HadoopAuthentication createHadoopAuthentication(HiveClientConfig hiveClientConfig)
            {
                String principal = hiveClientConfig.getHdfsPrestoPrincipal();
                String keytabLocation = hiveClientConfig.getHdfsPrestoKeytab();
                return createCachingKerberosHadoopAuthentication(principal, keytabLocation);
            }
        };
    }

    public static Module kerberosImpersonatingHdfsAuthenticationModule()
    {
        return new Module()
        {
            @Override
            public void configure(Binder binder)
            {
                binder.bind(HdfsAuthentication.class)
                        .to(ImpersonatingHdfsAuthentication.class)
                        .in(SINGLETON);
            }

            @Inject
            @Provides
            @Singleton
            @ForHdfs
            HadoopAuthentication createHadoopAuthentication(HiveClientConfig hiveClientConfig)
            {
                String principal = hiveClientConfig.getHdfsPrestoPrincipal();
                String keytabLocation = hiveClientConfig.getHdfsPrestoKeytab();
                return createCachingKerberosHadoopAuthentication(principal, keytabLocation);
            }
        };
    }

    private static HadoopAuthentication createCachingKerberosHadoopAuthentication(String principal, String keytabLocation)
    {
        KerberosAuthentication kerberosAuthentication = new KerberosAuthentication(principal, keytabLocation);
        KerberosHadoopAuthentication kerberosHadoopAuthentication = new KerberosHadoopAuthentication(kerberosAuthentication);
        return new CachingKerberosHadoopAuthentication(kerberosHadoopAuthentication);
    }
}
