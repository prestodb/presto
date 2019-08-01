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
import com.facebook.presto.hive.HdfsConfigurationInitializer;
import com.google.inject.Binder;
import com.google.inject.Key;
import com.google.inject.Module;
import com.google.inject.Provides;
import com.google.inject.Singleton;

import javax.inject.Inject;

import static com.facebook.airlift.configuration.ConfigBinder.configBinder;
import static com.facebook.presto.hive.authentication.KerberosHadoopAuthentication.createKerberosHadoopAuthentication;
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
                configBinder(binder).bindConfig(MetastoreKerberosConfig.class);
            }

            @Inject
            @Provides
            @Singleton
            @ForHiveMetastore
            HadoopAuthentication createHadoopAuthentication(MetastoreKerberosConfig config, HdfsConfigurationInitializer initializer)
            {
                String principal = config.getHiveMetastoreClientPrincipal();
                String keytabLocation = config.getHiveMetastoreClientKeytab();
                return createCachingKerberosHadoopAuthentication(principal, keytabLocation, initializer);
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
                configBinder(binder).bindConfig(HdfsKerberosConfig.class);
            }

            @Inject
            @Provides
            @Singleton
            @ForHdfs
            HadoopAuthentication createHadoopAuthentication(HdfsKerberosConfig config, HdfsConfigurationInitializer initializer)
            {
                String principal = config.getHdfsPrestoPrincipal();
                String keytabLocation = config.getHdfsPrestoKeytab();
                return createCachingKerberosHadoopAuthentication(principal, keytabLocation, initializer);
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
                configBinder(binder).bindConfig(HdfsKerberosConfig.class);
            }

            @Inject
            @Provides
            @Singleton
            @ForHdfs
            HadoopAuthentication createHadoopAuthentication(HdfsKerberosConfig config, HdfsConfigurationInitializer initializer)
            {
                String principal = config.getHdfsPrestoPrincipal();
                String keytabLocation = config.getHdfsPrestoKeytab();
                return createCachingKerberosHadoopAuthentication(principal, keytabLocation, initializer);
            }
        };
    }

    private static HadoopAuthentication createCachingKerberosHadoopAuthentication(String principal, String keytabLocation, HdfsConfigurationInitializer initializer)
    {
        KerberosAuthentication kerberosAuthentication = new KerberosAuthentication(principal, keytabLocation);
        KerberosHadoopAuthentication kerberosHadoopAuthentication = createKerberosHadoopAuthentication(kerberosAuthentication, initializer);
        return new CachingKerberosHadoopAuthentication(kerberosHadoopAuthentication);
    }
}
