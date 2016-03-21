package com.facebook.presto.hive.authentication;

import com.facebook.presto.hive.ForHiveMetastore;
import com.facebook.presto.hive.HiveClientConfig;
import com.google.inject.Binder;
import com.google.inject.Inject;
import com.google.inject.Module;
import com.google.inject.Provides;
import com.google.inject.Singleton;

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

            @Inject
            @Provides
            @Singleton
            @ForHiveMetastore
            HadoopAuthentication createHadoopAuthentication(HiveClientConfig hiveClientConfig)
            {
                String principal = hiveClientConfig.getHiveMetastoreClientPrincipal();
                String keytabLocation = hiveClientConfig.getHiveMetastoreClientKeytab();
                KerberosAuthentication kerberosAuthentication = new KerberosAuthentication(principal, keytabLocation);
                KerberosHadoopAuthentication kerberosHadoopAuthentication = new KerberosHadoopAuthentication(kerberosAuthentication);
                return new CachingKerberosHadoopAuthentication(kerberosHadoopAuthentication);
            }
        };
    }
}
