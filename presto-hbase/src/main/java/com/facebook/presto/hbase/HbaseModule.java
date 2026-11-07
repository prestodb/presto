package com.facebook.presto.hbase;

import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.TypeManager;
import com.facebook.presto.hbase.conf.HbaseConfig;
import com.facebook.presto.hbase.conf.HbaseSessionProperties;
import com.facebook.presto.hbase.conf.HbaseTableProperties;
import com.facebook.presto.hbase.io.HbasePageSinkProvider;
import com.facebook.presto.hbase.io.HbaseRecordSetProvider;
import com.facebook.presto.hbase.metadata.ZooKeeperMetadataManager;
import com.facebook.presto.spi.PrestoException;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.deser.std.FromStringDeserializer;
import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Scopes;
import io.airlift.log.Logger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

import javax.inject.Inject;
import javax.inject.Provider;

import java.io.IOException;
import org.apache.hadoop.security.UserGroupInformation;

import static com.facebook.presto.hbase.HbaseErrorCode.UNEXPECTED_HBASE_ERROR;
import static com.google.common.base.Preconditions.checkArgument;
import static io.airlift.configuration.ConfigBinder.configBinder;
import static java.util.Objects.requireNonNull;

/**
 * @author spancer.ray
 */
public class HbaseModule implements Module {

    @Override
    public void configure(Binder binder) {
        configBinder(binder).bindConfig(HbaseConfig.class);

        binder.bind(HbaseClient.class).in(Scopes.SINGLETON);

        binder.bind(HbaseConnector.class).in(Scopes.SINGLETON);
        binder.bind(HbaseMetadata.class).in(Scopes.SINGLETON);
        binder.bind(HbaseSplitManager.class).in(Scopes.SINGLETON);
        binder.bind(HbaseRecordSetProvider.class).in(Scopes.SINGLETON);
        binder.bind(HbasePageSinkProvider.class).in(Scopes.SINGLETON);

        binder.bind(ZooKeeperMetadataManager.class).in(Scopes.SINGLETON);
        binder.bind(HbaseTableProperties.class).in(Scopes.SINGLETON);
        binder.bind(HbaseSessionProperties.class).in(Scopes.SINGLETON);
        binder.bind(HbaseTableManager.class).in(Scopes.SINGLETON);

        binder.bind(Connection.class).toProvider(ConnectionProvider.class).in(Scopes.SINGLETON);
    }

    public static final class TypeDeserializer extends FromStringDeserializer<Type> {

        private static final long serialVersionUID = 4800237080895755226L;
        private final TypeManager typeManager;

        @Inject
        public TypeDeserializer(TypeManager typeManager) {
            super(Type.class);
            this.typeManager = requireNonNull(typeManager, "typeManager is null");
        }

        @Override
        protected Type _deserialize(String value, DeserializationContext context) {
            Type type = typeManager
                    .getType(com.facebook.presto.common.type.TypeSignature.parseTypeSignature(value));
            checkArgument(type != null, "Unknown type %s", value);
            return type;
        }
    }

    private static class ConnectionProvider implements Provider<Connection> {

        private static final Logger LOG = Logger.get(ConnectionProvider.class);
        private final String zooKeepers;

        private String zookeeperZnodeParent;

        private String krb5Conf;

        private String hadoopSecurityAuthentication = "kerberos";

        private String hbaseSecurityAuthentication = "kerberos";

        private String hbaseKeytabFile;

        private String hbaseKerberosPricipal;

        private String hbaseMasterKerberosPricipal;

        private String hbaseRegionserverPricipal;

        private boolean enableKerberos;

        @Inject
        public ConnectionProvider(HbaseConfig config) {
            requireNonNull(config, "config is null");
            this.zooKeepers = config.getZooKeepers();
            this.zookeeperZnodeParent = config.getZookeeperZnodeParent();
            this.krb5Conf = config.getKrb5Conf();
            this.hbaseKeytabFile = config.getHbaseKeytabFile();
            this.hbaseKerberosPricipal = config.getHbaseKerberoPrincipal();
            this.hbaseMasterKerberosPricipal = config.getHbaseMasterKerberoPrincipal();
            this.hbaseRegionserverPricipal = config.getHbaseRegionserverKerberoPrincipal();
            this.enableKerberos = config.getEnableKerberos();
        }

        @Override
        public Connection get() {
            try {
                Configuration conf = HBaseConfiguration.create();
                conf.set("hbase.zookeeper.quorum", zooKeepers);

                conf.set("hbase.client.pause", "50");
                conf.set("hbase.client.retries.number", "3");
                conf.set("hbase.rpc.timeout", "2000");
                conf.set("hbase.client.operation.timeout", "3000");
                conf.set("hbase.client.scanner.timeout.period", "10000");
                conf.set("zookeeper.znode.parent", zookeeperZnodeParent);
                if (enableKerberos){
                    conf.set("hadoop.security.authentication", hadoopSecurityAuthentication);
                    conf.set("hbase.security.authentication", hbaseSecurityAuthentication);
                    conf.set("keytab.file", hbaseKeytabFile);
                    conf.set("kerberos.principal", hbaseKerberosPricipal);
                    conf.set("hbase.master.kerberos.principal", hbaseMasterKerberosPricipal);
                    conf.set("hbase.regionserver.kerberos.principal", hbaseRegionserverPricipal);
                    System.setProperty("java.security.krb5.conf", krb5Conf);
                    System.setProperty("javax.security.auth.useSubjectCredsOnly", "false");
                    String kinitCmd = " kinit -kt " + hbaseKeytabFile + " " + hbaseKerberosPricipal;
                    try {
                        Runtime.getRuntime().exec(kinitCmd);
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                }
                Connection connection = ConnectionFactory.createConnection(conf);
                return connection;
            } catch (IOException e) {
                throw new PrestoException(UNEXPECTED_HBASE_ERROR, "Failed to get connection to HBASE", e);
            }
        }
    }
}
