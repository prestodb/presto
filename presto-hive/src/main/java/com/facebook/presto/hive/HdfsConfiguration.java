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
package com.facebook.presto.hive;

import com.facebook.presto.hive.util.SecurityUtils;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.net.HostAndPort;
import com.google.common.primitives.Ints;
import com.google.common.util.concurrent.UncheckedExecutionException;
import io.airlift.log.Logger;
import io.airlift.units.Duration;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.net.DNSToSwitchMapping;
import org.apache.hadoop.net.SocksSocketFactory;
import org.apache.hadoop.security.UserGroupInformation;

import javax.inject.Inject;
import javax.net.SocketFactory;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static java.lang.String.format;
import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHENTICATION;
import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHORIZATION;

public class HdfsConfiguration
{
    private final HostAndPort socksProxy;
    private final Duration dfsTimeout;
    private final Duration dfsConnectTimeout;
    private final int dfsConnectMaxRetries;
    private final String domainSocketPath;
    private final String s3AwsAccessKey;
    private final String s3AwsSecretKey;

    private Boolean hadoopSecurityAuthorization;
    private UserGroupInformation.AuthenticationMethod hadoopSecurityAuthentication;
    private String dfsNamenodeKerberosPrincipal;
    private String dfsNamenodeKeytabFile;
    private String dfsDatanodeKerberosPrincipal;
    private String dfsDatanodeKeytabFile;
    private String prestoKeytabFile;
    private String prestoKerberosPrincipal;

    private HiveConnectorId hiveConnectorId;

    private final List<String> resourcePaths;

    @SuppressWarnings("ThreadLocalNotStaticFinal")
    private final ThreadLocal<Configuration> hadoopConfiguration = new ThreadLocal<Configuration>()
    {
        @Override
        protected Configuration initialValue()
        {
            return createConfiguration();
        }
    };

    private static final ConcurrentHashMap<String, UserGroupInformation> userGroupInformationMap = new ConcurrentHashMap<>();

    private final static ThreadLocal<HiveConnectorId> hiveConnectorIdThreadLocal = new ThreadLocal<HiveConnectorId>() {
    };

    @Inject
    public HdfsConfiguration(HiveClientConfig hiveClientConfig, HiveConnectorId hiveConnectorId)
    {
        checkNotNull(hiveClientConfig, "hiveClientConfig is null");
        checkArgument(hiveClientConfig.getDfsTimeout().toMillis() >= 1, "dfsTimeout must be at least 1 ms");

        this.socksProxy = hiveClientConfig.getMetastoreSocksProxy();
        this.dfsTimeout = hiveClientConfig.getDfsTimeout();
        this.dfsConnectTimeout = hiveClientConfig.getDfsConnectTimeout();
        this.dfsConnectMaxRetries = hiveClientConfig.getDfsConnectMaxRetries();
        this.domainSocketPath = hiveClientConfig.getDomainSocketPath();
        this.s3AwsAccessKey = hiveClientConfig.getS3AwsAccessKey();
        this.s3AwsSecretKey = hiveClientConfig.getS3AwsSecretKey();
        this.resourcePaths = hiveClientConfig.getResourceConfigFiles();
        this.hadoopSecurityAuthorization = hiveClientConfig.getHadoopSecurityAuthorization();
        this.hadoopSecurityAuthentication = hiveClientConfig.getHadoopSecurityAuthentication();
        this.dfsNamenodeKerberosPrincipal = hiveClientConfig.getDfsNamenodeKerberosPrincipal();
        this.dfsNamenodeKeytabFile = hiveClientConfig.getDfsNamenodeKeytabFile();
        this.dfsDatanodeKerberosPrincipal = hiveClientConfig.getDfsDatanodeKerberosPrincipal();
        this.dfsDatanodeKeytabFile = hiveClientConfig.getDfsDatanodeKeytabFile();
        this.prestoKeytabFile = hiveClientConfig.getPrestoKeytabFile();
        this.prestoKerberosPrincipal = hiveClientConfig.getPrestoKerberosPrincipal();
        this.hiveConnectorId = hiveConnectorId;
    }

    @SuppressWarnings("UnusedParameters")
    public Configuration getConfiguration(String host)
    {
        // subclasses can provide per-host configuration
        return hadoopConfiguration.get();
    }

    protected Configuration createConfiguration()
    {
        Configuration config = new Configuration();

        if (resourcePaths != null) {
            for (String resourcePath : resourcePaths) {
                config.addResource(new Path(resourcePath));
            }
        }

        // this is to prevent dfs client from doing reverse DNS lookups to determine whether nodes are rack local
        config.setClass("topology.node.switch.mapping.impl", NoOpDNSToSwitchMapping.class, DNSToSwitchMapping.class);

        if (socksProxy != null) {
            config.setClass("hadoop.rpc.socket.factory.class.default", SocksSocketFactory.class, SocketFactory.class);
            config.set("hadoop.socks.server", socksProxy.toString());
        }

        if (domainSocketPath != null) {
            config.setStrings("dfs.domain.socket.path", domainSocketPath);
        }

        // only enable short circuit reads if domain socket path is properly configured
        if (!config.get("dfs.domain.socket.path", "").trim().isEmpty()) {
            config.setBooleanIfUnset("dfs.client.read.shortcircuit", true);
        }

        config.setInt("dfs.socket.timeout", Ints.checkedCast(dfsTimeout.toMillis()));
        config.setInt("ipc.ping.interval", Ints.checkedCast(dfsTimeout.toMillis()));
        config.setInt("ipc.client.connect.timeout", Ints.checkedCast(dfsConnectTimeout.toMillis()));
        config.setInt("ipc.client.connect.max.retries", dfsConnectMaxRetries);
        if (hadoopSecurityAuthorization && hadoopSecurityAuthentication == UserGroupInformation
                .AuthenticationMethod.KERBEROS) {
            config.setBoolean(HADOOP_SECURITY_AUTHORIZATION, hadoopSecurityAuthorization);
            config.setEnum(HADOOP_SECURITY_AUTHENTICATION, hadoopSecurityAuthentication);
            config.set(DFSConfigKeys.DFS_NAMENODE_USER_NAME_KEY,
                    checkNotNull(dfsNamenodeKerberosPrincipal, "%s cannot be null when kerberos " +
                            "is enabled", DFSConfigKeys.DFS_NAMENODE_USER_NAME_KEY));
            config.set(DFSConfigKeys.DFS_NAMENODE_KEYTAB_FILE_KEY,
                    checkNotNull(dfsNamenodeKeytabFile, "%s cannot be null when kerberos is " +
                            "enabled", DFSConfigKeys.DFS_NAMENODE_KEYTAB_FILE_KEY));
            config.set(DFSConfigKeys.DFS_DATANODE_USER_NAME_KEY,
                    checkNotNull(dfsDatanodeKerberosPrincipal, "%s cannot be null when kerberos " +
                            "is enabled", DFSConfigKeys.DFS_DATANODE_USER_NAME_KEY));
            config.set(DFSConfigKeys.DFS_DATANODE_KEYTAB_FILE_KEY,
                    checkNotNull(dfsDatanodeKeytabFile, "%s cannot be null when kerberos is " +
                            "enabled", DFSConfigKeys.DFS_DATANODE_KEYTAB_FILE_KEY));
        }
        // re-map filesystem schemes to match Amazon Elastic MapReduce
        config.set("fs.s3.impl", PrestoS3FileSystem.class.getName());
        config.set("fs.s3n.impl", PrestoS3FileSystem.class.getName());
        config.set("fs.s3bfs.impl", "org.apache.hadoop.fs.s3.S3FileSystem");

        // set AWS credentials for S3
        for (String scheme : ImmutableList.of("s3", "s3bfs", "s3n")) {
            if (s3AwsAccessKey != null) {
                config.set(format("fs.%s.awsAccessKeyId", scheme), s3AwsAccessKey);
            }
            if (s3AwsSecretKey != null) {
                config.set(format("fs.%s.awsSecretAccessKey", scheme), s3AwsSecretKey);
            }
        }

        updateConfiguration(config);
        UserGroupInformation.setConfiguration(config);
        if (prestoKerberosPrincipal != null && prestoKeytabFile != null) {
            try {
                UserGroupInformation userGroupInformation = SecurityUtils.login(prestoKerberosPrincipal,
                        prestoKeytabFile);
                if (userGroupInformation != null) {
                    userGroupInformationMap.put(this.hiveConnectorId.toString(), userGroupInformation);
                }
            } catch (Throwable t) {
                throw Throwables.propagate(new UncheckedExecutionException(t));
            }
        }
        hiveConnectorIdThreadLocal.set(hiveConnectorId);
        return config;
    }

    @SuppressWarnings("UnusedParameters")
    protected void updateConfiguration(Configuration config)
    {
        // allow subclasses to modify configuration objects
    }

    public static class NoOpDNSToSwitchMapping
            implements DNSToSwitchMapping
    {
        @Override
        public List<String> resolve(List<String> names)
        {
            // dfs client expects an empty list as an indication that the host->switch mapping for the given names are not known
            return ImmutableList.of();
        }

        @Override
        public void reloadCachedMappings()
        {
            // no-op
        }
    }

    public static UserGroupInformation getUserGroupInformation() {
        return userGroupInformationMap.get(hiveConnectorIdThreadLocal.get().toString());
    }
}
