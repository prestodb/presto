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
package com.facebook.presto.mongodb;

import com.google.common.base.Splitter;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.mongodb.MongoCredential;
import com.mongodb.ServerAddress;
import io.airlift.configuration.Config;

import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;
import javax.validation.constraints.Size;

import java.util.Arrays;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static com.mongodb.MongoCredential.createCredential;

public class MongoClientConfig
{
    private static final Splitter SPLITTER = Splitter.on(',').trimResults().omitEmptyStrings();
    private static final Splitter PORT_SPLITTER = Splitter.on(':').trimResults().omitEmptyStrings();
    private static final Splitter USER_SPLITTER = Splitter.onPattern("[:@]").trimResults().omitEmptyStrings();

    private String schemaCollection = "_schema";
    private List<ServerAddress> seeds = ImmutableList.of();
    private List<MongoCredential> credentials = ImmutableList.of();

    private int minConnectionsPerHost = 0;
    private int connectionsPerHost = 100;
    private int maxWaitTime = 120_000;
    private int connectionTimeout = 10_000;
    private int socketTimeout = 0;
    private boolean socketKeepAlive = false;

    // query configurations
    private int cursorBatchSize = 0; // use driver default

    private ReadPreferenceType readPreference = ReadPreferenceType.PRIMARY;
    private WriteConcernType writeConcern = WriteConcernType.ACKNOWLEDGED;
    private String requiredReplicaSetName;
    private String implicitRowFieldPrefix = "_pos";

    @NotNull
    public String getSchemaCollection()
    {
        return schemaCollection;
    }

    @Config("mongodb.schema-collection")
    public MongoClientConfig setSchemaCollection(String schemaCollection)
    {
        this.schemaCollection = schemaCollection;
        return this;
    }

    @NotNull
    @Size(min = 1)
    public List<ServerAddress> getSeeds()
    {
        return seeds;
    }

    @Config("mongodb.seeds")
    public MongoClientConfig setSeeds(String commaSeparatedList)
    {
        this.seeds = buildSeeds(SPLITTER.split(commaSeparatedList));
        return this;
    }

    public MongoClientConfig setSeeds(String... seeds)
    {
        this.seeds = buildSeeds(Arrays.asList(seeds));
        return this;
    }

    @NotNull
    @Size(min = 0)
    public List<MongoCredential> getCredentials()
    {
        return credentials;
    }

    @Config("mongodb.credentials")
    public MongoClientConfig setCredentials(String credentials)
    {
        this.credentials = buildCredentials(SPLITTER.split(credentials));
        return this;
    }

    public MongoClientConfig setCredentils(String... credentials)
    {
        this.credentials = buildCredentials(Arrays.asList(credentials));
        return this;
    }

    private List<ServerAddress> buildSeeds(Iterable<String> hostPorts)
    {
        ImmutableList.Builder<ServerAddress> builder = ImmutableList.builder();
        for (String hostPort : hostPorts) {
            List<String> values = PORT_SPLITTER.splitToList(hostPort);
            checkArgument(values.size() == 1 || values.size() == 2, "Invalid ServerAddress format. Requires host[:port]");
            try {
                if (values.size() == 1) {
                    builder.add(new ServerAddress(values.get(0)));
                }
                else {
                    builder.add(new ServerAddress(values.get(0), Integer.parseInt(values.get(1))));
                }
            }
            catch (NumberFormatException e) {
                throw Throwables.propagate(e);
            }
        }
        return builder.build();
    }

    private List<MongoCredential> buildCredentials(Iterable<String> userPasses)
    {
        ImmutableList.Builder<MongoCredential> builder = ImmutableList.builder();
        for (String userPass : userPasses) {
            List<String> values = USER_SPLITTER.splitToList(userPass);
            checkArgument(values.size() == 3, "Invalid Credential format. Requires user:password@collection");
            builder.add(createCredential(values.get(0), values.get(2), values.get(1).toCharArray()));
        }
        return builder.build();
    }

    @Min(0)
    public int getMinConnectionsPerHost()
    {
        return minConnectionsPerHost;
    }

    @Config("mongodb.min-connections-per-host")
    public MongoClientConfig setMinConnectionsPerHost(int minConnectionsPerHost)
    {
        this.minConnectionsPerHost = minConnectionsPerHost;
        return this;
    }

    @Min(1)
    public int getConnectionsPerHost()
    {
        return connectionsPerHost;
    }

    @Config("mongodb.connection-per-host")
    public MongoClientConfig setConnectionsPerHost(int connectionsPerHost)
    {
        this.connectionsPerHost = connectionsPerHost;
        return this;
    }

    @Min(0)
    public int getMaxWaitTime()
    {
        return maxWaitTime;
    }

    @Config("mongodb.max-wait-time")
    public MongoClientConfig setMaxWaitTime(int maxWaitTime)
    {
        this.maxWaitTime = maxWaitTime;
        return this;
    }

    @Min(0)
    public int getConnectionTimeout()
    {
        return connectionTimeout;
    }

    @Config("mongodb.connection-timeout")
    public MongoClientConfig setConnectionTimeout(int connectionTimeout)
    {
        this.connectionTimeout = connectionTimeout;
        return this;
    }

    @Min(0)
    public int getSocketTimeout()
    {
        return socketTimeout;
    }

    @Config("mongodb.socket-timeout")
    public MongoClientConfig setSocketTimeout(int socketTimeout)
    {
        this.socketTimeout = socketTimeout;
        return this;
    }

    public boolean getSocketKeepAlive()
    {
        return socketKeepAlive;
    }

    @Config("mongodb.socket-keep-alive")
    public MongoClientConfig setSocketKeepAlive(boolean socketKeepAlive)
    {
        this.socketKeepAlive = socketKeepAlive;
        return this;
    }

    public ReadPreferenceType getReadPreference()
    {
        return readPreference;
    }

    @Config("mongodb.read-preference")
    public MongoClientConfig setReadPreference(ReadPreferenceType readPreference)
    {
        this.readPreference = readPreference;
        return this;
    }

    public WriteConcernType getWriteConcern()
    {
        return writeConcern;
    }

    @Config("mongodb.write-concern")
    public MongoClientConfig setWriteConcern(WriteConcernType writeConcern)
    {
        this.writeConcern = writeConcern;
        return this;
    }

    public String getRequiredReplicaSetName()
    {
        return requiredReplicaSetName;
    }

    @Config("mongodb.required-replica-set")
    public MongoClientConfig setRequiredReplicaSetName(String requiredReplicaSetName)
    {
        this.requiredReplicaSetName = requiredReplicaSetName;
        return this;
    }

    public int getCursorBatchSize()
    {
        return cursorBatchSize;
    }

    @Config("mongodb.cursor-batch-size")
    public MongoClientConfig setCursorBatchSize(int cursorBatchSize)
    {
        this.cursorBatchSize = cursorBatchSize;
        return this;
    }

    public String getImplicitRowFieldPrefix()
    {
        return implicitRowFieldPrefix;
    }

    @Config("mongodb.implicit-row-field-prefix")
    public MongoClientConfig setImplicitRowFieldPrefix(String implicitRowFieldPrefix)
    {
        this.implicitRowFieldPrefix = implicitRowFieldPrefix;
        return this;
    }
}
