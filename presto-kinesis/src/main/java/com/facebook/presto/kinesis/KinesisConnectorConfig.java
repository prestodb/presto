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
package com.facebook.presto.kinesis;

import io.airlift.configuration.Config;

import java.io.File;
import java.util.Set;

import javax.validation.constraints.NotNull;

import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableSet;

/**
 * This Class handles all the configuration settings that is stored in /etc/catalog/kinesis.properties file
 *
 */
public class KinesisConnectorConfig
{
    /**
     * Set of tables known to this connector. For each table, a description file may be present in the catalog folder which describes columns for the given topic.
     */
    private Set<String> tableNames = ImmutableSet.of();

    /**
     * The schema name to use in the connector.
     */
    private String defaultSchema = "default";

    /**
     * Folder holding the JSON description files for Kafka topics.
     */
    private File tableDescriptionDir = new File("etc/kinesis/");

    /**
     * Whether internal columns are shown in table metadata or not. Default is no.
     */
    private boolean hideInternalColumns = true;

    /**
     * Region to be used to read stream from.
     */
    private String awsRegion = "us-east-1";

    /**
     * Defines maximum number of records to return in one call
     */
    private int batchSize = 10000;

    /**
     * Defines number of attempts to fetch records from stream until received non-empty
     */
    private int fetchAttmepts = 3;

    /**
     *  Defines sleep time (in milliseconds) for the thread which is trying to fetch the records from kinesis streams
     */
    private int sleepTime = 1000;

    private String accessKey = null;

    private String secretKey = null;

    @NotNull
    public File getTableDescriptionDir()
    {
        return tableDescriptionDir;
    }

    @Config("kinesis.table-description-dir")
    public KinesisConnectorConfig setTableDescriptionDir(File tableDescriptionDir)
    {
        this.tableDescriptionDir = tableDescriptionDir;
        return this;
    }

    public boolean isHideInternalColumns()
    {
        return hideInternalColumns;
    }

    @Config("kinesis.hide-internal-columns")
    public KinesisConnectorConfig setHideInternalColumns(boolean hideInternalColumns)
    {
        this.hideInternalColumns = hideInternalColumns;
        return this;
    }

    @NotNull
    public Set<String> getTableNames()
    {
        return tableNames;
    }

    @Config("kinesis.table-names")
    public KinesisConnectorConfig setTableNames(String tableNames)
    {
        this.tableNames = ImmutableSet.copyOf(Splitter.on(',').omitEmptyStrings().trimResults().split(tableNames));
        return this;
    }

    @NotNull
    public String getDefaultSchema()
    {
        return defaultSchema;
    }

    @Config("kinesis.default-schema")
    public KinesisConnectorConfig setDefaultSchema(String defaultSchema)
    {
        this.defaultSchema = defaultSchema;
        return this;
    }

    @Config("kinesis.access-key")
    public KinesisConnectorConfig setAccessKey(String accessKey)
    {
        this.accessKey = accessKey;
        return this;
    }

    public String getAccessKey()
    {
        return this.accessKey;
    }

    @Config("kinesis.secret-key")
    public KinesisConnectorConfig setSecretKey(String secretKey)
    {
        this.secretKey = secretKey;
        return this;
    }

    public String getSecretKey()
    {
        return this.secretKey;
    }

    @Config("kinesis.aws-region")
    public KinesisConnectorConfig setAwsRegion(String awsRegion)
    {
        this.awsRegion = awsRegion;
        return this;
    }

    public String getAwsRegion()
    {
        return awsRegion;
    }

    @Config("kinesis.batch-size")
    public KinesisConnectorConfig setBatchSize(int batchSize)
    {
        this.batchSize = batchSize;
        return this;
    }

    public int getBatchSize()
    {
        return this.batchSize;
    }

    @Config("kinesis.fetch-attempts")
    public KinesisConnectorConfig setFetchAttempts(int fetchAttempts)
    {
        this.fetchAttmepts = fetchAttempts;
        return this;
    }

    public int getFetchAttempts()
    {
        return this.fetchAttmepts;
    }

    @Config("kinesis.sleep-time")
    public KinesisConnectorConfig setSleepTime(int sleepTime)
    {
        this.sleepTime = sleepTime;
        return this;
    }

    public int getSleepTime()
    {
        return this.sleepTime;
    }
}
