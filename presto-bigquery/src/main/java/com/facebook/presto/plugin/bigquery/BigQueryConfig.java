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
package com.facebook.presto.plugin.bigquery;

import com.facebook.airlift.configuration.Config;
import com.facebook.airlift.configuration.ConfigDescription;
import com.google.auth.oauth2.GoogleCredentials;
import jakarta.validation.constraints.AssertTrue;
import jakarta.validation.constraints.Min;

import java.io.IOException;
import java.util.Optional;
import java.util.OptionalInt;

public class BigQueryConfig
{
    public static final int DEFAULT_MAX_READ_ROWS_RETRIES = 3;

    private static final int VIEW_EXPIRATION_TIMEIN_HOURS = 24;

    private Optional<String> credentialsKey = Optional.empty();
    private Optional<String> credentialsFile = Optional.empty();
    private Optional<String> projectId = Optional.empty();
    private Optional<String> parentProjectId = Optional.empty();
    private OptionalInt parallelism = OptionalInt.empty();
    private boolean viewsEnabled;
    private boolean caseSensitiveNameMatching;
    private Optional<String> viewMaterializationProject = Optional.empty();
    private Optional<String> viewMaterializationDataset = Optional.empty();
    private int maxReadRowsRetries = DEFAULT_MAX_READ_ROWS_RETRIES;

    @AssertTrue(message = "Either one of 'bigquery.credentials-key' or 'bigquery.credentials-file' must be specified, or the default GoogleCredentials could be created")
    public boolean isCredentialsConfigurationValid()
    {
        // at most one of them should present
        if (credentialsKey.isPresent() && credentialsFile.isPresent()) {
            return false;
        }
        // if no credentials, try create default ones
        if (!credentialsKey.isPresent() && !credentialsFile.isPresent()) {
            try {
                GoogleCredentials.getApplicationDefault();
            }
            catch (IOException e) {
                return false;
            }
        }
        return true;
    }

    public Optional<String> getCredentialsKey()
    {
        return credentialsKey;
    }

    @Config("bigquery.credentials-key")
    @ConfigDescription("credentials key (base64 encoded)")
    public BigQueryConfig setCredentialsKey(String credentialsKey)
    {
        this.credentialsKey = Optional.of(credentialsKey);
        return this;
    }

    public Optional<String> getCredentialsFile()
    {
        return credentialsFile;
    }

    @Config("bigquery.credentials-file")
    @ConfigDescription("JSON credentials file path")
    public BigQueryConfig setCredentialsFile(String credentialsFile)
    {
        this.credentialsFile = Optional.of(credentialsFile);
        return this;
    }

    public Optional<String> getProjectId()
    {
        return projectId;
    }

    @Config("bigquery.project-id")
    @ConfigDescription("Google Cloud project ID")
    public BigQueryConfig setProjectId(String projectId)
    {
        this.projectId = Optional.of(projectId);
        return this;
    }

    public Optional<String> getParentProjectId()
    {
        return parentProjectId;
    }

    @Config("bigquery.parent-project-id")
    @ConfigDescription("Google Cloud parent project ID")
    public BigQueryConfig setParentProjectId(String parentProjectId)
    {
        this.parentProjectId = Optional.of(parentProjectId);
        return this;
    }

    public OptionalInt getParallelism()
    {
        return parallelism;
    }

    @Config("bigquery.parallelism")
    @ConfigDescription("The number of partitions to split the data")
    public BigQueryConfig setParallelism(int parallelism)
    {
        this.parallelism = OptionalInt.of(parallelism);
        return this;
    }

    public boolean isViewsEnabled()
    {
        return viewsEnabled;
    }

    @Config("bigquery.views-enabled")
    @ConfigDescription("Enable BigQuery connector to read views")
    public BigQueryConfig setViewsEnabled(boolean viewsEnabled)
    {
        this.viewsEnabled = viewsEnabled;
        return this;
    }

    public int getViewExpirationTimeInHours()
    {
        return VIEW_EXPIRATION_TIMEIN_HOURS;
    }

    public Optional<String> getViewMaterializationProject()
    {
        return viewMaterializationProject;
    }

    @Config("bigquery.view-materialization-project")
    @ConfigDescription("The project where the materialized view is going to be created")
    public BigQueryConfig setViewMaterializationProject(String viewMaterializationProject)
    {
        this.viewMaterializationProject = Optional.of(viewMaterializationProject);
        return this;
    }

    public Optional<String> getViewMaterializationDataset()
    {
        return viewMaterializationDataset;
    }

    @Config("bigquery.view-materialization-dataset")
    @ConfigDescription("The dataset where the materialized view is going to be created")
    public BigQueryConfig setViewMaterializationDataset(String viewMaterializationDataset)
    {
        this.viewMaterializationDataset = Optional.of(viewMaterializationDataset);
        return this;
    }

    @Min(0)
    public int getMaxReadRowsRetries()
    {
        return maxReadRowsRetries;
    }

    @Config("bigquery.max-read-rows-retries")
    @ConfigDescription("The number of retries in case of server issues")
    public BigQueryConfig setMaxReadRowsRetries(int maxReadRowsRetries)
    {
        this.maxReadRowsRetries = maxReadRowsRetries;
        return this;
    }

    public boolean isCaseSensitiveNameMatching()
    {
        return caseSensitiveNameMatching;
    }

    @Config("case-sensitive-name-matching")
    @ConfigDescription(
            "Case sensitivity for schema and table name matching. " +
             "true = preserve case and require exact matches; " +
             "false (default) = normalize to lower case and match case-insensitively.")
    public BigQueryConfig setCaseSensitiveNameMatching(boolean caseSensitiveNameMatching)
    {
        this.caseSensitiveNameMatching = caseSensitiveNameMatching;
        return this;
    }

    ReadSessionCreatorConfig createReadSessionCreatorConfig()
    {
        return new ReadSessionCreatorConfig(
                getParentProjectId(),
                isViewsEnabled(),
                getViewMaterializationProject(),
                getViewMaterializationProject(),
                getViewExpirationTimeInHours(),
                getMaxReadRowsRetries());
    }
}
