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
package com.facebook.presto.verifier.prestoaction;

import com.facebook.airlift.configuration.Config;
import io.airlift.units.Duration;
import io.airlift.units.MinDuration;

import javax.validation.constraints.NotNull;

import static java.util.concurrent.TimeUnit.MINUTES;

public class QueryActionsConfig
{
    private String controlQueryActionType = JdbcPrestoAction.QUERY_ACTION_TYPE;
    private String testQueryActionType = JdbcPrestoAction.QUERY_ACTION_TYPE;
    private boolean runHelperQueriesOnControl = true;

    private Duration metadataTimeout = new Duration(3, MINUTES);
    private Duration checksumTimeout = new Duration(30, MINUTES);

    @NotNull
    public String getControlQueryActionType()
    {
        return controlQueryActionType;
    }

    @Config("control.query-action-type")
    public QueryActionsConfig setControlQueryActionType(String controlQueryActionType)
    {
        this.controlQueryActionType = controlQueryActionType;
        return this;
    }

    @NotNull
    public String getTestQueryActionType()
    {
        return testQueryActionType;
    }

    @Config("test.query-action-type")
    public QueryActionsConfig setTestQueryActionType(String testQueryActionType)
    {
        this.testQueryActionType = testQueryActionType;
        return this;
    }

    public boolean isRunHelperQueriesOnControl()
    {
        return runHelperQueriesOnControl;
    }

    @Config("run-helper-queries-on-control")
    public QueryActionsConfig setRunHelperQueriesOnControl(boolean runHelperQueriesOnControl)
    {
        this.runHelperQueriesOnControl = runHelperQueriesOnControl;
        return this;
    }

    @MinDuration("1s")
    public Duration getMetadataTimeout()
    {
        return metadataTimeout;
    }

    @Config("metadata-timeout")
    public QueryActionsConfig setMetadataTimeout(Duration metadataTimeout)
    {
        this.metadataTimeout = metadataTimeout;
        return this;
    }

    @MinDuration("1s")
    public Duration getChecksumTimeout()
    {
        return checksumTimeout;
    }

    @Config("checksum-timeout")
    public QueryActionsConfig setChecksumTimeout(Duration checksumTimeout)
    {
        this.checksumTimeout = checksumTimeout;
        return this;
    }
}
