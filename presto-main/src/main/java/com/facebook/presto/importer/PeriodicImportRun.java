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
package com.facebook.presto.importer;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Objects;
import org.skife.jdbi.v2.StatementContext;
import org.skife.jdbi.v2.tweak.ResultSetMapper;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;

public class PeriodicImportRun
{
    private final long runId;
    private final long jobId;
    private final Timestamp jobStarttime;
    private final Timestamp jobEndtime;
    private final boolean success;

    @JsonCreator
    public PeriodicImportRun(@JsonProperty("runId") long runId,
            @JsonProperty("jobId") long jobId,
            @JsonProperty("jobStarttime") Timestamp jobStarttime,
            @JsonProperty("jobEndtime") Timestamp jobEndtime,
            @JsonProperty("success") boolean success)
    {
        this.runId = runId;
        this.jobId = jobId;
        this.jobStarttime = jobStarttime;
        this.jobEndtime = jobEndtime;
        this.success = success;
    }

    @JsonProperty
    public long getRunId()
    {
        return runId;
    }

    @JsonProperty
    public long getJobId()
    {
        return jobId;
    }

    @JsonProperty
    public Timestamp getJobStarttime()
    {
        return jobStarttime;
    }

    @JsonProperty
    public Timestamp getJobEndtime()
    {
        return jobEndtime;
    }

    @JsonProperty
    public boolean isSuccess()
    {
        return success;
    }

    @Override
    public int hashCode()
    {
        return Objects.hashCode(runId, jobId, jobStarttime, jobEndtime, success);
    }

    @Override
    public boolean equals(Object object)
    {
        if (object == this) {
            return true;
        }
        else if (object == null || getClass() != object.getClass()) {
            return false;
        }

        PeriodicImportRun that = (PeriodicImportRun) object;
        return Objects.equal(this.runId, that.runId)
                && Objects.equal(this.jobId, that.jobId)
                && Objects.equal(this.jobStarttime, that.jobStarttime)
                && Objects.equal(this.jobEndtime, that.jobEndtime)
                && Objects.equal(this.success, that.success);
    }

    @Override
    public String toString()
    {
        return Objects.toStringHelper(this)
                .add("runId", runId)
                .add("jobId", jobId)
                .add("jobStarttime", jobStarttime)
                .add("jobEndtime", jobEndtime)
                .add("success", success)
                .toString();
    }

    public static class PeriodicImportRunMapper
            implements ResultSetMapper<PeriodicImportRun>
    {
        @Override
        public PeriodicImportRun map(int index, ResultSet r, StatementContext ctx)
                throws SQLException
        {
            return new PeriodicImportRun(r.getLong("run_id"),
                    r.getLong("job_id"),
                    r.getTimestamp("job_start_time"),
                    r.getTimestamp("job_end_time"),
                    r.getBoolean("success"));
        }
    }
}
