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

import com.facebook.presto.metadata.QualifiedTableName;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.base.Objects;
import org.skife.jdbi.v2.StatementContext;
import org.skife.jdbi.v2.tweak.ResultSetMapper;

import java.sql.ResultSet;
import java.sql.SQLException;

public final class PersistentPeriodicImportJob
{
    private static final Function<PersistentPeriodicImportJob, Long> GET_JOB_ID = new Function<PersistentPeriodicImportJob, Long>()
    {
        @Override
        public Long apply(PersistentPeriodicImportJob input)
        {
            return input.getJobId();
        }
    };

    public static Function<PersistentPeriodicImportJob, Long> jobIdGetter()
    {
        return GET_JOB_ID;
    }

    private final long jobId;
    private final PeriodicImportJob importJob;

    public PersistentPeriodicImportJob(long jobId,
            QualifiedTableName srcTable,
            QualifiedTableName dstTable,
            long interval)
    {
        this.importJob = new PeriodicImportJob(srcTable, dstTable, interval);
        this.jobId = jobId;
    }

    @JsonProperty
    public String getSrcCatalogName()
    {
        return importJob.getSrcCatalogName();
    }

    @JsonProperty
    public String getSrcSchemaName()
    {
        return importJob.getSrcSchemaName();
    }

    @JsonProperty
    public String getSrcTableName()
    {
        return importJob.getSrcTableName();
    }

    @JsonProperty
    public String getDstCatalogName()
    {
        return importJob.getDstCatalogName();
    }

    @JsonProperty
    public String getDstSchemaName()
    {
        return importJob.getDstSchemaName();
    }

    @JsonProperty
    public String getDstTableName()
    {
        return importJob.getDstTableName();
    }

    @JsonProperty
    public long getInterval()
    {
        return importJob.getIntervalSeconds();
    }

    @JsonProperty
    public long getJobId()
    {
        return jobId;
    }

    @VisibleForTesting
    PeriodicImportJob getImportJob()
    {
        return importJob;
    }

    public QualifiedTableName getSrcTable()
    {
        return importJob.getSrcTable();
    }

    public QualifiedTableName getDstTable()
    {
        return importJob.getDstTable();
    }

    @Override
    public int hashCode()
    {
        return Objects.hashCode(importJob, jobId);
    }

    @Override
    public boolean equals(Object object)
    {
        if (object == this) {
            return true;
        }
        else if (object == null || (getClass() != object.getClass())) {
            return false;
        }

        PersistentPeriodicImportJob that = (PersistentPeriodicImportJob) object;
        return Objects.equal(this.importJob, that.importJob) &&
                this.jobId == that.jobId;
    }

    @Override
    public String toString()
    {
        return Objects.toStringHelper(this)
                .add("importJob", importJob)
                .add("jobId", jobId)
                .toString();
    }

    public static class PersistentPeriodicImportJobMapper
            implements ResultSetMapper<PersistentPeriodicImportJob>
    {
        @Override
        public PersistentPeriodicImportJob map(int index, ResultSet r, StatementContext ctx)
                throws SQLException
        {
            QualifiedTableName srcTable = new QualifiedTableName(
                    r.getString("src_catalog_name"),
                    r.getString("src_schema_name"),
                    r.getString("src_table_name"));

            QualifiedTableName dstTable = new QualifiedTableName(
                    r.getString("dst_catalog_name"),
                    r.getString("dst_schema_name"),
                    r.getString("dst_table_name"));

            return new PersistentPeriodicImportJob(
                    r.getLong("job_id"),
                    srcTable,
                    dstTable,
                    r.getLong("job_interval_seconds"));
        }
    }
}
