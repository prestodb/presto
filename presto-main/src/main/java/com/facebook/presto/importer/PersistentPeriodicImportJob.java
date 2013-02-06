package com.facebook.presto.importer;

import com.fasterxml.jackson.annotation.JsonIgnore;
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
    private static final Function<PersistentPeriodicImportJob, Long> GET_JOB_ID = new Function<PersistentPeriodicImportJob, Long>() {

        @Override
        public Long apply(PersistentPeriodicImportJob input)
        {
            return input.getJobId();
        }
    };

    public static final Function<PersistentPeriodicImportJob, Long> jobIdGetter()
    {
        return GET_JOB_ID;
    }

    private final long jobId;
    private final PeriodicImportJob importJob;

    public PersistentPeriodicImportJob(long jobId,
                             String srcCatalogName,
                             String srcSchemaName,
                             String srcTableName,
                             String dstCatalogName,
                             String dstSchemaName,
                             String dstTableName,
                             long interval)
    {
        this.importJob = new PeriodicImportJob(srcCatalogName, srcSchemaName, srcTableName, dstCatalogName, dstSchemaName, dstTableName, interval);
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

    @JsonIgnore
    @VisibleForTesting
    PeriodicImportJob getImportJob()
    {
        return importJob;
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

    public static class PersistentPeriodicImportJobMapper implements ResultSetMapper<PersistentPeriodicImportJob>
    {
        @Override
        public PersistentPeriodicImportJob map(int index, ResultSet r, StatementContext ctx)
                throws SQLException
        {
            return new PersistentPeriodicImportJob(
                r.getLong("job_id"),
                r.getString("src_catalog_name"),
                r.getString("src_schema_name"),
                r.getString("src_table_name"),
                r.getString("dst_catalog_name"),
                r.getString("dst_schema_name"),
                r.getString("dst_table_name"),
                r.getLong("job_interval_seconds"));
        }
    }
}
