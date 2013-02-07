package com.facebook.presto.cron;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Objects;
import org.skife.jdbi.v2.StatementContext;
import org.skife.jdbi.v2.tweak.ResultSetMapper;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;

public class PersistentCronJob extends CronJob
{
    private final long jobId;
    private final Timestamp modified;

    public PersistentCronJob(long jobId,
                             Timestamp modified,
                             String srcCatalogName,
                             String srcSchemaName,
                             String srcTableName,
                             String dstCatalogName,
                             String dstSchemaName,
                             String dstTableName,
                             long interval)
    {
        super(srcCatalogName, srcSchemaName, srcTableName, dstCatalogName, dstSchemaName, dstTableName, interval);

        this.jobId = jobId;
        this.modified = modified;
    }

    @JsonProperty
    public long getJobId()
    {
        return jobId;
    }

    @JsonIgnore
    public Timestamp getModified()
    {
        return modified;
    }

    @Override
    public int hashCode()
    {
        return 31 * super.hashCode() + Objects.hashCode(jobId, modified);
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        else if (!super.equals(obj)) {
            return false;
        }
        else if (getClass() != obj.getClass()) {
            return false;
        }
        PersistentCronJob other = (PersistentCronJob) obj;
        return Objects.equal(jobId, other.jobId)
                && Objects.equal(modified, other.modified);
    }

    @Override
    public String toString()
    {
        return Objects.toStringHelper(this)
                .add("modified", modified)
                .add("jobId", jobId)
                .toString();
    }

    public static class PersistentCronJobMapper implements ResultSetMapper<PersistentCronJob>
    {
        @Override
        public PersistentCronJob map(int index, ResultSet r, StatementContext ctx)
                throws SQLException
        {
            return new PersistentCronJob(
                r.getLong("job_id"),
                r.getTimestamp("modified"),
                r.getString("src_catalog_name"),
                r.getString("src_schema_name"),
                r.getString("src_table_name"),
                r.getString("dst_catalog_name"),
                r.getString("dst_schema_name"),
                r.getString("dst_table_name"),
                r.getLong("job_interval"));
        }
    }
}
