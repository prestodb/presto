package com.facebook.presto.cron;

import org.skife.jdbi.v2.sqlobject.Bind;
import org.skife.jdbi.v2.sqlobject.BindBean;
import org.skife.jdbi.v2.sqlobject.GetGeneratedKeys;
import org.skife.jdbi.v2.sqlobject.SqlQuery;
import org.skife.jdbi.v2.sqlobject.SqlUpdate;
import org.skife.jdbi.v2.sqlobject.customizers.Mapper;

import java.util.List;

public interface CronDao
{
    @SqlUpdate("CREATE TABLE IF NOT EXISTS jobs (\n" +
            "  job_id BIGINT PRIMARY KEY AUTO_INCREMENT,\n" +
            "  src_catalog_name VARCHAR(255) NOT NULL,\n" +
            "  src_schema_name VARCHAR(255) NOT NULL,\n" +
            "  src_table_name VARCHAR(255) NOT NULL,\n" +
            "  dst_catalog_name VARCHAR(255) NOT NULL,\n" +
            "  dst_schema_name VARCHAR(255) NOT NULL,\n" +
            "  dst_table_name VARCHAR(255) NOT NULL,\n" +
            "  job_interval INT NOT NULL,\n" +
            "  modified TIMESTAMP NOT NULL DEFAULT NOW()\n" +
            ")")
    void createJobsTable();

    @SqlUpdate("INSERT INTO jobs (src_catalog_name, src_schema_name, src_table_name,\n" +
               "dst_catalog_name, dst_schema_name, dst_table_name, job_interval)\n" +
               "VALUES (:srcCatalogName, :srcSchemaName, :srcTableName,\n" +
               ":dstCatalogName, :dstSchemaName, :dstTableName, :interval)")
    @GetGeneratedKeys
    long insertJob(@BindBean CronJob job);

    @SqlUpdate("DELETE FROM jobs\n" +
            "WHERE job_id = :jobId\n")
    void dropJob(@Bind("jobId") long jobId);

    @SqlQuery("SELECT COUNT(*) FROM jobs")
    long getJobCount();

    @SqlQuery("SELECT * FROM jobs WHERE job_id = :jobId")
    @Mapper(PersistentCronJob.PersistentCronJobMapper.class)
    PersistentCronJob getJob(@Bind("jobId") long jobId);

    @SqlQuery("SELECT * FROM jobs")
    @Mapper(PersistentCronJob.PersistentCronJobMapper.class)
    List<PersistentCronJob> getJobs();
}
