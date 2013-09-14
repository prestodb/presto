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

import com.google.common.annotations.VisibleForTesting;
import org.skife.jdbi.v2.sqlobject.Bind;
import org.skife.jdbi.v2.sqlobject.BindBean;
import org.skife.jdbi.v2.sqlobject.GetGeneratedKeys;
import org.skife.jdbi.v2.sqlobject.SqlQuery;
import org.skife.jdbi.v2.sqlobject.SqlUpdate;
import org.skife.jdbi.v2.sqlobject.customizers.Mapper;

import java.util.List;

/**
 * Provides access to the periodic importer schema.
 * <p/>
 * The job table that described import jobs and their interval,
 * the run table keeps a log of the executed import runs and their outcome.
 */
public interface PeriodicImportDao
{
    //
    // Job table Methods
    //

    @SqlUpdate("CREATE TABLE IF NOT EXISTS job (\n" +
            "  job_id BIGINT PRIMARY KEY AUTO_INCREMENT,\n" +
            "  src_catalog_name VARCHAR(255) NOT NULL,\n" +
            "  src_schema_name VARCHAR(255) NOT NULL,\n" +
            "  src_table_name VARCHAR(255) NOT NULL,\n" +
            "  dst_catalog_name VARCHAR(255) NOT NULL,\n" +
            "  dst_schema_name VARCHAR(255) NOT NULL,\n" +
            "  dst_table_name VARCHAR(255) NOT NULL,\n" +
            "  job_interval_seconds INT NOT NULL,\n" +
            "  enabled BOOLEAN NOT NULL DEFAULT TRUE\n" +
            ")")
    void createJobTable();

    @SqlUpdate("INSERT INTO job \n" +
            "(src_catalog_name, src_schema_name, src_table_name,dst_catalog_name, dst_schema_name, dst_table_name, job_interval_seconds)\n" +
            "VALUES (:srcCatalogName, :srcSchemaName, :srcTableName, :dstCatalogName, :dstSchemaName, :dstTableName, :intervalSeconds)")
    @GetGeneratedKeys
    long insertJob(@BindBean PeriodicImportJob job);

    @SqlUpdate("UPDATE job SET enabled = FALSE\n" +
            "WHERE job_id = :jobId\n")
    void dropJob(@Bind("jobId") long jobId);

    @SqlQuery("SELECT COUNT(*) FROM job WHERE enabled = :enabled")
    long getJobCount(@Bind("enabled") boolean enabled);

    @SqlQuery("SELECT * FROM job WHERE job_id = :jobId")
    @Mapper(PersistentPeriodicImportJob.PersistentPeriodicImportJobMapper.class)
    PersistentPeriodicImportJob getJob(@Bind("jobId") long jobId);

    @SqlQuery("SELECT * FROM job WHERE enabled = :enabled")
    @Mapper(PersistentPeriodicImportJob.PersistentPeriodicImportJobMapper.class)
    List<PersistentPeriodicImportJob> getJobs(@Bind("enabled") boolean enabled);

    //
    // Run table methods
    //

    @SqlUpdate("CREATE TABLE IF NOT EXISTS run (\n" +
            "  run_id BIGINT PRIMARY KEY AUTO_INCREMENT,\n" +
            "  job_id BIGINT NOT NULL,\n" +
            "  node_id VARCHAR(255) NOT NULL,\n" +
            "  job_start_time DATETIME NOT NULL,\n" +
            "  job_end_time DATETIME,\n" +
            "  success BOOLEAN NOT NULL DEFAULT FALSE,\n" +
            "  FOREIGN KEY (job_id) REFERENCES job (job_id) ON DELETE CASCADE\n" +
            ")")
    void createRunTable();

    @SqlUpdate("INSERT INTO run (job_id, node_id, job_start_time) VALUES (:jobId, :nodeId, CURRENT_TIMESTAMP)")
    @GetGeneratedKeys
    long beginRun(@Bind("jobId") long jobId, @Bind("nodeId") String nodeId);

    @SqlUpdate("UPDATE run SET success = :success,\n" +
            "  job_end_time = CURRENT_TIMESTAMP\n" +
            "  WHERE run_id = :runId\n")
    void finishRun(@Bind("runId") long runId, @Bind("success") boolean success);

    @SqlQuery("SELECT COUNT(*) FROM run\n" +
            "  WHERE success = :success")
    long getRunCount(@Bind("success") boolean success);

    @SqlQuery("SELECT run.*\n" +
            "  FROM run\n" +
            "  JOIN (SELECT job_id, MAX(job_start_time) AS job_start_time FROM run GROUP BY job_id) AS a\n" +
            "  ON (run.job_id = a.job_id AND run.job_start_time = a.job_start_time)\n" +
            "  WHERE job_end_time IS NULL")
    @Mapper(PeriodicImportRun.PeriodicImportRunMapper.class)
    @VisibleForTesting
    List<PeriodicImportRun> getJobsStarted();

    @SqlQuery("SELECT run.*\n" +
            "  FROM run\n" +
            "  JOIN (SELECT job_id, MAX(job_start_time) AS job_start_time FROM run GROUP BY job_id) AS a\n" +
            "  ON (run.job_id = a.job_id AND run.job_start_time = a.job_start_time)\n" +
            "  WHERE job_end_time IS NOT NULL AND success = :success")
    @Mapper(PeriodicImportRun.PeriodicImportRunMapper.class)
    @VisibleForTesting
    List<PeriodicImportRun> getJobsFinished(@Bind("success") boolean success);

    @SqlQuery("SELECT * FROM run WHERE run_id = :runId")
    @Mapper(PeriodicImportRun.PeriodicImportRunMapper.class)
    @VisibleForTesting
    PeriodicImportRun getRun(@Bind("runId") long runId);

    public static final class Utils
    {
        public static void createTables(PeriodicImportDao dao)
        {
            dao.createJobTable();
            dao.createRunTable();
        }
    }
}
