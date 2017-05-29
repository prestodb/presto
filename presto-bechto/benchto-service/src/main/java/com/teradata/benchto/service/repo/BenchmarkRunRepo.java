/*
 * Copyright 2013-2016, Teradata, Inc. All rights reserved.
 */
package com.teradata.benchto.service.repo;

import com.teradata.benchto.service.model.BenchmarkRun;
import com.teradata.benchto.service.model.Environment;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.sql.Timestamp;
import java.time.ZonedDateTime;
import java.util.List;

@Repository
public interface BenchmarkRunRepo
        extends JpaRepository<BenchmarkRun, String>
{
    BenchmarkRun findByUniqueNameAndSequenceId(String uniqueName, String sequenceId);

    List<BenchmarkRun> findByUniqueNameAndEnvironmentOrderBySequenceIdDesc(String uniqueName, Environment environment);

    @Query(value = "" +
            "WITH summary AS ( " +
            "  SELECT " +
            "    b.id, " +
            "    b.name, " +
            "    b.unique_name, " +
            "    b.sequence_id, " +
            "    b.status, " +
            "    b.version, " +
            "    b.started, " +
            "    b.ended, " +
            "    b.environment_id, " +
            "    b.executions_mean_duration, " +
            "    b.executions_stddev_duration, " +
            "    rank() " +
            "    OVER (PARTITION BY b.unique_name, b.environment_id " +
            "      ORDER BY b.sequence_id DESC) AS rk " +
            "  FROM benchmark_runs b " +
            "  WHERE b.environment_id = :environment_id " +
            ") " +
            "SELECT s.* " +
            "FROM summary s " +
            "WHERE s.rk = 1 " +
            "ORDER BY s.started DESC ",
            nativeQuery = true)
    List<BenchmarkRun> findLatest(@Param("environment_id") long environmentId);

    @Query("SELECT br FROM BenchmarkRun br WHERE " +
            "br.status = 'STARTED' AND " +
            "br.started <= :startDate")
    List<BenchmarkRun> findStartedBefore(@Param("startDate") ZonedDateTime startDate);

    @Query(value = "" +
            "SELECT MAX(ended) " +
            "FROM benchmark_runs " +
            "WHERE unique_name = :uniqueName and status = 'ENDED'",
            nativeQuery = true)
    Timestamp findTimeOfLatestSuccessfulExecution(@Param("uniqueName") String uniqueName);
}
