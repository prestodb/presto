/*
 * Copyright 2013-2016, Teradata, Inc. All rights reserved.
 */
package com.teradata.benchto.service;

import com.google.common.collect.ImmutableMap;
import com.teradata.benchto.service.category.IntegrationTest;
import com.teradata.benchto.service.model.BenchmarkRun;
import com.teradata.benchto.service.model.Environment;
import com.teradata.benchto.service.repo.BenchmarkRunRepo;
import com.teradata.benchto.service.repo.EnvironmentRepo;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.springframework.beans.factory.annotation.Autowired;

import java.time.ZonedDateTime;

import static com.teradata.benchto.service.CleanerService.BENCHMARK_TIMEOUT_HOURS;
import static com.teradata.benchto.service.model.Status.FAILED;
import static com.teradata.benchto.service.model.Status.STARTED;
import static com.teradata.benchto.service.utils.TimeUtils.currentDateTime;
import static org.assertj.core.api.Assertions.assertThat;

@Category(IntegrationTest.class)
public class CleanerServiceTest
        extends IntegrationTestBase
{
    private static final String UNIQUE_NAME = "unique name";
    private static final String SEQUENCE_ID = "sequencId";

    @Autowired
    private BenchmarkRunRepo benchmarkRunRepo;

    @Autowired
    private CleanerService cleanerService;

    @Autowired
    private EnvironmentRepo environmentRepo;

    @Test
    public void cleanUpStaleBenchmarks()
            throws Exception
    {
        Environment environment = new Environment();
        environmentRepo.save(environment);

        ZonedDateTime currentDate = currentDateTime();
        BenchmarkRun staleBenchmark = new BenchmarkRun("stale benchmark test", SEQUENCE_ID, ImmutableMap.of(), UNIQUE_NAME);
        staleBenchmark.setStatus(STARTED);
        staleBenchmark.setStarted(currentDate.minusHours(BENCHMARK_TIMEOUT_HOURS).minusMinutes(1));
        staleBenchmark.setEnvironment(environment);
        benchmarkRunRepo.save(staleBenchmark);

        cleanerService.cleanUpStaleBenchmarks();

        BenchmarkRun benchmarkRun = benchmarkRunRepo.findByUniqueNameAndSequenceId(UNIQUE_NAME, SEQUENCE_ID);
        assertThat(benchmarkRun.getStatus()).isEqualTo(FAILED);
        assertThat(benchmarkRun.getEnded()).isAfter(currentDate);
    }
}
