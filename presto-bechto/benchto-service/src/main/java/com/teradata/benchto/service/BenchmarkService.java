/*
 * Copyright 2013-2016, Teradata, Inc. All rights reserved.
 */
package com.teradata.benchto.service;

import com.teradata.benchto.service.model.AggregatedMeasurement;
import com.teradata.benchto.service.model.BenchmarkRun;
import com.teradata.benchto.service.model.BenchmarkRunExecution;
import com.teradata.benchto.service.model.Environment;
import com.teradata.benchto.service.model.Measurement;
import com.teradata.benchto.service.model.Status;
import com.teradata.benchto.service.repo.BenchmarkRunRepo;
import org.hibernate.Hibernate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.dao.TransientDataAccessException;
import org.springframework.retry.annotation.Retryable;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.sql.Timestamp;
import java.time.Duration;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static com.teradata.benchto.service.model.Environment.DEFAULT_ENVIRONMENT_NAME;
import static com.teradata.benchto.service.model.Status.STARTED;
import static com.teradata.benchto.service.utils.BenchmarkUniqueNameUtils.generateBenchmarkUniqueName;
import static com.teradata.benchto.service.utils.TimeUtils.currentDateTime;
import static java.lang.String.format;

@Service
public class BenchmarkService
{
    private static final Logger LOG = LoggerFactory.getLogger(BenchmarkService.class);

    @Autowired
    private BenchmarkRunRepo benchmarkRunRepo;

    @Autowired
    private EnvironmentService environmentService;

    @Retryable(value = {TransientDataAccessException.class, DataIntegrityViolationException.class}, maxAttempts = 1)
    @Transactional
    public String startBenchmarkRun(String uniqueName, String name, String sequenceId, Optional<String> environmentName, Map<String, String> variables,
            Map<String, String> attributes)
    {
        String generatedUniqueName = generateBenchmarkUniqueName(name, variables);
        checkArgument(uniqueName.equals(generatedUniqueName), "Passed unique benchmark name (%s) does not match generated one: (%s) - name: %s, variables: %s",
                uniqueName, generatedUniqueName, name, variables);

        BenchmarkRun benchmarkRun = benchmarkRunRepo.findByUniqueNameAndSequenceId(uniqueName, sequenceId);
        if (benchmarkRun == null) {
            Environment environment = environmentService.findEnvironment(environmentName.orElse(DEFAULT_ENVIRONMENT_NAME));
            benchmarkRun = new BenchmarkRun(name, sequenceId, variables, uniqueName);
            benchmarkRun.setStatus(STARTED);
            benchmarkRun.setEnvironment(environment);
            benchmarkRun.getAttributes().putAll(attributes);
            benchmarkRun.setStarted(currentDateTime());
            benchmarkRunRepo.save(benchmarkRun);
        }
        LOG.debug("Starting benchmark - {}", benchmarkRun);

        return benchmarkRun.getUniqueName();
    }

    @Retryable(value = {TransientDataAccessException.class, DataIntegrityViolationException.class}, maxAttempts = 1)
    @Transactional
    public void finishBenchmarkRun(String uniqueName, String sequenceId, Status status, List<Measurement> measurements, Map<String, String> attributes)
    {
        BenchmarkRun benchmarkRun = findBenchmarkRun(uniqueName, sequenceId);
        benchmarkRun.getMeasurements().addAll(measurements);
        benchmarkRun.getAttributes().putAll(attributes);
        benchmarkRun.setEnded(currentDateTime());
        benchmarkRun.setStatus(status);
        AggregatedMeasurement durationAggregatedMeasurement = benchmarkRun.getAggregatedMeasurements().get("duration");
        if (durationAggregatedMeasurement != null) {
            benchmarkRun.setExecutionsMeanDuration(durationAggregatedMeasurement.getMean());
            benchmarkRun.setExecutionStdDevDuration(durationAggregatedMeasurement.getStdDev());
        }
        LOG.debug("Finishing benchmark - {}", benchmarkRun);
    }

    @Retryable(value = {TransientDataAccessException.class, DataIntegrityViolationException.class}, maxAttempts = 1)
    @Transactional
    public void startExecution(String uniqueName, String benchmarkSequenceId, String executionSequenceId, Map<String, String> attributes)
    {
        BenchmarkRun benchmarkRun = findBenchmarkRun(uniqueName, benchmarkSequenceId);
        verifyBenchmarkRunInStartedStatus(benchmarkRun);

        boolean executionPresent = benchmarkRun.getExecutions().stream()
                .filter(e -> executionSequenceId.equals(e.getSequenceId()))
                .findAny()
                .isPresent();
        if (executionPresent) {
            LOG.debug("Execution ({}) already present for benchmark ({})", executionSequenceId, benchmarkRun);
            return;
        }

        LOG.debug("Starting new execution ({}) for benchmark ({})", executionSequenceId, benchmarkRun);
        BenchmarkRunExecution execution = new BenchmarkRunExecution();
        execution.setSequenceId(executionSequenceId);
        execution.setStatus(STARTED);
        execution.setStarted(currentDateTime());
        execution.setBenchmarkRun(benchmarkRun);
        execution.getAttributes().putAll(attributes);
        benchmarkRun.getExecutions().add(execution);
    }

    @Retryable(value = {TransientDataAccessException.class, DataIntegrityViolationException.class}, maxAttempts = 1)
    @Transactional
    public void finishExecution(String uniqueName, String benchmarkSequenceId, String executionSequenceId, Status status,
            List<Measurement> measurements, Map<String, String> attributes)
    {
        BenchmarkRun benchmarkRun = findBenchmarkRun(uniqueName, benchmarkSequenceId);
        verifyBenchmarkRunInStartedStatus(benchmarkRun);

        BenchmarkRunExecution execution = benchmarkRun.getExecutions().stream()
                .filter(e -> executionSequenceId.equals(e.getSequenceId()))
                .findAny().get();

        execution.getMeasurements().addAll(measurements);
        execution.getAttributes().putAll(attributes);
        execution.setEnded(currentDateTime());
        execution.setStatus(status);
    }

    @Transactional(readOnly = true)
    public BenchmarkRun findBenchmarkRun(String uniqueName, String sequenceId)
    {
        BenchmarkRun benchmarkRun = benchmarkRunRepo.findByUniqueNameAndSequenceId(uniqueName, sequenceId);
        if (benchmarkRun == null) {
            throw new IllegalArgumentException("Could not find benchmark " + uniqueName + " - " + sequenceId);
        }
        Hibernate.initialize(benchmarkRun.getExecutions());
        return benchmarkRun;
    }

    @Transactional(readOnly = true)
    public List<BenchmarkRun> findBenchmark(String uniqueName, String environmentName)
    {
        List<BenchmarkRun> benchmarkRuns = benchmarkRunRepo.findByUniqueNameAndEnvironmentOrderBySequenceIdDesc(uniqueName, findEnvironment(environmentName));
        for (BenchmarkRun benchmarkRun : benchmarkRuns) {
            Hibernate.initialize(benchmarkRun.getExecutions());
        }
        return benchmarkRuns;
    }

    private Environment findEnvironment(String environmentName)
    {
        return environmentService.findEnvironment(environmentName);
    }

    @Transactional(readOnly = true)
    public List<BenchmarkRun> findLatest(String environmentName)
    {
        return benchmarkRunRepo.findLatest(findEnvironment(environmentName).getId());
    }

    public String generateUniqueBenchmarkName(String name, Map<String, String> variables)
    {
        LOG.debug("Generating unique benchmark name for: name = {}, variables = {}", name, variables);
        return generateBenchmarkUniqueName(name, variables);
    }

    private void verifyBenchmarkRunInStartedStatus(BenchmarkRun benchmarkRun)
    {
        if (benchmarkRun.getStatus() != STARTED) {
            throw new IllegalArgumentException(format("Benchmark run %s - %s in %s status", benchmarkRun.getName(), benchmarkRun.getSequenceId(), benchmarkRun.getStatus()));
        }
    }

    public Duration getSuccessfulExecutionAge(String uniqueName)
    {
        Timestamp ended = benchmarkRunRepo.findTimeOfLatestSuccessfulExecution(uniqueName);
        if (ended == null) {
            return Duration.ofDays(Integer.MAX_VALUE);
        }
        ZonedDateTime endedAsZDT = ZonedDateTime.of(ended.toLocalDateTime(), ZoneId.systemDefault());
        return Duration.between(endedAsZDT, currentDateTime());
    }
}
