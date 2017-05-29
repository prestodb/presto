/*
 * Copyright 2013-2016, Teradata, Inc. All rights reserved.
 */
package com.teradata.benchto.service.rest;

import com.teradata.benchto.service.BenchmarkService;
import com.teradata.benchto.service.model.BenchmarkRun;
import com.teradata.benchto.service.rest.requests.BenchmarkStartRequest;
import com.teradata.benchto.service.rest.requests.ExecutionStartRequest;
import com.teradata.benchto.service.rest.requests.FinishRequest;
import com.teradata.benchto.service.rest.requests.GenerateBenchmarkNamesRequestItem;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import javax.validation.Valid;

import java.time.Duration;
import java.util.List;

import static com.teradata.benchto.service.utils.CollectionUtils.failSafeEmpty;
import static java.util.Optional.ofNullable;
import static java.util.stream.Collectors.toList;
import static org.springframework.web.bind.annotation.RequestMethod.GET;
import static org.springframework.web.bind.annotation.RequestMethod.POST;

@RestController
public class BenchmarkController
{

    @Autowired
    private BenchmarkService benchmarkService;

    @RequestMapping(value = "/v1/benchmark/generate-unique-names", method = POST)
    public List<String> generateUniqueBenchmarkNames(@RequestBody List<GenerateBenchmarkNamesRequestItem> generateItems)
    {
        return generateItems.stream()
                .map(requestItem -> benchmarkService.generateUniqueBenchmarkName(requestItem.getName(), requestItem.getVariables()))
                .collect(toList());
    }

    @RequestMapping(value = "/v1/benchmark/get-successful-execution-ages", method = POST)
    public List<Duration> getExecutionAges(@RequestBody List<String> uniqueBenchmarkNames)
    {
        return uniqueBenchmarkNames.stream()
                .map(uniqueName -> benchmarkService.getSuccessfulExecutionAge(uniqueName))
                .collect(toList());
    }

    @RequestMapping(value = "/v1/benchmark/{uniqueName}/{benchmarkSequenceId}/start", method = POST)
    public String startBenchmark(
            @PathVariable("uniqueName") String uniqueName,
            @PathVariable("benchmarkSequenceId") String benchmarkSequenceId,
            @RequestBody @Valid BenchmarkStartRequest startRequest)
    {
        return benchmarkService.startBenchmarkRun(uniqueName,
                startRequest.getName(),
                benchmarkSequenceId,
                ofNullable(startRequest.getEnvironmentName()),
                failSafeEmpty(startRequest.getVariables()),
                failSafeEmpty(startRequest.getAttributes()));
    }

    @RequestMapping(value = "/v1/benchmark/{uniqueName}/{benchmarkSequenceId}/finish", method = POST)
    public void finishBenchmark(
            @PathVariable("uniqueName") String uniqueName,
            @PathVariable("benchmarkSequenceId") String benchmarkSequenceId,
            @RequestBody @Valid FinishRequest finishRequest)
    {
        benchmarkService.finishBenchmarkRun(uniqueName,
                benchmarkSequenceId,
                finishRequest.getStatus(),
                failSafeEmpty(finishRequest.getMeasurements()),
                failSafeEmpty(finishRequest.getAttributes()));
    }

    @RequestMapping(value = "/v1/benchmark/{uniqueName}/{benchmarkSequenceId}/execution/{executionSequenceId}/start", method = POST)
    public void startExecution(
            @PathVariable("uniqueName") String uniqueName,
            @PathVariable("benchmarkSequenceId") String benchmarkSequenceId,
            @PathVariable("executionSequenceId") String executionSequenceId,
            @RequestBody @Valid ExecutionStartRequest startRequest)
    {
        benchmarkService.startExecution(uniqueName,
                benchmarkSequenceId,
                executionSequenceId,
                failSafeEmpty(startRequest.getAttributes()));
    }

    @RequestMapping(value = "/v1/benchmark/{uniqueName}/{benchmarkSequenceId}/execution/{executionSequenceId}/finish", method = POST)
    public void finishExecution(
            @PathVariable("uniqueName") String uniqueName,
            @PathVariable("benchmarkSequenceId") String benchmarkSequenceId,
            @PathVariable("executionSequenceId") String executionSequenceId,
            @RequestBody @Valid FinishRequest finishRequest)
    {
        benchmarkService.finishExecution(uniqueName,
                benchmarkSequenceId,
                executionSequenceId,
                finishRequest.getStatus(),
                failSafeEmpty(finishRequest.getMeasurements()),
                failSafeEmpty(finishRequest.getAttributes()));
    }

    @RequestMapping(value = "/v1/benchmark/{uniqueName}/{benchmarkSequenceId}", method = GET)
    public BenchmarkRun findBenchmark(
            @PathVariable("uniqueName") String uniqueName,
            @PathVariable("benchmarkSequenceId") String benchmarkSequenceId)
    {
        return benchmarkService.findBenchmarkRun(uniqueName, benchmarkSequenceId);
    }

    @RequestMapping(value = "/v1/benchmark/{uniqueName}", method = GET)
    public List<BenchmarkRun> findBenchmarks(
            @PathVariable("uniqueName") String uniqueName,
            @RequestParam("environment") String environmentName)
    {
        return benchmarkService.findBenchmark(uniqueName, environmentName);
    }

    @RequestMapping(value = "/v1/benchmark/latest/{environmentName}", method = GET)
    public List<BenchmarkRun> findLatestBenchmarkRuns(
            @PathVariable("environmentName") String environmentName
    )
    {
        return benchmarkService.findLatest(environmentName);
    }
}
