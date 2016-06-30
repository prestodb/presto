/*
 * Copyright 2013-2016, Teradata, Inc. All rights reserved.
 */
package com.teradata.benchto.service;

import com.teradata.benchto.service.category.IntegrationTest;
import com.teradata.benchto.service.model.BenchmarkRun;
import com.teradata.benchto.service.model.BenchmarkRunExecution;
import com.teradata.benchto.service.model.Environment;
import com.teradata.benchto.service.repo.BenchmarkRunRepo;
import com.teradata.benchto.service.repo.EnvironmentRepo;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.springframework.beans.factory.annotation.Autowired;

import java.time.ZonedDateTime;

import static com.teradata.benchto.service.model.MeasurementUnit.BYTES;
import static com.teradata.benchto.service.model.MeasurementUnit.MILLISECONDS;
import static com.teradata.benchto.service.model.Status.ENDED;
import static com.teradata.benchto.service.model.Status.FAILED;
import static com.teradata.benchto.service.utils.TimeUtils.currentDateTime;
import static org.assertj.core.api.Assertions.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.springframework.http.MediaType.APPLICATION_JSON;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@Category(IntegrationTest.class)
public class BenchmarkControllerTest
        extends IntegrationTestBase
{

    @Autowired
    private BenchmarkRunRepo benchmarkRunRepo;

    @Autowired
    private EnvironmentRepo environmentRepo;

    @Test
    public void benchmarkStartEndHappyPath()
            throws Exception
    {
        String environmentName = "environmentName";
        String benchmarkName = "benchmarkName";
        String uniqueName = "benchmarkName_k1=v1";
        String benchmarkSequenceId = "benchmarkSequenceId";
        String executionSequenceId = "executionSequenceId";
        ZonedDateTime testStart = currentDateTime();

        // create environment
        mvc.perform(post("/v1/environment/{environmentName}", environmentName)
                .contentType(APPLICATION_JSON)
                .content("{\"attribute1\": \"value1\", \"attribute2\": \"value2\"}"))
                .andExpect(status().isOk());

        // get environment
        mvc.perform(get("/v1/environment/{environmentName}", environmentName))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.name", is(environmentName)))
                .andExpect(jsonPath("$.attributes.attribute1", is("value1")))
                .andExpect(jsonPath("$.attributes.attribute2", is("value2")));

        // start benchmark
        mvc.perform(post("/v1/benchmark/{benchmarkName}/{benchmarkSequenceId}/start", uniqueName, benchmarkSequenceId)
                .contentType(APPLICATION_JSON)
                .content("{\"name\": \"" + benchmarkName + "\",\"environmentName\": \"" + environmentName + "\", \"variables\":{\"k1\":\"v1\"}}"))
                .andExpect(status().isOk())
                .andExpect(content().string(uniqueName));

        // get benchmark - no measurements, no executions
        mvc.perform(get("/v1/benchmark/{uniqueName}/{benchmarkSequenceId}", uniqueName, benchmarkSequenceId))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.name", is(benchmarkName)))
                .andExpect(jsonPath("$.uniqueName", is(uniqueName)))
                .andExpect(jsonPath("$.status", is("STARTED")))
                .andExpect(jsonPath("$.sequenceId", is(benchmarkSequenceId)))
                .andExpect(jsonPath("$.environment.name", is(environmentName)))
                .andExpect(jsonPath("$.variables.k1", is("v1")))
                .andExpect(jsonPath("$.measurements", hasSize(0)))
                .andExpect(jsonPath("$.executions", hasSize(0)));

        // start execution
        mvc.perform(post("/v1/benchmark/{uniqueName}/{benchmarkSequenceId}/execution/{executionSequenceId}/start",
                uniqueName, benchmarkSequenceId, executionSequenceId)
                .contentType(APPLICATION_JSON)
                .content("{\"attributes\": {}}"))
                .andExpect(status().isOk());

        // get benchmark - no measurements, single execution without measurements
        mvc.perform(get("/v1/benchmark/{uniqueName}/{benchmarkSequenceId}", uniqueName, benchmarkSequenceId))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.name", is(benchmarkName)))
                .andExpect(jsonPath("$.uniqueName", is(uniqueName)))
                .andExpect(jsonPath("$.status", is("STARTED")))
                .andExpect(jsonPath("$.sequenceId", is(benchmarkSequenceId)))
                .andExpect(jsonPath("$.environment.name", is(environmentName)))
                .andExpect(jsonPath("$.measurements", hasSize(0)))
                .andExpect(jsonPath("$.executions", hasSize(1)))
                .andExpect(jsonPath("$.variables.k1", is("v1")))
                .andExpect(jsonPath("$.executions[0].status", is("STARTED")))
                .andExpect(jsonPath("$.executions[0].sequenceId", is(executionSequenceId)));

        // finish execution - post execution measurements
        mvc.perform(post("/v1/benchmark/{uniqueName}/{benchmarkSequenceId}/execution/{executionSequenceId}/finish",
                uniqueName, benchmarkSequenceId, executionSequenceId)
                .contentType(APPLICATION_JSON)
                .content("{\"measurements\":[{\"name\": \"duration\", \"value\": 12.34, \"unit\": \"MILLISECONDS\"},{\"name\": \"bytes\", \"value\": 56789.0, \"unit\": \"BYTES\"}]," +
                        "\"attributes\":{\"attribute1\": \"value1\"}, \"status\": \"FAILED\"}"))
                .andExpect(status().isOk());

        // finish benchmark - post benchmark measurements
        mvc.perform(post("/v1/benchmark/{uniqueName}/{benchmarkSequenceId}/finish", uniqueName, benchmarkSequenceId)
                .contentType(APPLICATION_JSON)
                .content("{\"measurements\":[{\"name\": \"meanDuration\", \"value\": 12.34, \"unit\": \"MILLISECONDS\"},{\"name\": \"sumBytes\", \"value\": 56789.0, \"unit\": \"BYTES\"}]," +
                        "\"attributes\":{\"attribute1\": \"value1\"}, \"status\": \"ENDED\"}"))
                .andExpect(status().isOk());

        ZonedDateTime testEnd = currentDateTime();

        mvc.perform(get("/v1/benchmark/{uniqueName}?environment={environment}", uniqueName, environmentName))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.[0].name", is(benchmarkName)))
                .andExpect(jsonPath("$.[0].uniqueName", is(uniqueName)))
                .andExpect(jsonPath("$.[0].sequenceId", is(benchmarkSequenceId)))
                .andExpect(jsonPath("$.[0].status", is("ENDED")))
                .andExpect(jsonPath("$.[0].variables.k1", is("v1")))
                .andExpect(jsonPath("$.[0].environment.name", is(environmentName)))
                .andExpect(jsonPath("$.[0].attributes.attribute1", is("value1")))
                .andExpect(jsonPath("$.[0].measurements", hasSize(2)))
                .andExpect(jsonPath("$.[0].measurements[*].name", containsInAnyOrder("meanDuration", "sumBytes")))
                .andExpect(jsonPath("$.[0].measurements[*].value", containsInAnyOrder(12.34, 56789.0)))
                .andExpect(jsonPath("$.[0].measurements[*].unit", containsInAnyOrder("MILLISECONDS", "BYTES")))
                .andExpect(jsonPath("$.[0].executions", hasSize(1)))
                .andExpect(jsonPath("$.[0].executions[0].sequenceId", is(executionSequenceId)))
                .andExpect(jsonPath("$.[0].executions[0].status", is("FAILED")))
                .andExpect(jsonPath("$.[0].executions[0].attributes.attribute1", is("value1")))
                .andExpect(jsonPath("$.[0].executions[0].measurements[*].name", containsInAnyOrder("duration", "bytes")))
                .andExpect(jsonPath("$.[0].executions[0].measurements[*].value", containsInAnyOrder(12.34, 56789.0)))
                .andExpect(jsonPath("$.[0].executions[0].measurements[*].unit", containsInAnyOrder("MILLISECONDS", "BYTES")));

        // assert database state
        withinTransaction(() -> {
            Environment environment = environmentRepo.findByName(environmentName);
            assertThat(environment).isNotNull();
            assertThat(environment.getName()).isEqualTo(environmentName);
            assertThat(environment.getAttributes().get("attribute1")).isEqualTo("value1");
            assertThat(environment.getAttributes().get("attribute2")).isEqualTo("value2");

            BenchmarkRun benchmarkRun = benchmarkRunRepo.findByUniqueNameAndSequenceId(uniqueName, benchmarkSequenceId);
            assertThat(benchmarkRun).isNotNull();
            assertThat(benchmarkRun.getId()).isGreaterThan(0);
            assertThat(benchmarkRun.getName()).isEqualTo(benchmarkName);
            assertThat(benchmarkRun.getVariables()).containsEntry("k1", "v1");
            assertThat(benchmarkRun.getUniqueName()).isEqualTo(uniqueName);
            assertThat(benchmarkRun.getSequenceId()).isEqualTo(benchmarkSequenceId);
            assertThat(benchmarkRun.getStatus()).isEqualTo(ENDED);
            assertThat(benchmarkRun.getMeasurements())
                    .hasSize(2)
                    .extracting("unit").contains(BYTES, MILLISECONDS);
            assertThat(benchmarkRun.getStarted())
                    .isAfter(testStart)
                    .isBefore(testEnd);
            assertThat(benchmarkRun.getEnded())
                    .isAfter(testStart)
                    .isBefore(testEnd);
            assertThat(benchmarkRun.getExecutions())
                    .hasSize(1);

            BenchmarkRunExecution execution = benchmarkRun.getExecutions().iterator().next();
            assertThat(execution.getId()).isGreaterThan(0);
            assertThat(execution.getSequenceId()).isEqualTo(executionSequenceId);
            assertThat(execution.getStatus()).isEqualTo(FAILED);
            assertThat(execution.getMeasurements())
                    .hasSize(2)
                    .extracting("name").contains("duration", "bytes");
            assertThat(execution.getStarted())
                    .isAfter(testStart)
                    .isBefore(testEnd);
            assertThat(execution.getEnded())
                    .isAfter(testStart)
                    .isBefore(testEnd);
        });
    }

    @Test
    public void testJsr303Validation()
            throws Exception
    {
        String environmentName = generateStringOfLength('T', 100);
        String benchmarkName = "benchmarkName";
        String benchmarkSequenceId = "benchmarkSequenceId";

        // environment name larger than max 64 bytes - we should get bad request response - 4XX
        mvc.perform(post("//v1/benchmark/{benchmarkName}/{benchmarkSequenceId}/start", benchmarkName, benchmarkSequenceId)
                .contentType(APPLICATION_JSON)
                .content("{\"environmentName\": \"" + environmentName + "\"}"))
                .andExpect(status().is4xxClientError());
    }

    public String generateStringOfLength(char ch, int length)
    {
        StringBuffer str = new StringBuffer(length);
        for (int i = 0; i < length; i++) {
            str.append(ch);
        }
        return str.toString();
    }
}
