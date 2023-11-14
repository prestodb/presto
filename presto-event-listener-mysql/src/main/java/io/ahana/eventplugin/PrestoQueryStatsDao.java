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
package io.ahana.eventplugin;

import org.jdbi.v3.sqlobject.customizer.Bind;
import org.jdbi.v3.sqlobject.statement.SqlUpdate;

public interface PrestoQueryStatsDao
{
    @SqlUpdate("INSERT into presto_query_creation_info(\n" +
            "query_id, \n" +
            "query, \n" +
            "create_time, \n" +
            "schema_name, \n" +
            "catalog_name, \n" +
            "environment, \n" +
            "user, \n" +
            "remote_client_address, \n" +
            "source, \n" +
            "user_agent, \n" +
            "uri, \n" +
            "session_properties_json, \n" +
            "server_version, \n" +
            "client_info, \n" +
            "resource_group_name, \n" +
            "principal, \n" +
            "transaction_id, \n" +
            "client_tags, \n" +
            "resource_estimates, \n" +
            "dt) \n" +
            "VALUES( \n" +
            ":query_id, " +
            ":query, " +
            ":create_time, " +
            ":schema_name," +
            ":catalog_name, " +
            ":environment," +
            ":user, " +
            ":remote_client_address, " +
            ":source, " +
            ":user_agent," +
            ":uri, " +
            ":session_properties_json, " +
            ":server_version," +
            ":client_info, " +
            ":resource_group_name, " +
            ":principal, " +
            ":transaction_id, " +
            ":client_tags, " +
            ":resource_estimates, " +
            ":dt)")
    void insertQueryCreationInfo(
            @Bind("query_id") String queryId,
            @Bind("query") String query,
            @Bind("create_time") String createTime,
            @Bind("schema_name") String schemName,
            @Bind("catalog_name") String catalogName,
            @Bind("environment") String environment,
            @Bind("user") String user,
            @Bind("remote_client_address") String remoteClientAddress,
            @Bind("source") String source,
            @Bind("user_agent") String userAgent,
            @Bind("uri") String uri,
            @Bind("session_properties_json") String sessionPropertiesJson,
            @Bind("server_version") String serverVersion,
            @Bind("client_info") String clientInfo,
            @Bind("resource_group_name") String resourceGroupName,
            @Bind("principal") String principal,
            @Bind("transaction_id") String transactionId,
            @Bind("client_tags") String clientGags,
            @Bind("resource_estimates") String resourceEstimates,
            @Bind("dt") String dt);

    @SqlUpdate("INSERT into presto_query_plans( \n" +
            "query_id, \n" +
            "query, \n" +
            "plan, \n" +
            "json_plan, \n" +
            "environment, \n" +
            "dt) \n" +
            "VALUES( \n" +
            ":query_id, " +
            ":query, " +
            ":plan," +
            ":json_plan," +
            ":environment," +
            ":dt)")
    void insertQueryPlans(
            @Bind("query_id") String queryId,
            @Bind("query") String query,
            @Bind("plan") String plan,
            @Bind("json_plan") String jsonPlan,
            @Bind("environment") String environment,
            @Bind("dt") String dt);

    @SqlUpdate("INSERT into presto_query_stage_stats(\n" +
            "query_id, \n" +
            "stage_id, \n" +
            "stage_execution_id, \n" +
            "tasks, \n" +
            "total_scheduled_time_ms, \n" +
            "total_cpu_time_ms, \n" +
            "retried_cpu_time_ms, \n" +
            "total_blocked_time_ms, \n" +
            "raw_input_data_size_bytes, \n" +
            "processed_input_data_size_bytes, \n" +
            "physical_written_data_size_bytes, \n" +
            "gc_statistics, \n" +
            "cpu_distribution, \n" +
            "memory_distribution, \n" +
            "dt) \n" +
            "VALUES(" +
            ":query_id," +
            ":stage_id," +
            ":stage_execution_id," +
            ":tasks," +
            ":total_scheduled_time_ms," +
            ":total_cpu_time_ms," +
            ":retried_cpu_time_ms," +
            ":total_blocked_time_ms," +
            ":raw_input_data_size_bytes," +
            ":processed_input_data_size_bytes," +
            ":physical_written_data_size_bytes," +
            ":gc_statistics," +
            ":cpu_distribution," +
            ":memory_distribution," +
            ":dt)")
    void insertQueryStageStats(
            @Bind("query_id") String queryId,
            @Bind("stage_id") int stageId,
            @Bind("stage_execution_id") int stageExecutionId,
            @Bind("tasks") int tasks,
            @Bind("total_scheduled_time_ms") long totalScheduledTimeMs,
            @Bind("total_cpu_time_ms") long totalCpuTimeMs,
            @Bind("retried_cpu_time_ms") long retriedCpuTimeMs,
            @Bind("total_blocked_time_ms") long totalBlockedTimeMs,
            @Bind("raw_input_data_size_bytes") long rawInputDataSizeBytes,
            @Bind("processed_input_data_size_bytes") long processedInputDataSizeBytes,
            @Bind("physical_written_data_size_bytes") long physicalWrittenDataSizeBytes,
            @Bind("gc_statistics") String gcStatistics,
            @Bind("cpu_distribution") String cpuDistribution,
            @Bind("memory_distribution") String memoryDistribution,
            @Bind("dt") String dt);

    @SqlUpdate("INSERT into presto_query_operator_stats(\n" +
            "query_id,\n" +
            "stage_id,\n" +
            "stage_execution_id,\n" +
            "pipeline_id,\n" +
            "operator_id,\n" +
            "plan_node_id,\n" +
            "operator_type,\n" +
            "total_drivers,\n" +
            "add_input_calls,\n" +
            "add_input_wall_ms,\n" +
            "add_input_cpu_ms,\n" +
            "add_input_allocation_bytes,\n" +
            "raw_input_data_size_bytes,\n" +
            "raw_input_positions,\n" +
            "input_data_size_bytes,\n" +
            "input_positions,\n" +
            "sum_squared_input_positions,\n" +
            "get_output_calls,\n" +
            "get_output_wall_ms,\n" +
            "get_output_cpu_ms,\n" +
            "get_output_allocation_bytes,\n" +
            "output_data_size_bytes,\n" +
            "output_positions,\n" +
            "physical_written_data_size_bytes,\n" +
            "blocked_wall_ms,\n" +
            "finish_calls,\n" +
            "finish_wall_ms,\n" +
            "finish_cpu_ms,\n" +
            "finish_allocation_bytes,\n" +
            "user_memory_reservation_bytes,\n" +
            "revocable_memory_reservation_bytes,\n" +
            "system_memory_reservation_bytes,\n" +
            "peak_user_memory_reservation_bytes,\n" +
            "peak_system_memory_reservation_bytes,\n" +
            "peak_total_memory_reservation_bytes,\n" +
            "spilled_data_size_bytes,\n" +
            "info,\n" +
            "dt) \n" +
            "VALUES( \n" +
            ":query_id," +
            ":stage_id," +
            ":stage_execution_id," +
            ":pipeline_id," +
            ":operator_id," +
            ":plan_node_id," +
            ":operator_type," +
            ":total_drivers," +
            ":add_input_calls," +
            ":add_input_wall_ms," +
            ":add_input_cpu_ms," +
            ":add_input_allocation_bytes," +
            ":raw_input_data_size_bytes," +
            ":raw_input_positions," +
            ":input_data_size_bytes," +
            ":input_positions," +
            ":sum_squared_input_positions," +
            ":get_output_calls," +
            ":get_output_wall_ms," +
            ":get_output_cpu_ms," +
            ":get_output_allocation_bytes," +
            ":output_data_size_bytes," +
            ":output_positions," +
            ":physical_written_data_size_bytes," +
            ":blocked_wall_ms," +
            ":finish_calls," +
            ":finish_wall_ms," +
            ":finish_cpu_ms," +
            ":finish_allocation_bytes," +
            ":user_memory_reservation_bytes," +
            ":revocable_memory_reservation_bytes," +
            ":system_memory_reservation_bytes," +
            ":peak_user_memory_reservation_bytes," +
            ":peak_system_memory_reservation_bytes," +
            ":peak_total_memory_reservation_bytes," +
            ":spilled_data_size_bytes," +
            ":info," +
            ":dt)")
    void insertQueryOperatorStats(
            @Bind("query_id") String queryId,
            @Bind("stage_id") int stageId,
            @Bind("stage_execution_id") int stageExecutionId,
            @Bind("pipeline_id") int pipelineId,
            @Bind("operator_id") int operatorId,
            @Bind("plan_node_id") String planNodeId,
            @Bind("operator_type") String operatorType,
            @Bind("total_drivers") long totalDrivers,
            @Bind("add_input_calls") long addInputCalls,
            @Bind("add_input_wall_ms") long addInputWallMs,
            @Bind("add_input_cpu_ms") long addInputCpuMs,
            @Bind("add_input_allocation_bytes") long addInputAllocationBytes,
            @Bind("raw_input_data_size_bytes") long rawInputDataSizeBytes,
            @Bind("raw_input_positions") long rawInputPositions,
            @Bind("input_data_size_bytes") long inputDataSizeBytes,
            @Bind("input_positions") long inputPositions,
            @Bind("sum_squared_input_positions") double sumSquaredInputPositions,
            @Bind("get_output_calls") long getOutputCalls,
            @Bind("get_output_wall_ms") long getOutputWallMs,
            @Bind("get_output_cpu_ms") long getOutputCpuMs,
            @Bind("get_output_allocation_bytes") long getOutputAllocationBytes,
            @Bind("output_data_size_bytes") long outputDataSizeBytes,
            @Bind("output_positions") long outputPositions,
            @Bind("physical_written_data_size_bytes") long physicalWrittenDataSizeBytes,
            @Bind("blocked_wall_ms") long blockedWallMs,
            @Bind("finish_calls") long finishCalls,
            @Bind("finish_wall_ms") long finishWallMs,
            @Bind("finish_cpu_ms") long finishCpuMs,
            @Bind("finish_allocation_bytes") long finishAllocationBytes,
            @Bind("user_memory_reservation_bytes") long userMemoryReservationBytes,
            @Bind("revocable_memory_reservation_bytes") long revocableMemoryReservationBytes,
            @Bind("system_memory_reservation_bytes") long systemMemoryReservationBytes,
            @Bind("peak_user_memory_reservation_bytes") long peakUserMemoryReservationBytes,
            @Bind("peak_system_memory_reservation_bytes") long peakSystemMemoryReservationBytes,
            @Bind("peak_total_memory_reservation_bytes") long peakTotalMemoryReservationBytes,
            @Bind("spilled_data_size_bytes") long spilledDataSizeBytes,
            @Bind("info") String info,
            @Bind("dt") String dt);

    @SqlUpdate("INSERT into presto_query_statistics(\n" +
            "query_id,\n" +
            "query,\n" +
            "query_type,\n" +
            "schema_name,\n" +
            "catalog_name,\n" +
            "environment,\n" +
            "user,\n" +
            "remote_client_address,\n" +
            "source,\n" +
            "user_agent,\n" +
            "uri,\n" +
            "session_properties_json,\n" +
            "server_version,\n" +
            "client_info,\n" +
            "resource_group_name,\n" +
            "principal,\n" +
            "transaction_id,\n" +
            "client_tags,\n" +
            "resource_estimates,\n" +
            "create_time,\n" +
            "end_time,\n" +
            "execution_start_time,\n" +
            "query_state,\n" +
            "failure_message,\n" +
            "failure_type,\n" +
            "failures_json,\n" +
            "failure_task,\n" +
            "failure_host,\n" +
            "error_code,\n" +
            "error_code_name,\n" +
            "error_category,\n" +
            "warnings_json,\n" +
            "splits,\n" +
            "analysis_time_ms,\n" +
            "queued_time_ms,\n" +
            "query_wall_time_ms,\n" +
            "query_execution_time_ms,\n" +
            "bytes_per_cpu_sec,\n" +
            "bytes_per_sec,\n" +
            "rows_per_cpu_sec,\n" +
            "total_bytes,\n" +
            "total_rows,\n" +
            "output_rows,\n" +
            "output_bytes,\n" +
            "written_rows,\n" +
            "written_bytes,\n" +
            "cumulative_memory,\n" +
            "peak_user_memory_bytes,\n" +
            "peak_total_memory_bytes,\n" +
            "peak_task_total_memory,\n" +
            "peak_task_user_memory,\n" +
            "written_intermediate_bytes,\n" +
            "peak_node_total_memory,\n" +
            "total_split_cpu_time_ms,\n" +
            "stage_count,\n" +
            "cumulative_total_memory,\n" +
            "dt) \n" +
            "VALUES( \n" +
            ":query_id," +
            ":query," +
            ":query_type," +
            ":schema_name," +
            ":catalog_name," +
            ":environment," +
            ":user," +
            ":remote_client_address," +
            ":source," +
            ":user_agent," +
            ":uri," +
            ":session_properties_json," +
            ":server_version," +
            ":client_info," +
            ":resource_group_name," +
            ":principal," +
            ":transaction_id," +
            ":client_tags," +
            ":resource_estimates," +
            ":create_time," +
            ":end_time," +
            ":execution_start_time," +
            ":query_state," +
            ":failure_message," +
            ":failure_type," +
            ":failures_json," +
            ":failure_task," +
            ":failure_host," +
            ":error_code," +
            ":error_code_name," +
            ":error_category," +
            ":warnings_json," +
            ":splits," +
            ":analysis_time_ms," +
            ":queued_time_ms," +
            ":query_wall_time_ms," +
            ":query_execution_time_ms," +
            ":bytes_per_cpu_sec," +
            ":bytes_per_sec," +
            ":rows_per_cpu_sec," +
            ":total_bytes," +
            ":total_rows," +
            ":output_rows," +
            ":output_bytes," +
            ":written_rows," +
            ":written_bytes," +
            ":cumulative_memory," +
            ":peak_user_memory_bytes," +
            ":peak_total_memory_bytes," +
            ":peak_task_total_memory," +
            ":peak_task_user_memory," +
            ":written_intermediate_bytes," +
            ":peak_node_total_memory," +
            ":total_split_cpu_time_ms," +
            ":stage_count," +
            ":cumulative_total_memory," +
            ":dt)")
    void insertQueryStatistics(
            @Bind("query_id") String queryId,
            @Bind("query") String query,
            @Bind("query_type") String queryType,
            @Bind("schema_name") String schemaName,
            @Bind("catalog_name") String catalogName,
            @Bind("environment") String environment,
            @Bind("user") String user,
            @Bind("remote_client_address") String remoteClientAddress,
            @Bind("source") String source,
            @Bind("user_agent") String userAgent,
            @Bind("uri") String uri,
            @Bind("session_properties_json") String sessionPropertiesJson,
            @Bind("server_version") String serverVersion,
            @Bind("client_info") String clientInfo,
            @Bind("resource_group_name") String resourceGroupName,
            @Bind("principal") String principal,
            @Bind("transaction_id") String transactionId,
            @Bind("client_tags") String clientTags,
            @Bind("resource_estimates") String resourceEstimates,
            @Bind("create_time") String createTime,
            @Bind("end_time") String endTime,
            @Bind("execution_start_time") String executionStartTime,
            @Bind("query_state") String queryState,
            @Bind("failure_message") String failureMessage,
            @Bind("failure_type") String failureType,
            @Bind("failures_json") String failuresJson,
            @Bind("failure_task") String failureTask,
            @Bind("failure_host") String failureHost,
            @Bind("error_code") int errorCode,
            @Bind("error_code_name") String errorCodeName,
            @Bind("error_category") String errorCatagory,
            @Bind("warnings_json") String warningsJson,
            @Bind("splits") int splits,
            @Bind("analysis_time_ms") long analysisTimeMs,
            @Bind("queued_time_ms") long queuedTimeMs,
            @Bind("query_wall_time_ms") long queryWallTimeMs,
            @Bind("query_execution_time_ms") long queryExecutionTimeMs,
            @Bind("bytes_per_cpu_sec") long bytesPerCpuSec,
            @Bind("bytes_per_sec") long bytesPerSec,
            @Bind("rows_per_cpu_sec") long rowsPerCpuSec,
            @Bind("total_bytes") long totalBytes,
            @Bind("total_rows") long totalRows,
            @Bind("output_rows") long outputRows,
            @Bind("output_bytes") long outputBytes,
            @Bind("written_rows") long writtenRows,
            @Bind("written_bytes") long writtenBytes,
            @Bind("cumulative_memory") double cumulativeMemory,
            @Bind("peak_user_memory_bytes") long peakUserMemoryBytes,
            @Bind("peak_total_memory_bytes") long peakTotalMemoryBytes,
            @Bind("peak_task_total_memory") long peakTaskTotalMemory,
            @Bind("peak_task_user_memory") long peakTaskUserMemory,
            @Bind("written_intermediate_bytes") long writtenIntermediateBytes,
            @Bind("peak_node_total_memory") long peakNodeTotalMemory,
            @Bind("total_split_cpu_time_ms") long totalSplitCpuTimeMs,
            @Bind("stage_count") long stageCount,
            @Bind("cumulative_total_memory") double cumulativeTotalMemory,
            @Bind("dt") String dt);
}
