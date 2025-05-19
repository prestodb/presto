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
package com.facebook.presto.server;

import com.facebook.presto.connector.ConnectorTypeSerdeManager;
import com.facebook.presto.execution.TaskInfo;
import com.facebook.presto.metadata.HandleResolver;
import com.facebook.presto.metadata.MetadataUpdates;
import com.facebook.presto.operator.DriverStats;
import com.facebook.presto.operator.OperatorStats;
import com.facebook.presto.operator.PipelineStats;
import com.facebook.presto.operator.TaskStats;
import com.facebook.presto.server.thrift.Any;
import com.facebook.presto.spi.ConnectorMetadataUpdateHandle;
import com.facebook.presto.spi.ConnectorTypeSerde;
import com.google.common.collect.ImmutableList;

import javax.ws.rs.core.HttpHeaders;

import java.util.List;

import static com.facebook.presto.operator.OperatorInfoUnion.convertToOperatorInfo;
import static com.facebook.presto.operator.OperatorInfoUnion.convertToOperatorInfoUnion;
import static java.util.stream.Collectors.toList;

public class TaskResourceUtils
{
    private TaskResourceUtils()
    {
    }

    public static boolean isThriftAcceptable(HttpHeaders httpHeaders)
    {
        return httpHeaders.getAcceptableMediaTypes().stream()
                .anyMatch(mediaType -> mediaType.toString().contains("application/x-thrift"));
    }

    public static TaskInfo convertToThriftTaskInfo(
            TaskInfo taskInfo,
            ConnectorTypeSerdeManager connectorTypeSerdeManager,
            HandleResolver handleResolver)
    {
        return new TaskInfo(
                taskInfo.getTaskId(),
                taskInfo.getTaskStatus(),
                taskInfo.getLastHeartbeatInMillis(),
                taskInfo.getOutputBuffers(),
                taskInfo.getNoMoreSplits(),
                convertToThriftTaskStats(taskInfo.getStats()),
                taskInfo.isNeedsPlan(),
                convertToThriftMetadataUpdates(taskInfo.getMetadataUpdates(), connectorTypeSerdeManager, handleResolver),
                taskInfo.getNodeId());
    }

    private static TaskStats convertToThriftTaskStats(TaskStats taskStats)
    {
        if (taskStats.getPipelines().isEmpty()) {
            return taskStats;
        }

        return new TaskStats(
                taskStats.getCreateTimeInMillis(),
                taskStats.getFirstStartTimeInMillis(),
                taskStats.getLastStartTimeInMillis(),
                taskStats.getLastEndTimeInMillis(),
                taskStats.getEndTimeInMillis(),
                taskStats.getElapsedTimeInNanos(),
                taskStats.getQueuedTimeInNanos(),
                taskStats.getTotalDrivers(),
                taskStats.getQueuedDrivers(),
                taskStats.getQueuedPartitionedDrivers(),
                taskStats.getQueuedPartitionedSplitsWeight(),
                taskStats.getRunningDrivers(),
                taskStats.getRunningPartitionedDrivers(),
                taskStats.getRunningPartitionedSplitsWeight(),
                taskStats.getBlockedDrivers(),
                taskStats.getCompletedDrivers(),
                taskStats.getCumulativeUserMemory(),
                taskStats.getCumulativeTotalMemory(),
                taskStats.getUserMemoryReservationInBytes(),
                taskStats.getRevocableMemoryReservationInBytes(),
                taskStats.getSystemMemoryReservationInBytes(),
                taskStats.getPeakUserMemoryInBytes(),
                taskStats.getPeakTotalMemoryInBytes(),
                taskStats.getPeakNodeTotalMemoryInBytes(),
                taskStats.getTotalScheduledTimeInNanos(),
                taskStats.getTotalCpuTimeInNanos(),
                taskStats.getTotalBlockedTimeInNanos(),
                taskStats.isFullyBlocked(),
                taskStats.getBlockedReasons(),
                taskStats.getTotalAllocationInBytes(),
                taskStats.getRawInputDataSizeInBytes(),
                taskStats.getRawInputPositions(),
                taskStats.getProcessedInputDataSizeInBytes(),
                taskStats.getProcessedInputPositions(),
                taskStats.getOutputDataSizeInBytes(),
                taskStats.getOutputPositions(),
                taskStats.getPhysicalWrittenDataSizeInBytes(),
                taskStats.getFullGcCount(),
                taskStats.getFullGcTimeInMillis(),
                convertToThriftPipeLineStatsList(taskStats.getPipelines()),
                taskStats.getRuntimeStats());
    }

    private static List<PipelineStats> convertToThriftPipeLineStatsList(List<PipelineStats> pipelines)
    {
        return pipelines.stream()
                .map(TaskResourceUtils::convertToThriftPipelineStats)
                .collect(toList());
    }

    private static PipelineStats convertToThriftPipelineStats(PipelineStats pipelineStats)
    {
        if (pipelineStats.getDrivers().isEmpty() && pipelineStats.getOperatorSummaries().isEmpty()) {
            return pipelineStats;
        }

        return new PipelineStats(
                pipelineStats.getPipelineId(),
                pipelineStats.getFirstStartTimeInMillis(),
                pipelineStats.getLastStartTimeInMillis(),
                pipelineStats.getLastEndTimeInMillis(),
                pipelineStats.isInputPipeline(),
                pipelineStats.isOutputPipeline(),
                pipelineStats.getTotalDrivers(),
                pipelineStats.getQueuedDrivers(),
                pipelineStats.getQueuedPartitionedDrivers(),
                pipelineStats.getQueuedPartitionedSplitsWeight(),
                pipelineStats.getRunningDrivers(),
                pipelineStats.getRunningPartitionedDrivers(),
                pipelineStats.getRunningPartitionedSplitsWeight(),
                pipelineStats.getBlockedDrivers(),
                pipelineStats.getCompletedDrivers(),
                pipelineStats.getUserMemoryReservationInBytes(),
                pipelineStats.getRevocableMemoryReservationInBytes(),
                pipelineStats.getSystemMemoryReservationInBytes(),
                pipelineStats.getQueuedTime(),
                pipelineStats.getElapsedTime(),
                pipelineStats.getTotalScheduledTimeInNanos(),
                pipelineStats.getTotalCpuTimeInNanos(),
                pipelineStats.getTotalBlockedTimeInNanos(),
                pipelineStats.isFullyBlocked(),
                pipelineStats.getBlockedReasons(),
                pipelineStats.getTotalAllocationInBytes(),
                pipelineStats.getRawInputDataSizeInBytes(),
                pipelineStats.getRawInputPositions(),
                pipelineStats.getProcessedInputDataSizeInBytes(),
                pipelineStats.getProcessedInputPositions(),
                pipelineStats.getOutputDataSizeInBytes(),
                pipelineStats.getOutputPositions(),
                pipelineStats.getPhysicalWrittenDataSizeInBytes(),
                convertToThriftOperatorStatsList(pipelineStats.getOperatorSummaries()),
                convertToThriftDriverStatsList(pipelineStats.getDrivers()));
    }

    private static List<DriverStats> convertToThriftDriverStatsList(List<DriverStats> drivers)
    {
        return drivers.stream()
                .map(d -> d.getOperatorStats().isEmpty() ? d : convertToThriftDriverStats(d))
                .collect(toList());
    }

    private static DriverStats convertToThriftDriverStats(DriverStats driverStats)
    {
        return new DriverStats(
                driverStats.getLifespan(),
                driverStats.getCreateTimeInMillis(),
                driverStats.getStartTimeInMillis(),
                driverStats.getEndTimeInMillis(),
                driverStats.getQueuedTime(),
                driverStats.getElapsedTime(),
                driverStats.getUserMemoryReservationInBytes(),
                driverStats.getRevocableMemoryReservationInBytes(),
                driverStats.getSystemMemoryReservationInBytes(),
                driverStats.getTotalScheduledTime(),
                driverStats.getTotalCpuTime(),
                driverStats.getTotalBlockedTime(),
                driverStats.isFullyBlocked(),
                driverStats.getBlockedReasons(),
                driverStats.getTotalAllocationInBytes(),
                driverStats.getRawInputDataSizeInBytes(),
                driverStats.getRawInputPositions(),
                driverStats.getRawInputReadTime(),
                driverStats.getProcessedInputDataSizeInBytes(),
                driverStats.getProcessedInputPositions(),
                driverStats.getOutputDataSizeInBytes(),
                driverStats.getOutputPositions(),
                driverStats.getPhysicalWrittenDataSizeInBytes(),
                convertToThriftOperatorStatsList(driverStats.getOperatorStats()));
    }

    private static List<OperatorStats> convertToThriftOperatorStatsList(List<OperatorStats> operatorSummaries)
    {
        return operatorSummaries.stream()
                .map(operatorStats -> operatorStats.getInfo() != null ? convertToThriftOperatorStats(operatorStats) : operatorStats)
                .collect(toList());
    }

    private static OperatorStats convertToThriftOperatorStats(OperatorStats operatorStats)
    {
        return new OperatorStats(
                operatorStats.getStageId(),
                operatorStats.getStageExecutionId(),
                operatorStats.getPipelineId(),
                operatorStats.getOperatorId(),
                operatorStats.getPlanNodeId(),
                operatorStats.getOperatorType(),
                operatorStats.getTotalDrivers(),
                operatorStats.getIsBlockedCalls(),
                operatorStats.getIsBlockedWall(),
                operatorStats.getIsBlockedCpu(),
                operatorStats.getIsBlockedAllocationInBytes(),
                operatorStats.getAddInputCalls(),
                operatorStats.getAddInputWall(),
                operatorStats.getAddInputCpu(),
                operatorStats.getAddInputAllocationInBytes(),
                operatorStats.getRawInputDataSizeInBytes(),
                operatorStats.getRawInputPositions(),
                operatorStats.getInputDataSizeInBytes(),
                operatorStats.getInputPositions(),
                operatorStats.getSumSquaredInputPositions(),
                operatorStats.getGetOutputCalls(),
                operatorStats.getGetOutputWall(),
                operatorStats.getGetOutputCpu(),
                operatorStats.getGetOutputAllocationInBytes(),
                operatorStats.getOutputDataSizeInBytes(),
                operatorStats.getOutputPositions(),
                operatorStats.getPhysicalWrittenDataSizeInBytes(),
                operatorStats.getAdditionalCpu(),
                operatorStats.getBlockedWall(),
                operatorStats.getFinishCalls(),
                operatorStats.getFinishWall(),
                operatorStats.getFinishCpu(),
                operatorStats.getFinishAllocationInBytes(),
                operatorStats.getUserMemoryReservationInBytes(),
                operatorStats.getRevocableMemoryReservationInBytes(),
                operatorStats.getSystemMemoryReservationInBytes(),
                operatorStats.getPeakUserMemoryReservationInBytes(),
                operatorStats.getPeakSystemMemoryReservationInBytes(),
                operatorStats.getPeakTotalMemoryReservationInBytes(),
                operatorStats.getSpilledDataSizeInBytes(),
                operatorStats.getBlockedReason(),
                operatorStats.getRuntimeStats(),
                operatorStats.getDynamicFilterStats(),
                convertToOperatorInfoUnion(operatorStats.getInfo()),
                operatorStats.getNullJoinBuildKeyCount(),
                operatorStats.getJoinBuildKeyCount(),
                operatorStats.getNullJoinProbeKeyCount(),
                operatorStats.getJoinProbeKeyCount());
    }

    private static MetadataUpdates convertToThriftMetadataUpdates(
            MetadataUpdates metadataUpdates,
            ConnectorTypeSerdeManager connectorTypeSerdeManager,
            HandleResolver handleResolver)
    {
        List<ConnectorMetadataUpdateHandle> metadataUpdateHandles = metadataUpdates.getMetadataUpdates();
        if (metadataUpdateHandles.isEmpty()) {
            return new MetadataUpdates(metadataUpdates.getConnectorId(), ImmutableList.of(), true);
        }
        ConnectorTypeSerde<ConnectorMetadataUpdateHandle> connectorTypeSerde =
                connectorTypeSerdeManager.getMetadataUpdateHandleSerde(metadataUpdates.getConnectorId());
        List<Any> anyMetadataHandles = convertToAny(metadataUpdateHandles, connectorTypeSerde, handleResolver);
        return new MetadataUpdates(metadataUpdates.getConnectorId(), anyMetadataHandles, true);
    }

    private static List<Any> convertToAny(
            List<ConnectorMetadataUpdateHandle> connectorMetadataUpdateHandles,
            ConnectorTypeSerde<ConnectorMetadataUpdateHandle> connectorTypeSerde,
            HandleResolver handleResolver)
    {
        return connectorMetadataUpdateHandles.stream()
                .map(e -> new Any(handleResolver.getId(e), connectorTypeSerde.serialize(e)))
                .collect(toList());
    }

    public static TaskInfo convertFromThriftTaskInfo(
            TaskInfo thriftTaskInfo,
            ConnectorTypeSerdeManager connectorTypeSerdeManager,
            HandleResolver handleResolver)
    {
        return new TaskInfo(
                thriftTaskInfo.getTaskId(),
                thriftTaskInfo.getTaskStatus(),
                thriftTaskInfo.getLastHeartbeatInMillis(),
                thriftTaskInfo.getOutputBuffers(),
                thriftTaskInfo.getNoMoreSplits(),
                convertFromThriftTaskStats(thriftTaskInfo.getStats()),
                thriftTaskInfo.isNeedsPlan(),
                convertFromThriftMetadataUpdates(thriftTaskInfo.getMetadataUpdates(), connectorTypeSerdeManager, handleResolver),
                thriftTaskInfo.getNodeId());
    }

    private static TaskStats convertFromThriftTaskStats(TaskStats thriftTaskStats)
    {
        if (thriftTaskStats.getPipelines().isEmpty()) {
            return thriftTaskStats;
        }

        return new TaskStats(
                thriftTaskStats.getCreateTimeInMillis(),
                thriftTaskStats.getFirstStartTimeInMillis(),
                thriftTaskStats.getLastStartTimeInMillis(),
                thriftTaskStats.getLastEndTimeInMillis(),
                thriftTaskStats.getEndTimeInMillis(),
                thriftTaskStats.getElapsedTimeInNanos(),
                thriftTaskStats.getQueuedTimeInNanos(),
                thriftTaskStats.getTotalDrivers(),
                thriftTaskStats.getQueuedDrivers(),
                thriftTaskStats.getQueuedPartitionedDrivers(),
                thriftTaskStats.getQueuedPartitionedSplitsWeight(),
                thriftTaskStats.getRunningDrivers(),
                thriftTaskStats.getRunningPartitionedDrivers(),
                thriftTaskStats.getRunningPartitionedSplitsWeight(),
                thriftTaskStats.getBlockedDrivers(),
                thriftTaskStats.getCompletedDrivers(),
                thriftTaskStats.getCumulativeUserMemory(),
                thriftTaskStats.getCumulativeTotalMemory(),
                thriftTaskStats.getUserMemoryReservationInBytes(),
                thriftTaskStats.getRevocableMemoryReservationInBytes(),
                thriftTaskStats.getSystemMemoryReservationInBytes(),
                thriftTaskStats.getPeakUserMemoryInBytes(),
                thriftTaskStats.getPeakTotalMemoryInBytes(),
                thriftTaskStats.getPeakNodeTotalMemoryInBytes(),
                thriftTaskStats.getTotalScheduledTimeInNanos(),
                thriftTaskStats.getTotalCpuTimeInNanos(),
                thriftTaskStats.getTotalBlockedTimeInNanos(),
                thriftTaskStats.isFullyBlocked(),
                thriftTaskStats.getBlockedReasons(),
                thriftTaskStats.getTotalAllocationInBytes(),
                thriftTaskStats.getRawInputDataSizeInBytes(),
                thriftTaskStats.getRawInputPositions(),
                thriftTaskStats.getProcessedInputDataSizeInBytes(),
                thriftTaskStats.getProcessedInputPositions(),
                thriftTaskStats.getOutputDataSizeInBytes(),
                thriftTaskStats.getOutputPositions(),
                thriftTaskStats.getPhysicalWrittenDataSizeInBytes(),
                thriftTaskStats.getFullGcCount(),
                thriftTaskStats.getFullGcTimeInMillis(),
                convertFromThriftPipeLineStatsList(thriftTaskStats.getPipelines()),
                thriftTaskStats.getRuntimeStats());
    }

    private static List<PipelineStats> convertFromThriftPipeLineStatsList(List<PipelineStats> pipelines)
    {
        return pipelines.stream()
                .map(TaskResourceUtils::convertFromThriftPipelineStats)
                .collect(toList());
    }

    private static PipelineStats convertFromThriftPipelineStats(PipelineStats thriftPipelineStats)
    {
        if (thriftPipelineStats.getDrivers().isEmpty() && thriftPipelineStats.getOperatorSummaries().isEmpty()) {
            return thriftPipelineStats;
        }

        return new PipelineStats(
                thriftPipelineStats.getPipelineId(),
                thriftPipelineStats.getFirstStartTimeInMillis(),
                thriftPipelineStats.getLastStartTimeInMillis(),
                thriftPipelineStats.getLastEndTimeInMillis(),
                thriftPipelineStats.isInputPipeline(),
                thriftPipelineStats.isOutputPipeline(),
                thriftPipelineStats.getTotalDrivers(),
                thriftPipelineStats.getQueuedDrivers(),
                thriftPipelineStats.getQueuedPartitionedDrivers(),
                thriftPipelineStats.getQueuedPartitionedSplitsWeight(),
                thriftPipelineStats.getRunningDrivers(),
                thriftPipelineStats.getRunningPartitionedDrivers(),
                thriftPipelineStats.getRunningPartitionedSplitsWeight(),
                thriftPipelineStats.getBlockedDrivers(),
                thriftPipelineStats.getCompletedDrivers(),
                thriftPipelineStats.getUserMemoryReservationInBytes(),
                thriftPipelineStats.getRevocableMemoryReservationInBytes(),
                thriftPipelineStats.getSystemMemoryReservationInBytes(),
                thriftPipelineStats.getQueuedTime(),
                thriftPipelineStats.getElapsedTime(),
                thriftPipelineStats.getTotalScheduledTimeInNanos(),
                thriftPipelineStats.getTotalCpuTimeInNanos(),
                thriftPipelineStats.getTotalBlockedTimeInNanos(),
                thriftPipelineStats.isFullyBlocked(),
                thriftPipelineStats.getBlockedReasons(),
                thriftPipelineStats.getTotalAllocationInBytes(),
                thriftPipelineStats.getRawInputDataSizeInBytes(),
                thriftPipelineStats.getRawInputPositions(),
                thriftPipelineStats.getProcessedInputDataSizeInBytes(),
                thriftPipelineStats.getProcessedInputPositions(),
                thriftPipelineStats.getOutputDataSizeInBytes(),
                thriftPipelineStats.getOutputPositions(),
                thriftPipelineStats.getPhysicalWrittenDataSizeInBytes(),
                convertFromThriftOperatorStatsList(thriftPipelineStats.getOperatorSummaries()),
                convertFromThriftDriverStatsList(thriftPipelineStats.getDrivers()));
    }

    private static List<DriverStats> convertFromThriftDriverStatsList(List<DriverStats> thriftDrivers)
    {
        return thriftDrivers.stream()
                .map(driverStats -> driverStats.getOperatorStats().isEmpty() ? driverStats : convertFromThriftDriverStats(driverStats))
                .collect(toList());
    }

    private static DriverStats convertFromThriftDriverStats(DriverStats thriftDriverStats)
    {
        return new DriverStats(
                thriftDriverStats.getLifespan(),
                thriftDriverStats.getCreateTimeInMillis(),
                thriftDriverStats.getStartTimeInMillis(),
                thriftDriverStats.getEndTimeInMillis(),
                thriftDriverStats.getQueuedTime(),
                thriftDriverStats.getElapsedTime(),
                thriftDriverStats.getUserMemoryReservationInBytes(),
                thriftDriverStats.getRevocableMemoryReservationInBytes(),
                thriftDriverStats.getSystemMemoryReservationInBytes(),
                thriftDriverStats.getTotalScheduledTime(),
                thriftDriverStats.getTotalCpuTime(),
                thriftDriverStats.getTotalBlockedTime(),
                thriftDriverStats.isFullyBlocked(),
                thriftDriverStats.getBlockedReasons(),
                thriftDriverStats.getTotalAllocationInBytes(),
                thriftDriverStats.getRawInputDataSizeInBytes(),
                thriftDriverStats.getRawInputPositions(),
                thriftDriverStats.getRawInputReadTime(),
                thriftDriverStats.getProcessedInputDataSizeInBytes(),
                thriftDriverStats.getProcessedInputPositions(),
                thriftDriverStats.getOutputDataSizeInBytes(),
                thriftDriverStats.getOutputPositions(),
                thriftDriverStats.getPhysicalWrittenDataSizeInBytes(),
                convertFromThriftOperatorStatsList(thriftDriverStats.getOperatorStats()));
    }

    private static List<OperatorStats> convertFromThriftOperatorStatsList(List<OperatorStats> thriftOperatorSummaries)
    {
        return thriftOperatorSummaries.stream()
                .map(operatorStats -> operatorStats.getInfoUnion() != null ? convertFromThriftOperatorStats(operatorStats) : operatorStats)
                .collect(toList());
    }

    private static OperatorStats convertFromThriftOperatorStats(OperatorStats thriftOperatorStats)
    {
        return new OperatorStats(
                thriftOperatorStats.getStageId(),
                thriftOperatorStats.getStageExecutionId(),
                thriftOperatorStats.getPipelineId(),
                thriftOperatorStats.getOperatorId(),
                thriftOperatorStats.getPlanNodeId(),
                thriftOperatorStats.getOperatorType(),
                thriftOperatorStats.getTotalDrivers(),
                thriftOperatorStats.getIsBlockedCalls(),
                thriftOperatorStats.getIsBlockedWall(),
                thriftOperatorStats.getIsBlockedCpu(),
                thriftOperatorStats.getIsBlockedAllocationInBytes(),
                thriftOperatorStats.getAddInputCalls(),
                thriftOperatorStats.getAddInputWall(),
                thriftOperatorStats.getAddInputCpu(),
                thriftOperatorStats.getAddInputAllocationInBytes(),
                thriftOperatorStats.getRawInputDataSizeInBytes(),
                thriftOperatorStats.getRawInputPositions(),
                thriftOperatorStats.getInputDataSizeInBytes(),
                thriftOperatorStats.getInputPositions(),
                thriftOperatorStats.getSumSquaredInputPositions(),
                thriftOperatorStats.getGetOutputCalls(),
                thriftOperatorStats.getGetOutputWall(),
                thriftOperatorStats.getGetOutputCpu(),
                thriftOperatorStats.getGetOutputAllocationInBytes(),
                thriftOperatorStats.getOutputDataSizeInBytes(),
                thriftOperatorStats.getOutputPositions(),
                thriftOperatorStats.getPhysicalWrittenDataSizeInBytes(),
                thriftOperatorStats.getAdditionalCpu(),
                thriftOperatorStats.getBlockedWall(),
                thriftOperatorStats.getFinishCalls(),
                thriftOperatorStats.getFinishWall(),
                thriftOperatorStats.getFinishCpu(),
                thriftOperatorStats.getFinishAllocationInBytes(),
                thriftOperatorStats.getUserMemoryReservationInBytes(),
                thriftOperatorStats.getRevocableMemoryReservationInBytes(),
                thriftOperatorStats.getSystemMemoryReservationInBytes(),
                thriftOperatorStats.getPeakUserMemoryReservationInBytes(),
                thriftOperatorStats.getPeakSystemMemoryReservationInBytes(),
                thriftOperatorStats.getPeakTotalMemoryReservationInBytes(),
                thriftOperatorStats.getSpilledDataSizeInBytes(),
                thriftOperatorStats.getBlockedReason(),
                convertToOperatorInfo(thriftOperatorStats.getInfoUnion()),
                thriftOperatorStats.getRuntimeStats(),
                thriftOperatorStats.getDynamicFilterStats(),
                thriftOperatorStats.getNullJoinBuildKeyCount(),
                thriftOperatorStats.getJoinBuildKeyCount(),
                thriftOperatorStats.getNullJoinProbeKeyCount(),
                thriftOperatorStats.getJoinProbeKeyCount());
    }

    private static MetadataUpdates convertFromThriftMetadataUpdates(
            MetadataUpdates metadataUpdates,
            ConnectorTypeSerdeManager connectorTypeSerdeManager,
            HandleResolver handleResolver)
    {
        List<Any> metadataUpdateHandles = metadataUpdates.getMetadataUpdatesAny();
        if (metadataUpdateHandles.isEmpty()) {
            return new MetadataUpdates(metadataUpdates.getConnectorId(), ImmutableList.of());
        }
        ConnectorTypeSerde<ConnectorMetadataUpdateHandle> connectorTypeSerde =
                connectorTypeSerdeManager.getMetadataUpdateHandleSerde(metadataUpdates.getConnectorId());
        List<ConnectorMetadataUpdateHandle> connectorMetadataUpdateHandles = convertToConnector(metadataUpdateHandles, connectorTypeSerde, handleResolver);
        return new MetadataUpdates(metadataUpdates.getConnectorId(), connectorMetadataUpdateHandles);
    }

    private static List<ConnectorMetadataUpdateHandle> convertToConnector(
            List<Any> metadataUpdateHandles,
            ConnectorTypeSerde<ConnectorMetadataUpdateHandle> connectorTypeSerde,
            HandleResolver handleResolver)
    {
        return metadataUpdateHandles.stream()
                .map(e -> connectorTypeSerde.deserialize(handleResolver.getMetadataUpdateHandleClass(e.getId()), e.getBytes()))
                .collect(toList());
    }
}
