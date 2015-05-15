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
package com.facebook.presto.kinesis;

import java.util.ArrayList;
import java.util.List;

import javax.inject.Named;

import com.amazonaws.services.kinesis.model.DescribeStreamRequest;
import com.amazonaws.services.kinesis.model.DescribeStreamResult;
import com.amazonaws.services.kinesis.model.ResourceNotFoundException;
import com.amazonaws.services.kinesis.model.Shard;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorPartition;
import com.facebook.presto.spi.ConnectorPartitionResult;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.ConnectorSplitManager;
import com.facebook.presto.spi.ConnectorSplitSource;
import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.FixedSplitSource;
import com.facebook.presto.spi.TupleDomain;
import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;

import static com.google.common.base.Preconditions.checkState;

/**
 *
 * Split data chunk from kinesis Stream to multiple small chunks for parallelization and distribution to multiple Presto workers.
 * By default, each shard of Kinesis Stream forms one Kinesis Split
 *
 */
public class KinesisSplitManager
        implements ConnectorSplitManager
{
    private final String connectorId;
    private final KinesisHandleResolver handleResolver;
    private final KinesisClientManager clientManager;

    @Inject
    public  KinesisSplitManager(@Named("connectorId") String connectorId,
            KinesisHandleResolver handleResolver,
            KinesisClientManager clientManager)
    {
        this.connectorId = connectorId;
        this.handleResolver = handleResolver;
        this.clientManager = clientManager;
    }

    @Override
    public ConnectorPartitionResult getPartitions(ConnectorTableHandle tableHandle, TupleDomain<ColumnHandle> tupleDomain)
    {
        KinesisTableHandle kinesisTableHandle = handleResolver.convertTableHandle(tableHandle);

        DescribeStreamRequest describeStreamRequest = clientManager.getDescribeStreamRequest();
        describeStreamRequest.setStreamName(kinesisTableHandle.getStreamName());

        String exclusiveStartShardId = null;
        describeStreamRequest.setExclusiveStartShardId(exclusiveStartShardId);
        DescribeStreamResult describeStreamResult = clientManager.getClient().describeStream(describeStreamRequest);

        String streamStatus = describeStreamResult.getStreamDescription().getStreamStatus();
        while ((streamStatus.equals("ACTIVE") == false) && (streamStatus.equals("UPDATING") == false)) {
            throw new ResourceNotFoundException("Stream not Active");
        }

        List<Shard> shards = new ArrayList<>();
        ImmutableList.Builder<ConnectorPartition> builder = ImmutableList.builder();
        do {
            shards.addAll(describeStreamResult.getStreamDescription().getShards());

            for (Shard shard : shards) {
                builder.add(new KinesisShard(kinesisTableHandle.getStreamName(), shard));
            }

            if (describeStreamResult.getStreamDescription().getHasMoreShards() && (shards.size() > 0)) {
                exclusiveStartShardId = shards.get(shards.size() - 1).getShardId();
            }
            else {
                exclusiveStartShardId = null;
            }

        } while (exclusiveStartShardId != null);

        return new ConnectorPartitionResult(builder.build(), tupleDomain);
    }

    @Override
    public ConnectorSplitSource getPartitionSplits(ConnectorTableHandle tableHandle, List<ConnectorPartition> partitions)
    {
        KinesisTableHandle kinesisTableHandle = handleResolver.convertTableHandle(tableHandle);

        ImmutableList.Builder<ConnectorSplit> builder = ImmutableList.builder();

        for (ConnectorPartition cp : partitions) {
            checkState(cp instanceof KinesisShard, "Found an unkown partition type: %s", cp.getClass().getSimpleName());
            KinesisShard kinesisShard = (KinesisShard) cp;

            KinesisSplit split = new KinesisSplit(connectorId,
                        kinesisShard.getStreamName(),
                        kinesisTableHandle.getMessageDataFormat(),
                        kinesisShard.getPartitionId(),
                        kinesisShard.getShard().getSequenceNumberRange().getStartingSequenceNumber(),
                        kinesisShard.getShard().getSequenceNumberRange().getEndingSequenceNumber());
            builder.add(split);
        }

        return new FixedSplitSource(connectorId, builder.build());
    }
}
