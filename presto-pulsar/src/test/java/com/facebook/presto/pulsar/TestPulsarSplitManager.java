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
package com.facebook.presto.pulsar;

import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorSplitSource;
import com.facebook.presto.spi.FixedSplitSource;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import io.airlift.log.Logger;
import org.apache.bookkeeper.mledger.impl.PositionImpl;
import org.apache.pulsar.common.naming.TopicName;
import org.mockito.ArgumentMatcher;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.Collection;
import java.util.List;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.argThat;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class TestPulsarSplitManager extends TestPulsarConnector {

    private static final Logger log = Logger.get(TestPulsarSplitManager.class);

    public class ResultCaptor<T> implements Answer {
        private T result = null;
        public T getResult() {
            return result;
        }

        @Override
        public T answer(InvocationOnMock invocationOnMock) throws Throwable {
            result = (T) invocationOnMock.callRealMethod();
            return result;
        }
    }

    @Test
    public void testTopic() throws Exception {

        for (TopicName topicName : topicNames) {
            log.info("!----- topic: %s -----!", topicName);
            setup();
            PulsarTableHandle pulsarTableHandle = new PulsarTableHandle(pulsarConnectorId.toString(),
                    topicName.getNamespace(),
                    topicName.getLocalName(),
                    topicName.getLocalName());
            PulsarTableLayoutHandle pulsarTableLayoutHandle = new PulsarTableLayoutHandle(pulsarTableHandle);

            final ResultCaptor<Collection<PulsarSplit>> resultCaptor = new ResultCaptor<>();
            doAnswer(resultCaptor).when(this.pulsarSplitManager).getSplitsNonPartitionedTopic(anyInt(), any(), any(), any());


            ConnectorSplitSource connectorSplitSource = this.pulsarSplitManager.getSplits(
                    mock(ConnectorTransactionHandle.class), mock(ConnectorSession.class),
                    pulsarTableLayoutHandle, null);

            verify(this.pulsarSplitManager, times(1))
                    .getSplitsNonPartitionedTopic(anyInt(), any(), any(), any());

            int totalSize = 0;
            for (PulsarSplit pulsarSplit : resultCaptor.getResult()) {
                Assert.assertEquals(pulsarSplit.getConnectorId(), pulsarConnectorId.toString());
                Assert.assertEquals(pulsarSplit.getSchemaName(), topicName.getNamespace());
                Assert.assertEquals(pulsarSplit.getTableName(), topicName.getLocalName());
                Assert.assertEquals(pulsarSplit.getSchema(),
                        new String(topicsToSchemas.get(topicName.getSchemaName()).getSchema()));
                Assert.assertEquals(pulsarSplit.getSchemaType(), topicsToSchemas.get(topicName.getSchemaName()).getType());
                Assert.assertEquals(pulsarSplit.getStartPositionEntryId(), totalSize);
                Assert.assertEquals(pulsarSplit.getStartPositionLedgerId(), 0);
                Assert.assertEquals(pulsarSplit.getStartPosition(), PositionImpl.get(0, totalSize));
                Assert.assertEquals(pulsarSplit.getEndPositionLedgerId(), 0);
                Assert.assertEquals(pulsarSplit.getEndPositionEntryId(), totalSize + pulsarSplit.getSplitSize());
                Assert.assertEquals(pulsarSplit.getEndPosition(), PositionImpl.get(0, totalSize + pulsarSplit.getSplitSize()));

                totalSize += pulsarSplit.getSplitSize();
            }

            Assert.assertEquals(totalSize, topicsToEntries.get(topicName.getSchemaName()).intValue());
            cleanup();
        }

    }

    @Test
    public void testPartitionedTopic() throws Exception {
        for (TopicName topicName : partitionedTopicNames) {
            setup();
            log.info("!----- topic: %s -----!", topicName);
            PulsarTableHandle pulsarTableHandle = new PulsarTableHandle(pulsarConnectorId.toString(),
                    topicName.getNamespace(),
                    topicName.getLocalName(),
                    topicName.getLocalName());
            PulsarTableLayoutHandle pulsarTableLayoutHandle = new PulsarTableLayoutHandle(pulsarTableHandle);

            final ResultCaptor<Collection<PulsarSplit>> resultCaptor = new ResultCaptor<>();
            doAnswer(resultCaptor).when(this.pulsarSplitManager).getSplitsPartitionedTopic(anyInt(), any(), any(), any());

            this.pulsarSplitManager.getSplits(mock(ConnectorTransactionHandle.class), mock(ConnectorSession.class),
                    pulsarTableLayoutHandle, null);

            verify(this.pulsarSplitManager, times(1))
                    .getSplitsPartitionedTopic(anyInt(), any(), any(), any());

            int partitions = partitionedTopicsToPartitions.get(topicName.toString());

            for (int i = 0; i < partitions; i++) {
                List<PulsarSplit> splits = getSplitsForPartition(topicName.getPartition(i), resultCaptor.getResult());
                int totalSize = 0;
                for (PulsarSplit pulsarSplit : splits) {
                    Assert.assertEquals(pulsarSplit.getConnectorId(), pulsarConnectorId.toString());
                    Assert.assertEquals(pulsarSplit.getSchemaName(), topicName.getNamespace());
                    Assert.assertEquals(pulsarSplit.getTableName(), topicName.getPartition(i).getLocalName());
                    Assert.assertEquals(pulsarSplit.getSchema(),
                            new String(topicsToSchemas.get(topicName.getSchemaName()).getSchema()));
                    Assert.assertEquals(pulsarSplit.getSchemaType(), topicsToSchemas.get(topicName.getSchemaName()).getType());
                    Assert.assertEquals(pulsarSplit.getStartPositionEntryId(), totalSize);
                    Assert.assertEquals(pulsarSplit.getStartPositionLedgerId(), 0);
                    Assert.assertEquals(pulsarSplit.getStartPosition(), PositionImpl.get(0, totalSize));
                    Assert.assertEquals(pulsarSplit.getEndPositionLedgerId(), 0);
                    Assert.assertEquals(pulsarSplit.getEndPositionEntryId(), totalSize + pulsarSplit.getSplitSize());
                    Assert.assertEquals(pulsarSplit.getEndPosition(), PositionImpl.get(0, totalSize + pulsarSplit.getSplitSize()));

                    totalSize += pulsarSplit.getSplitSize();
                }

                Assert.assertEquals(totalSize, topicsToEntries.get(topicName.getSchemaName()).intValue());
            }

            cleanup();
        }
    }

    private List<PulsarSplit> getSplitsForPartition(TopicName target, Collection<PulsarSplit> splits) {
        return splits.stream().filter(new Predicate<PulsarSplit>() {
            @Override
            public boolean test(PulsarSplit pulsarSplit) {
                 TopicName topicName = TopicName.get(pulsarSplit.getSchemaName() + "/" + pulsarSplit.getTableName());

                 return target.equals(topicName);
            }
        }).collect(Collectors.toList());
    }
    
}
