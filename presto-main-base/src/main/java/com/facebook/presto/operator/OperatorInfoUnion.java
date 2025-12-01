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
package com.facebook.presto.operator;

import com.facebook.drift.annotations.ThriftConstructor;
import com.facebook.drift.annotations.ThriftField;
import com.facebook.drift.annotations.ThriftUnion;
import com.facebook.drift.annotations.ThriftUnionId;
import com.facebook.presto.operator.exchange.LocalExchangeBufferInfo;
import com.facebook.presto.operator.repartition.PartitionedOutputInfo;

@ThriftUnion
public class OperatorInfoUnion
{
    private ExchangeClientStatus exchangeClientStatus;

    private LocalExchangeBufferInfo localExchangeBufferInfo;

    private TableFinishInfo tableFinishInfo;

    private SplitOperatorInfo splitOperatorInfo;

    private HashCollisionsInfo hashCollisionsInfo;

    private PartitionedOutputInfo partitionedOutputInfo;

    private JoinOperatorInfo joinOperatorInfo;

    private WindowInfo windowInfo;

    private TableWriterOperator.TableWriterInfo tableWriterInfo;

    private TableWriterMergeInfo tableWriterMergeInfo;

    private short id;

    @ThriftConstructor
    public OperatorInfoUnion()
    {
        this.id = 0;
    }

    @ThriftConstructor
    public OperatorInfoUnion(ExchangeClientStatus exchangeClientStatus)
    {
        this.exchangeClientStatus = exchangeClientStatus;
        this.id = 1;
    }

    @ThriftField(1)
    public ExchangeClientStatus getExchangeClientStatus()
    {
        return exchangeClientStatus;
    }

    @ThriftConstructor
    public OperatorInfoUnion(LocalExchangeBufferInfo localExchangeBufferInfo)
    {
        this.localExchangeBufferInfo = localExchangeBufferInfo;
        this.id = 2;
    }

    @ThriftField(2)
    public LocalExchangeBufferInfo getLocalExchangeBufferInfo()
    {
        return localExchangeBufferInfo;
    }

    @ThriftConstructor
    public OperatorInfoUnion(TableFinishInfo tableFinishInfo)
    {
        this.tableFinishInfo = tableFinishInfo;
        this.id = 3;
    }

    @ThriftField(3)
    public TableFinishInfo getTableFinishInfo()
    {
        return tableFinishInfo;
    }

    @ThriftConstructor
    public OperatorInfoUnion(SplitOperatorInfo splitOperatorInfo)
    {
        this.splitOperatorInfo = splitOperatorInfo;
        this.id = 4;
    }

    @ThriftField(4)
    public SplitOperatorInfo getSplitOperatorInfo()
    {
        return splitOperatorInfo;
    }

    @ThriftConstructor
    public OperatorInfoUnion(HashCollisionsInfo hashCollisionsInfo)
    {
        this.hashCollisionsInfo = hashCollisionsInfo;
        this.id = 5;
    }

    @ThriftField(5)
    public HashCollisionsInfo getHashCollisionsInfo()
    {
        return hashCollisionsInfo;
    }

    @ThriftConstructor
    public OperatorInfoUnion(PartitionedOutputInfo partitionedOutputInfo)
    {
        this.partitionedOutputInfo = partitionedOutputInfo;
        this.id = 6;
    }

    @ThriftField(6)
    public PartitionedOutputInfo getPartitionedOutputInfo()
    {
        return partitionedOutputInfo;
    }

    @ThriftConstructor
    public OperatorInfoUnion(JoinOperatorInfo joinOperatorInfo)
    {
        this.joinOperatorInfo = joinOperatorInfo;
        this.id = 7;
    }

    @ThriftField(7)
    public JoinOperatorInfo getJoinOperatorInfo()
    {
        return joinOperatorInfo;
    }

    @ThriftConstructor
    public OperatorInfoUnion(WindowInfo windowInfo)
    {
        this.windowInfo = windowInfo;
        this.id = 8;
    }

    @ThriftField(8)
    public WindowInfo getWindowInfo()
    {
        return windowInfo;
    }

    @ThriftConstructor
    public OperatorInfoUnion(TableWriterOperator.TableWriterInfo tableWriterInfo)
    {
        this.tableWriterInfo = tableWriterInfo;
        this.id = 9;
    }

    @ThriftField(9)
    public TableWriterOperator.TableWriterInfo getTableWriterInfo()
    {
        return tableWriterInfo;
    }

    @ThriftConstructor
    public OperatorInfoUnion(TableWriterMergeInfo tableWriterMergeInfo)
    {
        this.tableWriterMergeInfo = tableWriterMergeInfo;
        this.id = 10;
    }

    @ThriftField(10)
    public TableWriterMergeInfo getTableWriterMergeInfo()
    {
        return tableWriterMergeInfo;
    }

    @ThriftUnionId
    public short getId()
    {
        return id;
    }

    public static OperatorInfoUnion convertToOperatorInfoUnion(OperatorInfo info)
    {
        if (info instanceof ExchangeClientStatus) {
            return new OperatorInfoUnion((ExchangeClientStatus) info);
        }
        else if (info instanceof LocalExchangeBufferInfo) {
            return new OperatorInfoUnion((LocalExchangeBufferInfo) info);
        }
        else if (info instanceof TableFinishInfo) {
            return new OperatorInfoUnion((TableFinishInfo) info);
        }
        else if (info instanceof SplitOperatorInfo) {
            return new OperatorInfoUnion((SplitOperatorInfo) info);
        }
        else if (info instanceof HashCollisionsInfo) {
            return new OperatorInfoUnion((HashCollisionsInfo) info);
        }
        else if (info instanceof PartitionedOutputInfo) {
            return new OperatorInfoUnion((PartitionedOutputInfo) info);
        }
        else if (info instanceof JoinOperatorInfo) {
            return new OperatorInfoUnion((JoinOperatorInfo) info);
        }
        else if (info instanceof WindowInfo) {
            return new OperatorInfoUnion((WindowInfo) info);
        }
        else if (info instanceof TableWriterOperator.TableWriterInfo) {
            return new OperatorInfoUnion((TableWriterOperator.TableWriterInfo) info);
        }
        else if (info instanceof TableWriterMergeInfo) {
            return new OperatorInfoUnion((TableWriterMergeInfo) info);
        }
        else {
            throw new IllegalArgumentException("OperatorInfo is of an unknown type: " + info.getClass().getName());
        }
    }

    public static OperatorInfo convertToOperatorInfo(OperatorInfoUnion infoUnion)
    {
        if (infoUnion.getExchangeClientStatus() != null) {
            return infoUnion.getExchangeClientStatus();
        }
        else if (infoUnion.getLocalExchangeBufferInfo() != null) {
            return infoUnion.getLocalExchangeBufferInfo();
        }
        else if (infoUnion.getTableFinishInfo() != null) {
            return infoUnion.getTableFinishInfo();
        }
        else if (infoUnion.getSplitOperatorInfo() != null) {
            return infoUnion.getSplitOperatorInfo();
        }
        else if (infoUnion.getHashCollisionsInfo() != null) {
            return infoUnion.getHashCollisionsInfo();
        }
        else if (infoUnion.getPartitionedOutputInfo() != null) {
            return infoUnion.getPartitionedOutputInfo();
        }
        else if (infoUnion.getJoinOperatorInfo() != null) {
            return infoUnion.getJoinOperatorInfo();
        }
        else if (infoUnion.getWindowInfo() != null) {
            return infoUnion.getWindowInfo();
        }
        else if (infoUnion.getTableWriterInfo() != null) {
            return infoUnion.getTableWriterInfo();
        }
        else if (infoUnion.getTableWriterMergeInfo() != null) {
            return infoUnion.getTableWriterMergeInfo();
        }
        else {
            return null;
        }
    }
}
