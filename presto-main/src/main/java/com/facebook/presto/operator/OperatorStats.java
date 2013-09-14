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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;

import javax.annotation.Nullable;
import javax.annotation.concurrent.Immutable;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static io.airlift.units.DataSize.Unit.BYTE;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

@Immutable
public class OperatorStats
{
    private final int operatorId;
    private final String operatorType;
    private final Duration getOutputWall;
    private final Duration getOutputCpu;
    private final Duration getOutputUser;
    private final DataSize outputDataSize;
    private final long outputPositions;

    private final Duration addInputWall;
    private final Duration addInputCpu;
    private final Duration addInputUser;
    private final DataSize inputDataSize;
    private final long inputPositions;

    private final Duration blockedWall;

    private final Duration finishWall;
    private final Duration finishCpu;
    private final Duration finishUser;

    private final DataSize memoryReservation;

    private final Object info;

    @JsonCreator
    public OperatorStats(
            @JsonProperty("operatorId") int operatorId,
            @JsonProperty("operatorType") String operatorType,

            @JsonProperty("getOutputWall") Duration getOutputWall,
            @JsonProperty("getOutputCpu") Duration getOutputCpu,
            @JsonProperty("getOutputUser") Duration getOutputUser,
            @JsonProperty("outputDataSize") DataSize outputDataSize,
            @JsonProperty("outputPositions") long outputPositions,

            @JsonProperty("addInputWall") Duration addInputWall,
            @JsonProperty("addInputCpu") Duration addInputCpu,
            @JsonProperty("addInputUser") Duration addInputUser,
            @JsonProperty("inputDataSize") DataSize inputDataSize,
            @JsonProperty("inputPositions") long inputPositions,

            @JsonProperty("blockedWall") Duration blockedWall,

            @JsonProperty("finishWall") Duration finishWall,
            @JsonProperty("finishCpu") Duration finishCpu,
            @JsonProperty("finishUser") Duration finishUser,

            @JsonProperty("memoryReservation") DataSize memoryReservation,

            @JsonProperty("info") Object info)
    {
        checkArgument(operatorId >= 0, "operatorId is negative");
        this.operatorId = operatorId;
        this.operatorType = checkNotNull(operatorType, "operatorType is null");
        this.getOutputWall = checkNotNull(getOutputWall, "getOutputWall is null");
        this.getOutputCpu = checkNotNull(getOutputCpu, "getOutputCpu is null");
        this.getOutputUser = checkNotNull(getOutputUser, "getOutputUser is null");
        this.outputDataSize = checkNotNull(outputDataSize, "outputDataSize is null");
        checkArgument(outputPositions >= 0, "outputPositions is negative");
        this.outputPositions = outputPositions;

        this.addInputWall = checkNotNull(addInputWall, "addInputWall is null");
        this.addInputCpu = checkNotNull(addInputCpu, "addInputCpu is null");
        this.addInputUser = checkNotNull(addInputUser, "addInputUser is null");
        this.inputDataSize = checkNotNull(inputDataSize, "inputDataSize is null");
        checkArgument(inputPositions >= 0, "inputPositions is negative");
        this.inputPositions = inputPositions;

        this.blockedWall = checkNotNull(blockedWall, "blockedWall is null");

        this.finishWall = checkNotNull(finishWall, "finishWall is null");
        this.finishCpu = checkNotNull(finishCpu, "finishCpu is null");
        this.finishUser = checkNotNull(finishUser, "finishUser is null");

        this.memoryReservation = checkNotNull(memoryReservation, "memoryReservation is null");

        this.info = info;
    }

    @JsonProperty
    public int getOperatorId()
    {
        return operatorId;
    }

    @JsonProperty
    public String getOperatorType()
    {
        return operatorType;
    }

    @JsonProperty
    public Duration getGetOutputWall()
    {
        return getOutputWall;
    }

    @JsonProperty
    public Duration getGetOutputCpu()
    {
        return getOutputCpu;
    }

    @JsonProperty
    public Duration getGetOutputUser()
    {
        return getOutputUser;
    }

    @JsonProperty
    public DataSize getOutputDataSize()
    {
        return outputDataSize;
    }

    @JsonProperty
    public long getOutputPositions()
    {
        return outputPositions;
    }

    @JsonProperty
    public Duration getAddInputWall()
    {
        return addInputWall;
    }

    @JsonProperty
    public Duration getAddInputCpu()
    {
        return addInputCpu;
    }

    @JsonProperty
    public Duration getAddInputUser()
    {
        return addInputUser;
    }

    @JsonProperty
    public DataSize getInputDataSize()
    {
        return inputDataSize;
    }

    @JsonProperty
    public long getInputPositions()
    {
        return inputPositions;
    }

    @JsonProperty
    public Duration getBlockedWall()
    {
        return blockedWall;
    }

    @JsonProperty
    public Duration getFinishWall()
    {
        return finishWall;
    }

    @JsonProperty
    public Duration getFinishCpu()
    {
        return finishCpu;
    }

    @JsonProperty
    public Duration getFinishUser()
    {
        return finishUser;
    }

    @JsonProperty
    public DataSize getMemoryReservation()
    {
        return memoryReservation;
    }

    @Nullable
    @JsonProperty
    public Object getInfo()
    {
        return info;
    }

    public OperatorStats add(OperatorStats... operators)
    {
        return add(ImmutableList.copyOf(operators));
    }

    public OperatorStats add(Iterable<OperatorStats> operators)
    {
        long getOutputWall = this.getOutputWall.roundTo(NANOSECONDS);
        long getOutputCpu = this.getOutputCpu.roundTo(NANOSECONDS);
        long getOutputUser = this.getOutputUser.roundTo(NANOSECONDS);
        long outputDataSize = this.outputDataSize.toBytes();
        long outputPositions = this.outputPositions;

        long addInputWall = this.addInputWall.roundTo(NANOSECONDS);
        long addInputCpu = this.addInputCpu.roundTo(NANOSECONDS);
        long addInputUser = this.addInputUser.roundTo(NANOSECONDS);
        long inputDataSize = this.inputDataSize.toBytes();
        long inputPositions = this.inputPositions;

        long blockedWall = this.blockedWall.roundTo(NANOSECONDS);

        long finishWall = this.finishWall.roundTo(NANOSECONDS);
        long finishCpu = this.finishCpu.roundTo(NANOSECONDS);
        long finishUser = this.finishUser.roundTo(NANOSECONDS);

        long memoryReservation = this.memoryReservation.toBytes();

        for (OperatorStats operator : operators) {
            checkArgument(operator.getOperatorId() == operatorId, "Expected operatorId to be %s but was %s", operatorId, operator.getOperatorId());

            getOutputWall += operator.getGetOutputWall().roundTo(NANOSECONDS);
            getOutputCpu += operator.getGetOutputCpu().roundTo(NANOSECONDS);
            getOutputUser += operator.getGetOutputUser().roundTo(NANOSECONDS);
            outputDataSize += operator.getOutputDataSize().toBytes();
            outputPositions += operator.getOutputPositions();

            addInputWall += operator.getAddInputWall().roundTo(NANOSECONDS);
            addInputCpu += operator.getAddInputCpu().roundTo(NANOSECONDS);
            addInputUser += operator.getAddInputUser().roundTo(NANOSECONDS);
            inputDataSize += operator.getInputDataSize().toBytes();
            inputPositions += operator.getInputPositions();

            finishWall += operator.getFinishWall().roundTo(NANOSECONDS);
            finishCpu += operator.getFinishCpu().roundTo(NANOSECONDS);
            finishUser += operator.getFinishUser().roundTo(NANOSECONDS);

            blockedWall += operator.getBlockedWall().roundTo(NANOSECONDS);

            memoryReservation += operator.getMemoryReservation().toBytes();
        }

        return new OperatorStats(
                operatorId,
                operatorType,
                new Duration(getOutputWall, NANOSECONDS),
                new Duration(getOutputCpu, NANOSECONDS),
                new Duration(getOutputUser, NANOSECONDS),
                new DataSize(outputDataSize, BYTE),
                outputPositions,

                new Duration(addInputWall, NANOSECONDS),
                new Duration(addInputCpu, NANOSECONDS),
                new Duration(addInputUser, NANOSECONDS),
                new DataSize(inputDataSize, BYTE),
                inputPositions,

                new Duration(blockedWall, NANOSECONDS),

                new Duration(finishWall, NANOSECONDS),
                new Duration(finishCpu, NANOSECONDS),
                new Duration(finishUser, NANOSECONDS),

                new DataSize(memoryReservation, BYTE),

                // todo merge operator info?
                null);
    }
}
