/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.execution;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Objects;
import io.airlift.stats.DistributionStat;
import io.airlift.stats.DistributionStat.DistributionStatSnapshot;
import io.airlift.units.Duration;

import java.util.concurrent.TimeUnit;

public class StageStats
{
    private final DistributionStat getSplitDistribution = new DistributionStat();
    private final DistributionStat scheduleTaskDistribution = new DistributionStat();
    private final DistributionStat addSplitDistribution = new DistributionStat();

    public void addGetSplitDuration(Duration getSplitDuration)
    {
        getSplitDistribution.add(getSplitDuration.roundTo(TimeUnit.NANOSECONDS));
    }

    public void addScheduleTaskDuration(Duration scheduleTaskDuration)
    {
        scheduleTaskDistribution.add(scheduleTaskDuration.roundTo(TimeUnit.NANOSECONDS));
    }

    public void addAddSplitDuration(Duration addSplitDuration)
    {
        addSplitDistribution.add(addSplitDuration.roundTo(TimeUnit.NANOSECONDS));
    }

    public StageStatsSnapshot snapshot()
    {
        return new StageStatsSnapshot(getSplitDistribution.snapshot(), scheduleTaskDistribution.snapshot(), addSplitDistribution.snapshot());
    }

    public static class StageStatsSnapshot
    {

        private final DistributionStatSnapshot getSplitDistribution;
        private final DistributionStatSnapshot scheduleTaskDistribution;
        private final DistributionStatSnapshot addSplitDistribution;

        @JsonCreator
        public StageStatsSnapshot(
                @JsonProperty("getSplitDistribution") DistributionStatSnapshot getSplitDistribution,
                @JsonProperty("scheduleTaskDistribution") DistributionStatSnapshot scheduleTaskDistribution,
                @JsonProperty("addSplitDistribution") DistributionStatSnapshot addSplitDistribution)
        {
            this.getSplitDistribution = getSplitDistribution;
            this.scheduleTaskDistribution = scheduleTaskDistribution;
            this.addSplitDistribution = addSplitDistribution;
        }

        @JsonProperty
        public DistributionStatSnapshot getGetSplitDistribution()
        {
            return getSplitDistribution;
        }

        @JsonProperty
        public DistributionStatSnapshot getScheduleTaskDistribution()
        {
            return scheduleTaskDistribution;
        }

        @JsonProperty
        public DistributionStatSnapshot getAddSplitDistribution()
        {
            return addSplitDistribution;
        }

        @Override
        public String toString()
        {
            return Objects.toStringHelper(this)
                    .add("getSplitDistribution", getSplitDistribution)
                    .add("scheduleTaskDistribution", scheduleTaskDistribution)
                    .add("addSplitDistribution", addSplitDistribution)
                    .toString();
        }
    }
}
