package com.facebook.presto.hive.util;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Objects;
import com.google.common.collect.ImmutableList;
import org.weakref.jmx.Managed;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class Distribution2
{
    private final static double MAX_ERROR = 0.01;

    private final QuantileDigest2 digest;

    public Distribution2()
    {
        digest = new QuantileDigest2(MAX_ERROR);
    }

    public Distribution2(double alpha)
    {
        digest = new QuantileDigest2(MAX_ERROR, alpha);
    }

    public void add(long value)
    {
        digest.add(value);
    }

    public void add(long value, long valueCount)
    {
        digest.add(value, valueCount);
    }

    @Managed
    public double getMaxError()
    {
        return digest.getConfidenceFactor();
    }

    @Managed
    public double getCount()
    {
        return digest.getCount();
    }

    @Managed
    public long getP01()
    {
        return digest.getQuantile(0.01);
    }

    @Managed
    public long getP05()
    {
        return digest.getQuantile(0.05);
    }

    @Managed
    public long getP10()
    {
        return digest.getQuantile(0.10);
    }

    @Managed
    public long getP25()
    {
        return digest.getQuantile(0.25);
    }

    @Managed
    public long getP50()
    {
        return digest.getQuantile(0.5);
    }

    @Managed
    public long getP75()
    {
        return digest.getQuantile(0.75);
    }

    @Managed
    public long getP90()
    {
        return digest.getQuantile(0.90);
    }

    @Managed
    public long getP95()
    {
        return digest.getQuantile(0.95);
    }

    @Managed
    public long getP99()
    {
        return digest.getQuantile(0.99);
    }

    @Managed
    public long getMin()
    {
        return digest.getMin();
    }

    @Managed
    public long getMax()
    {
        return digest.getMax();
    }

    @Managed
    public Map<Double, Long> getPercentiles()
    {
        List<Double> percentiles = new ArrayList<>(100);
        for (int i = 0; i < 100; ++i) {
            percentiles.add(i / 100.0);
        }

        List<Long> values = digest.getQuantiles(percentiles);

        Map<Double, Long> result = new LinkedHashMap<>(values.size());
        for (int i = 0; i < percentiles.size(); ++i) {
            result.put(percentiles.get(i), values.get(i));
        }

        return result;
    }

    public List<Long> getPercentiles(List<Double> percentiles)
    {
        return digest.getQuantiles(percentiles);
    }

    public DistributionSnapshot snapshot()
    {
        List<Long> quantiles = digest.getQuantiles(ImmutableList.of(0.01, 0.05, 0.10, 0.25, 0.5, 0.75, 0.9, 0.95, 0.99));
        return new DistributionSnapshot(
                getMaxError(),
                getCount(),
                quantiles.get(0),
                quantiles.get(1),
                quantiles.get(2),
                quantiles.get(3),
                quantiles.get(4),
                quantiles.get(5),
                quantiles.get(6),
                quantiles.get(7),
                quantiles.get(8),
                getMin(),
                getMax());
    }

    public static class DistributionSnapshot
    {
        private final double maxError;
        private final double count;
        private final long p01;
        private final long p05;
        private final long p10;
        private final long p25;
        private final long p50;
        private final long p75;
        private final long p90;
        private final long p95;
        private final long p99;
        private final long min;
        private final long max;

        @JsonCreator
        public DistributionSnapshot(
                @JsonProperty("maxError") double maxError,
                @JsonProperty("count") double count,
                @JsonProperty("p01") long p01,
                @JsonProperty("p05") long p05,
                @JsonProperty("p10") long p10,
                @JsonProperty("p25") long p25,
                @JsonProperty("p50") long p50,
                @JsonProperty("p75") long p75,
                @JsonProperty("p90") long p90,
                @JsonProperty("p95") long p95,
                @JsonProperty("p99") long p99,
                @JsonProperty("min") long min,
                @JsonProperty("max") long max)
        {
            this.maxError = maxError;
            this.count = count;
            this.p01 = p01;
            this.p05 = p05;
            this.p10 = p10;
            this.p25 = p25;
            this.p50 = p50;
            this.p75 = p75;
            this.p90 = p90;
            this.p95 = p95;
            this.p99 = p99;
            this.min = min;
            this.max = max;
        }

        @JsonProperty
        public double getMaxError()
        {
            return maxError;
        }

        @JsonProperty
        public double getCount()
        {
            return count;
        }

        @JsonProperty
        public long getP01()
        {
            return p01;
        }

        @JsonProperty
        public long getP05()
        {
            return p05;
        }

        @JsonProperty
        public long getP10()
        {
            return p10;
        }

        @JsonProperty
        public long getP25()
        {
            return p25;
        }

        @JsonProperty
        public long getP50()
        {
            return p50;
        }

        @JsonProperty
        public long getP75()
        {
            return p75;
        }

        @JsonProperty
        public long getP90()
        {
            return p90;
        }

        @JsonProperty
        public long getP95()
        {
            return p95;
        }

        @JsonProperty
        public long getP99()
        {
            return p99;
        }

        @JsonProperty
        public long getMin()
        {
            return min;
        }

        @JsonProperty
        public long getMax()
        {
            return max;
        }

        @Override
        public String toString()
        {
            return Objects.toStringHelper(this)
                    .add("maxError", maxError)
                    .add("count", count)
                    .add("p01", p01)
                    .add("p05", p05)
                    .add("p10", p10)
                    .add("p25", p25)
                    .add("p50", p50)
                    .add("p75", p75)
                    .add("p90", p90)
                    .add("p95", p95)
                    .add("p99", p99)
                    .add("min", min)
                    .add("max", max)
                    .toString();
        }
    }
}
