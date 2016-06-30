/*
 * Copyright 2013-2016, Teradata, Inc. All rights reserved.
 */
package com.teradata.benchto.service.model;

import com.google.common.collect.Iterables;
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;

import java.io.Serializable;
import java.util.Collection;

public class AggregatedMeasurement
        implements Serializable
{
    private final MeasurementUnit unit;
    private final double min, max, mean, stdDev, stdDevPercent;

    public AggregatedMeasurement(MeasurementUnit unit, double min, double max, double mean, double stdDev, double stdDevPercent)
    {
        this.unit = unit;
        this.stdDev = stdDev;
        this.mean = mean;
        this.max = max;
        this.min = min;
        this.stdDevPercent = stdDevPercent;
    }

    public static AggregatedMeasurement aggregate(MeasurementUnit unit, Collection<Double> values)
    {
        if (values.size() < 2) {
            Double value = Iterables.getOnlyElement(values);
            return new AggregatedMeasurement(unit, value, value, value, 0.0, 0.0);
        }
        DescriptiveStatistics statistics = new DescriptiveStatistics(values.stream()
                .mapToDouble(Double::doubleValue)
                .toArray());

        double stdDevPercent = 0.0;
        if (statistics.getStandardDeviation() > 0.0) {
            stdDevPercent = (statistics.getStandardDeviation() / statistics.getMean()) * 100;
        }

        return new AggregatedMeasurement(unit,
                statistics.getMin(),
                statistics.getMax(),
                statistics.getMean(),
                statistics.getStandardDeviation(),
                stdDevPercent
        );
    }

    public MeasurementUnit getUnit()
    {
        return unit;
    }

    public double getMin()
    {
        return min;
    }

    public double getMax()
    {
        return max;
    }

    public double getMean()
    {
        return mean;
    }

    public double getStdDev()
    {
        return stdDev;
    }

    public double getStdDevPercent()
    {
        return stdDevPercent;
    }
}
