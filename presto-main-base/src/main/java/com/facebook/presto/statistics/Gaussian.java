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
package com.facebook.presto.statistics;

import io.airlift.slice.Slice;
import io.airlift.slice.Slices;

import java.util.Arrays;

import static com.facebook.presto.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static com.facebook.presto.util.Failures.checkCondition;
import static com.google.common.base.Preconditions.checkArgument;
import static io.airlift.slice.SizeOf.SIZE_OF_BYTE;
import static io.airlift.slice.SizeOf.SIZE_OF_DOUBLE;
import static java.lang.Math.max;
import static java.lang.Math.min;
import static java.util.Objects.requireNonNull;

/**
 * The parameters of a fitted multivariate Gaussian distribution, and the in-memory form of
 * {@link GaussianType}.
 */
public class Gaussian
{
    public static final byte FORMAT_VERSION = 1;

    /**
     * The dimensionality is stored in a single byte.
     */
    public static final int MAX_DIMENSION = Byte.MAX_VALUE;

    private static final int VERSION_OFFSET = 0;
    private static final int DIMENSION_OFFSET = VERSION_OFFSET + SIZE_OF_BYTE;
    private static final int WEIGHT_SUM_OFFSET = DIMENSION_OFFSET + SIZE_OF_BYTE;
    private static final int MEAN_OFFSET = WEIGHT_SUM_OFFSET + SIZE_OF_DOUBLE;

    private final double weightSum;
    private final double[] mean;
    private final double[] covarianceUpper;

    /**
     * @param weightSum the sum of the weights of the points fitted, not a per-point weight
     * @param mean the mean vector, whose length fixes the dimensionality
     * @param covarianceUpper the upper triangle of the covariance matrix, in row-major order
     */
    public Gaussian(double weightSum, double[] mean, double[] covarianceUpper)
    {
        requireNonNull(mean, "mean is null");
        requireNonNull(covarianceUpper, "covarianceUpper is null");
        checkArgument(
                mean.length >= 1 && mean.length <= MAX_DIMENSION,
                "dimensionality must be between 1 and %s, found %s",
                MAX_DIMENSION,
                mean.length);
        checkArgument(
                covarianceUpper.length == covarianceEntryCount(mean.length),
                "dimensionality %s requires %s covariance entries, found %s",
                mean.length,
                covarianceEntryCount(mean.length),
                covarianceUpper.length);

        this.weightSum = weightSum;
        this.mean = mean.clone();
        this.covarianceUpper = covarianceUpper.clone();
    }

    public static Gaussian deserialize(Slice serialized)
    {
        requireNonNull(serialized, "serialized is null");
        checkCondition(serialized.length() > MEAN_OFFSET, INVALID_FUNCTION_ARGUMENT, "Gaussian is truncated");
        byte version = serialized.getByte(VERSION_OFFSET);
        checkCondition(version == FORMAT_VERSION, INVALID_FUNCTION_ARGUMENT, "Unsupported Gaussian format version: %s", version);
        int dimension = serialized.getByte(DIMENSION_OFFSET);
        checkCondition(dimension >= 1, INVALID_FUNCTION_ARGUMENT, "Gaussian dimensionality must be at least 1, found %s", dimension);
        checkCondition(
                serialized.length() == serializedLength(dimension),
                INVALID_FUNCTION_ARGUMENT,
                "Gaussian of dimensionality %s must be %s bytes, found %s",
                dimension,
                serializedLength(dimension),
                serialized.length());

        double[] mean = new double[dimension];
        for (int i = 0; i < dimension; i++) {
            mean[i] = serialized.getDouble(MEAN_OFFSET + (i * SIZE_OF_DOUBLE));
        }
        double[] covarianceUpper = new double[covarianceEntryCount(dimension)];
        int covarianceOffset = covarianceOffset(dimension);
        for (int i = 0; i < covarianceUpper.length; i++) {
            covarianceUpper[i] = serialized.getDouble(covarianceOffset + (i * SIZE_OF_DOUBLE));
        }
        return new Gaussian(serialized.getDouble(WEIGHT_SUM_OFFSET), mean, covarianceUpper);
    }

    public Slice serialize()
    {
        int dimension = getDimension();
        Slice serialized = Slices.allocate(serializedLength(dimension));
        serialized.setByte(VERSION_OFFSET, FORMAT_VERSION);
        serialized.setByte(DIMENSION_OFFSET, dimension);
        serialized.setDouble(WEIGHT_SUM_OFFSET, weightSum);
        for (int i = 0; i < dimension; i++) {
            serialized.setDouble(MEAN_OFFSET + (i * SIZE_OF_DOUBLE), mean[i]);
        }
        int covarianceOffset = covarianceOffset(dimension);
        for (int i = 0; i < covarianceUpper.length; i++) {
            serialized.setDouble(covarianceOffset + (i * SIZE_OF_DOUBLE), covarianceUpper[i]);
        }
        return serialized;
    }

    public int getDimension()
    {
        return mean.length;
    }

    /**
     * The sum of the weights of the points the distribution was fitted to. Weights are
     * frequency weights, so this is the number of observations it stands for.
     */
    public double getWeightSum()
    {
        return weightSum;
    }

    public double[] getMean()
    {
        return mean.clone();
    }

    public double getMean(int index)
    {
        return mean[index];
    }

    /**
     * @return the upper triangle of the covariance matrix, in row-major order
     */
    public double[] getCovarianceUpper()
    {
        return covarianceUpper.clone();
    }

    /**
     * The covariance matrix is symmetric, so {@code (row, column)} and {@code (column, row)}
     * return the same entry.
     */
    public double getCovariance(int row, int column)
    {
        int dimension = getDimension();
        checkArgument(row >= 0 && row < dimension, "row must be between 0 and %s, found %s", dimension - 1, row);
        checkArgument(column >= 0 && column < dimension, "column must be between 0 and %s, found %s", dimension - 1, column);
        int i = min(row, column);
        int j = max(row, column);
        return covarianceUpper[((i * dimension) - ((i * (i - 1)) / 2)) + (j - i)];
    }

    public static int serializedLength(int dimension)
    {
        return MEAN_OFFSET + ((dimension + covarianceEntryCount(dimension)) * SIZE_OF_DOUBLE);
    }

    public static int covarianceEntryCount(int dimension)
    {
        return (dimension * (dimension + 1)) / 2;
    }

    private static int covarianceOffset(int dimension)
    {
        return MEAN_OFFSET + (dimension * SIZE_OF_DOUBLE);
    }

    @Override
    public boolean equals(Object other)
    {
        if (this == other) {
            return true;
        }
        if (!(other instanceof Gaussian)) {
            return false;
        }
        Gaussian that = (Gaussian) other;
        return Double.compare(weightSum, that.weightSum) == 0
                && Arrays.equals(mean, that.mean)
                && Arrays.equals(covarianceUpper, that.covarianceUpper);
    }

    @Override
    public int hashCode()
    {
        return (31 * ((31 * Double.hashCode(weightSum)) + Arrays.hashCode(mean))) + Arrays.hashCode(covarianceUpper);
    }

    @Override
    public String toString()
    {
        return "Gaussian{weightSum=" + weightSum + ", mean=" + Arrays.toString(mean) + ", covarianceUpper=" + Arrays.toString(covarianceUpper) + "}";
    }
}
