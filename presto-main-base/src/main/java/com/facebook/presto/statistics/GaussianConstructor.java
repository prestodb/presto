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

import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.type.ArrayType;
import com.facebook.presto.common.type.StandardTypes;
import com.facebook.presto.spi.function.Description;
import com.facebook.presto.spi.function.ScalarFunction;
import com.facebook.presto.spi.function.SqlType;
import io.airlift.slice.Slice;

import static com.facebook.presto.common.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static com.facebook.presto.util.Failures.checkCondition;
import static java.lang.Double.isFinite;
import static java.lang.Math.abs;
import static java.lang.Math.max;

/**
 * Builds a {@link GaussianType} value from its parameters, for a distribution that is
 * known rather than fitted.
 * <p>
 * Everything a caller supplies is checked here, because this is the only place a Gaussian
 * enters the engine from arbitrary user input: a cast from varbinary is unchecked by
 * design, and the fitting aggregation produces valid values by construction. Rejecting a
 * bad matrix at the call site that supplied it is the difference between an error naming
 * the problem and a NaN surfacing somewhere unrelated.
 */
public final class GaussianConstructor
{
    /**
     * Relative floor on the gap between a matrix and its transpose, and on each pivot of
     * the Cholesky decomposition below which the matrix is treated as degenerate. Both
     * comparisons scale with the entries they are applied to, so neither depends on the
     * units of the input: variances of 1e-15 describe a real distribution, and a matrix
     * mixing degrees with metres is held to the same standard along each axis separately.
     * Matched to the aggregation's own tolerance, so a declared distribution faces the
     * same test as a fitted one.
     */
    private static final double TOLERANCE = 1e-12;

    private static final ArrayType DOUBLE_ARRAY = new ArrayType(DOUBLE);

    private GaussianConstructor() {}

    /**
     * Without a weight the distribution stands for a single observation, which is not
     * enough to merge with: merging needs the number of observations each side represents.
     */
    @ScalarFunction("gaussian")
    @Description("A Gaussian distribution with the given mean and covariance")
    @SqlType(GaussianType.NAME)
    public static Slice gaussian(
            @SqlType("array(double)") Block mean,
            @SqlType("array(array(double))") Block covariance)
    {
        return gaussian(mean, covariance, 1.0);
    }

    @ScalarFunction("gaussian")
    @Description("A Gaussian distribution with the given mean, covariance and total weight")
    @SqlType(GaussianType.NAME)
    public static Slice gaussian(
            @SqlType("array(double)") Block mean,
            @SqlType("array(array(double))") Block covariance,
            @SqlType(StandardTypes.DOUBLE) double weightSum)
    {
        int dimension = mean.getPositionCount();
        checkCondition(
                dimension >= 1 && dimension <= Gaussian.MAX_DIMENSION,
                INVALID_FUNCTION_ARGUMENT,
                "mean must have between 1 and %s elements, found %s",
                Gaussian.MAX_DIMENSION,
                dimension);
        checkCondition(
                covariance.getPositionCount() == dimension,
                INVALID_FUNCTION_ARGUMENT,
                "covariance must have one row per element of mean, found %s rows for %s elements",
                covariance.getPositionCount(),
                dimension);
        checkCondition(
                weightSum >= 0 && isFinite(weightSum),
                INVALID_FUNCTION_ARGUMENT,
                "weight sum must be finite and not negative, found %s",
                weightSum);

        return new Gaussian(weightSum, readMean(mean, dimension), readCovariance(covariance, dimension)).serialize();
    }

    private static double[] readMean(Block mean, int dimension)
    {
        double[] values = new double[dimension];
        for (int i = 0; i < dimension; i++) {
            checkCondition(!mean.isNull(i), INVALID_FUNCTION_ARGUMENT, "mean must not contain nulls");
            values[i] = DOUBLE.getDouble(mean, i);
            checkCondition(isFinite(values[i]), INVALID_FUNCTION_ARGUMENT, "mean must be finite, found %s", values[i]);
        }
        return values;
    }

    /**
     * @return the upper triangle of the covariance matrix, in row-major order
     */
    private static double[] readCovariance(Block covariance, int dimension)
    {
        double[][] matrix = new double[dimension][];
        for (int row = 0; row < dimension; row++) {
            checkCondition(!covariance.isNull(row), INVALID_FUNCTION_ARGUMENT, "covariance must not contain null rows");
            Block values = (Block) DOUBLE_ARRAY.getObject(covariance, row);
            checkCondition(
                    values.getPositionCount() == dimension,
                    INVALID_FUNCTION_ARGUMENT,
                    "covariance must be square, found a row of %s entries for %s elements of mean",
                    values.getPositionCount(),
                    dimension);
            matrix[row] = new double[dimension];
            for (int column = 0; column < dimension; column++) {
                checkCondition(!values.isNull(column), INVALID_FUNCTION_ARGUMENT, "covariance must not contain nulls");
                matrix[row][column] = DOUBLE.getDouble(values, column);
                checkCondition(
                        isFinite(matrix[row][column]),
                        INVALID_FUNCTION_ARGUMENT,
                        "covariance must be finite, found %s",
                        matrix[row][column]);
            }
        }

        checkSymmetric(matrix, dimension);

        // Average the halves before checking definiteness, so that what is checked is what
        // is stored: averaging moves the matrix by up to the symmetry tolerance, which is
        // enough to carry a nearly degenerate one across the boundary. Halving each side
        // before adding keeps the sum finite for entries near the top of the range.
        double[][] symmetric = new double[dimension][dimension];
        for (int row = 0; row < dimension; row++) {
            for (int column = 0; column < dimension; column++) {
                symmetric[row][column] = (matrix[row][column] / 2) + (matrix[column][row] / 2);
            }
        }
        checkPositiveDefinite(symmetric, dimension);

        double[] upper = new double[Gaussian.covarianceEntryCount(dimension)];
        int entry = 0;
        for (int row = 0; row < dimension; row++) {
            for (int column = row; column < dimension; column++) {
                upper[entry] = symmetric[row][column];
                entry++;
            }
        }
        return upper;
    }

    /**
     * Only the upper triangle is stored, so an asymmetric matrix would have its lower half
     * silently discarded. The comparison is relative, because a covariance computed
     * numerically can differ across the diagonal in the last bits.
     */
    private static void checkSymmetric(double[][] matrix, int dimension)
    {
        for (int row = 0; row < dimension; row++) {
            for (int column = row + 1; column < dimension; column++) {
                double scale = max(abs(matrix[row][column]), abs(matrix[column][row]));
                checkCondition(
                        abs(matrix[row][column] - matrix[column][row]) <= TOLERANCE * scale,
                        INVALID_FUNCTION_ARGUMENT,
                        "covariance must be symmetric, found %s at (%s, %s) and %s at (%s, %s)",
                        matrix[row][column],
                        row + 1,
                        column + 1,
                        matrix[column][row],
                        column + 1,
                        row + 1);
            }
        }
    }

    /**
     * A Cholesky decomposition, which succeeds exactly when the matrix is positive
     * definite. Written out rather than taken from a library because it is a dozen lines
     * and avoids converting the matrix into another representation first.
     */
    private static void checkPositiveDefinite(double[][] matrix, int dimension)
    {
        double[][] lower = new double[dimension][dimension];
        for (int row = 0; row < dimension; row++) {
            for (int column = 0; column <= row; column++) {
                double sum = matrix[row][column];
                for (int k = 0; k < column; k++) {
                    sum -= lower[row][k] * lower[column][k];
                }
                if (row == column) {
                    checkCondition(
                            sum > TOLERANCE * abs(matrix[row][row]),
                            INVALID_FUNCTION_ARGUMENT,
                            "covariance must be positive definite, which rules out a distribution with no extent in some direction");
                    lower[row][column] = Math.sqrt(sum);
                }
                else {
                    lower[row][column] = sum / lower[column][column];
                }
            }
        }
    }
}
