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
package io.prestosql.operator.aggregation;

import com.google.common.base.CaseFormat;
import io.prestosql.operator.aggregation.state.CentralMomentsState;
import io.prestosql.operator.aggregation.state.CorrelationState;
import io.prestosql.operator.aggregation.state.CovarianceState;
import io.prestosql.operator.aggregation.state.RegressionState;
import io.prestosql.operator.aggregation.state.VarianceState;
import io.prestosql.spi.Page;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.type.TypeSignature;

import java.util.List;
import java.util.function.Function;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Locale.ENGLISH;

public final class AggregationUtils
{
    private AggregationUtils()
    {
    }

    public static void updateVarianceState(VarianceState state, double value)
    {
        state.setCount(state.getCount() + 1);
        double delta = value - state.getMean();
        state.setMean(state.getMean() + delta / state.getCount());
        state.setM2(state.getM2() + delta * (value - state.getMean()));
    }

    public static void updateCentralMomentsState(CentralMomentsState state, double value)
    {
        long n1 = state.getCount();
        long n = n1 + 1;
        double m1 = state.getM1();
        double m2 = state.getM2();
        double m3 = state.getM3();
        double delta = value - m1;
        double deltaN = delta / n;
        double deltaN2 = deltaN * deltaN;
        double dm2 = delta * deltaN * n1;
        state.setCount(n);
        state.setM1(m1 + deltaN);
        state.setM2(m2 + dm2);
        state.setM3(m3 + dm2 * deltaN * (n - 2) - 3 * deltaN * m2);
        state.setM4(state.getM4() + dm2 * deltaN2 * (n * (double) n - 3 * n + 3) + 6 * deltaN2 * m2 - 4 * deltaN * m3);
    }

    public static void updateCovarianceState(CovarianceState state, double x, double y)
    {
        long n = state.getCount() + 1;
        state.setCount(n);
        double oldMeanX = state.getMeanX();
        state.setMeanX(oldMeanX + (x - oldMeanX) / n);
        double oldMeanY = state.getMeanY();
        double newMeanY = oldMeanY + (y - oldMeanY) / n;
        state.setMeanY(newMeanY);
        state.setC2(state.getC2() + (x - oldMeanX) * (y - newMeanY));
    }

    public static double getCovarianceSample(CovarianceState state)
    {
        return state.getC2() / (state.getCount() - 1);
    }

    public static double getCovariancePopulation(CovarianceState state)
    {
        return state.getC2() / state.getCount();
    }

    public static void updateCorrelationState(CorrelationState state, double x, double y)
    {
        double oldMeanX = state.getMeanX();
        double oldMeanY = state.getMeanY();
        updateCovarianceState(state, x, y);
        state.setM2X(state.getM2X() + (x - oldMeanX) * (x - state.getMeanX()));
        state.setM2Y(state.getM2Y() + (y - oldMeanY) * (y - state.getMeanY()));
    }

    public static double getCorrelation(CorrelationState state)
    {
        // This is defined as covariance(x, y) / (stdev(x) * stdev(y))
        double covariance = state.getC2();
        double stdevX = Math.sqrt(state.getM2X());
        double stdevY = Math.sqrt(state.getM2Y());

        // stdevX and stdevY deliberately not checked for zero because the result can be Infinity or NaN even
        // if they are both not zero
        return covariance / stdevX / stdevY;
    }

    public static void updateRegressionState(RegressionState state, double x, double y)
    {
        double oldMeanX = state.getMeanX();
        updateCovarianceState(state, x, y);
        state.setM2X(state.getM2X() + (x - oldMeanX) * (x - state.getMeanX()));
    }

    public static double getRegressionSlope(RegressionState state)
    {
        double dividend = state.getC2();
        double divisor = state.getM2X();

        // divisor deliberately not checked for zero because the result can be Infty or NaN even if it is not zero
        return dividend / divisor;
    }

    public static double getRegressionIntercept(RegressionState state)
    {
        double slope = getRegressionSlope(state);
        double meanX = state.getMeanX();
        double meanY = state.getMeanY();

        return meanY - slope * meanX;
    }

    public static void mergeVarianceState(VarianceState state, VarianceState otherState)
    {
        long count = otherState.getCount();
        double mean = otherState.getMean();
        double m2 = otherState.getM2();

        checkArgument(count >= 0, "count is negative");
        if (count == 0) {
            return;
        }
        long newCount = count + state.getCount();
        double newMean = ((count * mean) + (state.getCount() * state.getMean())) / (double) newCount;
        double delta = mean - state.getMean();
        state.setM2(state.getM2() + m2 + delta * delta * count * state.getCount() / (double) newCount);
        state.setCount(newCount);
        state.setMean(newMean);
    }

    public static void mergeCentralMomentsState(CentralMomentsState state, CentralMomentsState otherState)
    {
        long na = state.getCount();
        long nb = otherState.getCount();

        checkArgument(nb >= 0, "count is negative");
        if (nb == 0) {
            return;
        }

        double m1a = state.getM1();
        double m2a = state.getM2();
        double m3a = state.getM3();
        double m1b = otherState.getM1();
        double m2b = otherState.getM2();
        double m3b = otherState.getM3();
        double n = na + nb; // Use double as type of n to avoid integer overflow for n*n and n*n*n
        double delta = m1b - m1a;
        double delta2 = delta * delta;
        double delta3 = delta * delta2;
        double delta4 = delta2 * delta2;

        state.setCount((long) n);
        state.setM1((na * m1a + nb * m1b) / n);
        state.setM2(m2a + m2b + delta2 * na * nb / n);
        state.setM3(m3a + m3b
                + delta3 * na * nb * (na - nb) / (n * n)
                + 3 * delta * (na * m2b - nb * m2a) / n);
        state.setM4(state.getM4() + otherState.getM4()
                + delta4 * na * nb * (na * na - na * nb + nb * nb) / (n * n * n)
                + 6 * delta2 * (na * na * m2b + nb * nb * m2a) / (n * n)
                + 4 * delta * (na * m3b - nb * m3a) / n);
    }

    private static void updateCovarianceState(CovarianceState state, CovarianceState otherState)
    {
        long na = state.getCount();
        long nb = otherState.getCount();
        long n = na + nb;
        state.setCount(n);
        double meanX = state.getMeanX();
        double meanY = state.getMeanY();
        double deltaX = otherState.getMeanX() - meanX;
        double deltaY = otherState.getMeanY() - meanY;
        state.setC2(state.getC2() + otherState.getC2() + deltaX * deltaY * na * nb / (double) n);
        state.setMeanX(meanX + deltaX * nb / (double) n);
        state.setMeanY(meanY + deltaY * nb / (double) n);
    }

    public static void mergeCovarianceState(CovarianceState state, CovarianceState otherState)
    {
        if (otherState.getCount() == 0) {
            return;
        }

        updateCovarianceState(state, otherState);
    }

    public static void mergeCorrelationState(CorrelationState state, CorrelationState otherState)
    {
        if (otherState.getCount() == 0) {
            return;
        }

        long na = state.getCount();
        long nb = otherState.getCount();
        state.setM2X(state.getM2X() + otherState.getM2X() + na * nb * Math.pow(state.getMeanX() - otherState.getMeanX(), 2) / (double) (na + nb));
        state.setM2Y(state.getM2Y() + otherState.getM2Y() + na * nb * Math.pow(state.getMeanY() - otherState.getMeanY(), 2) / (double) (na + nb));
        updateCovarianceState(state, otherState);
    }

    public static void mergeRegressionState(RegressionState state, RegressionState otherState)
    {
        if (otherState.getCount() == 0) {
            return;
        }

        long na = state.getCount();
        long nb = otherState.getCount();
        state.setM2X(state.getM2X() + otherState.getM2X() + na * nb * Math.pow(state.getMeanX() - otherState.getMeanX(), 2) / (double) (na + nb));
        updateCovarianceState(state, otherState);
    }

    public static String generateAggregationName(String baseName, TypeSignature outputType, List<TypeSignature> inputTypes)
    {
        StringBuilder sb = new StringBuilder();
        sb.append(CaseFormat.LOWER_CAMEL.to(CaseFormat.UPPER_CAMEL, outputType.toString()));
        for (TypeSignature inputType : inputTypes) {
            sb.append(CaseFormat.LOWER_CAMEL.to(CaseFormat.UPPER_CAMEL, inputType.toString()));
        }
        sb.append(CaseFormat.LOWER_UNDERSCORE.to(CaseFormat.UPPER_CAMEL, baseName.toLowerCase(ENGLISH)));

        return sb.toString();
    }

    // used by aggregation compiler
    @SuppressWarnings("UnusedDeclaration")
    public static Function<Integer, Block> pageBlockGetter(final Page page)
    {
        return page::getBlock;
    }
}
