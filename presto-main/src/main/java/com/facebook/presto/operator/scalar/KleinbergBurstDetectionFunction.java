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
package com.facebook.presto.operator.scalar;

import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.function.Description;
import com.facebook.presto.spi.function.ScalarFunction;
import com.facebook.presto.spi.function.SqlType;
import com.facebook.presto.spi.type.StandardTypes;

import java.util.Arrays;

import static com.facebook.presto.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.IntegerType.INTEGER;
import static com.facebook.presto.util.Failures.checkCondition;
import static com.google.common.base.Preconditions.checkArgument;

public final class KleinbergBurstDetectionFunction
{
    private KleinbergBurstDetectionFunction() {}

    /**
     * Base of the exponential distribution that is used for modeling the
     * event frequencies. Must be greater than 1.0, and determines the granularity
     * of burst intensities detected (greater value, lower and more compact
     * intensities).
     */
    private static final double DEFAULT_KLEINBERG_S = 2.0;

    /**
     * Coefficient for the transition costs between states. Should be greater or
     * equal than 1.0, although any value is accepted. High values make increases
     * in the cost of intervals higher, amplifying big distances.
     */
    private static final double DEFAULT_KLEINBERG_GAMMA = 1.0;

    @ScalarFunction
    @Description("Return the Kleinberg burst detection for given timeSeries")
    @SqlType("array(int)")
    public static Block kleinbergBurstDetection(@SqlType("array(double)") Block timeSeries)
    {
        return kleinbergBurstDetection(timeSeries, DEFAULT_KLEINBERG_S, DEFAULT_KLEINBERG_GAMMA);
    }

    @ScalarFunction
    @Description("Return the Kleinberg burst detection for given timeSeries, s and gamma")
    @SqlType("array(int)")
    public static Block kleinbergBurstDetection(
            @SqlType("array(double)") Block timeSeries,
            @SqlType(StandardTypes.DOUBLE) double s,
            @SqlType(StandardTypes.DOUBLE) double gamma)
    {
        checkCondition(timeSeries.getPositionCount() > 1, INVALID_FUNCTION_ARGUMENT, "time series must have at least two values");
        checkCondition(s > 1.0, INVALID_FUNCTION_ARGUMENT, "s: base of exponential distribution must be greater than 1");
        checkCondition(gamma >= 1.0, INVALID_FUNCTION_ARGUMENT, "gamma: must be greater than or equal to 1");

        return kleinberg(timeSeries, s, gamma);
    }

    /**
     * Burst detection algorithm.
     * @param timeSeries for which kleinberg burst detection need to be computed
     * @param s base of the exponential distribution used for modeling the event frequencies
     * @param gamma the coefficient for the transition costs between states
     * @return burst intensity from each input timestamp in 'timeSeries'
     */
    private static Block kleinberg(Block timeSeries, double s, double gamma)
    {
        // The number of events [N].
        int numEntries = timeSeries.getPositionCount();

        // Calculate time intervals between successive events.
        double[] intervals = new double[numEntries - 1];
        // The minimum interval.
        double delta = Double.MAX_VALUE;
        for (int i = 0; i < numEntries - 1; i++) {
            double timeCurrent = DOUBLE.getDouble(timeSeries, i);
            double timeNext = DOUBLE.getDouble(timeSeries, i + 1);
            // 'timeSeries' should be sorted in ascending order.
            checkCondition(timeNext > timeCurrent, INVALID_FUNCTION_ARGUMENT, "time series must be sorted in ascending order");
            intervals[i] = timeNext - timeCurrent;
            delta = Math.min(delta, intervals[i]);
        }

        // The time length of the whole timeSeries [T].
        double timeLength = DOUBLE.getDouble(timeSeries, numEntries - 1) - DOUBLE.getDouble(timeSeries, 0);
        checkCondition(timeLength > 0, INVALID_FUNCTION_ARGUMENT, "time length of the whole series must be greater than 0");

        // The upper limit of burst levels [K].
        int burstLevelUpperLimit = (int) (Math.ceil(1 + Math.log(timeLength / delta) / Math.log(s)));
        checkArgument(burstLevelUpperLimit > 0);

        double[] alpha = new double[burstLevelUpperLimit];  // Event rate at each burst level.
        double[] alphaLn = new double[burstLevelUpperLimit];

        alpha[0] = numEntries / timeLength;  // Average event rate.
        alphaLn[0] = Math.log(alpha[0]);

        for (int i = 1; i < burstLevelUpperLimit; i++) {
            alpha[i] = s * alpha[i - 1];
            alphaLn[i] = Math.log(alpha[i]);
        }

        // Initialize.
        Node[] q = new Node[burstLevelUpperLimit];
        double[] costs = new double[burstLevelUpperLimit];
        Arrays.fill(costs, Double.MAX_VALUE);
        costs[0] = 0;

        // Start optimization.
        double gammaLogN = gamma * Math.log((double) numEntries);
        for (double interval : intervals) {
            Node[] qNew = new Node[burstLevelUpperLimit];
            double[] costsNew = new double[burstLevelUpperLimit];
            for (int i = 0; i < burstLevelUpperLimit; i++) {
                double[] c = new double[burstLevelUpperLimit];
                for (int j = 0; j < burstLevelUpperLimit; j++) {
                    c[j] = costs[j] + tau(gammaLogN, j, i);
                }
                int jMin = findIndexOfSmallest(c);
                // Store the cost for setting the burst level i.
                costsNew[i] = -logf(alpha, alphaLn, i, interval) + c[jMin];
                qNew[i] = new Node(i, q[jMin]);
            }
            q = qNew;
            costs = costsNew;
        }

        // Build output structure.
        int[] bursts = new int[numEntries];
        int seqMin = findIndexOfSmallest(costs);
        int count = 0;
        Node p = q[seqMin];
        while (p != null) {
            count++;
            bursts[numEntries - count] = p.getState();
            p = p.getParent();
        }

        BlockBuilder burstsBuilder = INTEGER.createBlockBuilder(null, numEntries);
        for (int i = 0; i < numEntries; i++) {
            burstsBuilder.writeInt(bursts[i]);
        }

        return burstsBuilder.build();
    }

    // Internal helper structure to hold discovered burst nodes.
    private static class Node
    {
        Node(int state, Node parent)
        {
            this.state = state;
            this.parent = parent;
        }

        private final int state; // burst level: 0, 1, ..., K-1
        private Node parent;

        int getState()
        {
            return state;
        }

        Node getParent()
        {
            return parent;
        }


    };

    /**
     * Cost function for reproducing a given interval.
     * @param i the burst level
     * @param x interval
     */
    private static double logf(double[] alpha, double[] lnAlpha, int i, double x)
    {
        return lnAlpha[i] - alpha[i] * x;
    }

    /**
     * Cost function for changing a burst level.
     * @param gammaLogN
     * @param i the previous burst level
     * @param j the next burst level
     */
    private static double tau(double gammaLogN, int i, int j)
    {
        return i < j ? (j - i) * gammaLogN : 0.0;
    }

    /**
     * Returns the index of the smallest value in an array.
     */
    private static int findIndexOfSmallest(double[] v)
    {
        checkArgument(v.length > 0, "array is empty");
        int min = 0;
        for (int i = 1; i < v.length; i++) {
            if (v[i] < v[min]) {
                min = i;
            }
        }
        return min;
    }
}
