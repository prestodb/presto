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
package io.prestosql.util;

import static com.google.common.base.Preconditions.checkState;
import static java.lang.Math.ceil;
import static java.lang.Math.floor;

/**
 * Estimates number of collisions when inserting values into hash table. The number of hash collisions is
 * estimated based on extrapolated precalculated values. Precalculated values are results of simulation
 * by HashCollisionsSimulator which mimics hash table with linear conflict resolution strategy (e.g:
 * after collision the next position is tried).
 * <p>
 * Extrapolation of estimates works very well for large hash tables. For instance assume c is the number of
 * collisions when inserting x entries into y sized hash table. Then the number of collisions for
 * inserting x*scale entries into y*scale sized hash table is approximately c*scale
 * (see TestHashCollisionsEstimator#hashEstimatesShouldApproximateSimulations).
 * <p>
 * It is not possible to deduce closed collisions formula for hash tables with linear conflict resolution
 * strategy. This is because one cannot assume uniform data distribution when inserting new value.
 */
public final class HashCollisionsEstimator
{
    private static final int NUMBER_OF_ESTIMATES = 500;
    private static final int NUMBER_OF_VALUES = 10000;
    private static final double[] COLLISION_ESTIMATES = new double[] {
            0.0, 9.77, 20.024, 30.252, 40.554, 50.588, 61.114, 71.45, 81.688, 92.836, 101.336, 112.032, 123.372, 133.756, 145.64, 154.32, 164.952, 174.632, 187.62, 196.424,
            208.184, 219.716, 230.614, 242.264, 252.744, 263.926, 274.3, 285.274, 296.816, 307.424, 318.646, 329.938, 341.668, 353.956, 363.388, 376.644, 386.668, 399.914, 410.468,
            422.378, 436.078, 447.45, 456.398, 469.622, 480.428, 494.58, 506.178, 518.132, 532.714, 541.288, 558.324, 566.92, 580.938, 592.824, 604.56, 617.244, 630.956, 642.238,
            654.44, 671.37, 685.586, 694.624, 708.336, 722.544, 735.21, 747.394, 761.752, 773.958, 790.866, 797.014, 812.912, 828.256, 844.09, 854.558, 870.072, 885.458, 894.42,
            909.33, 922.748, 939.542, 952.638, 965.736, 979.576, 998.964, 1010.66, 1021.938, 1038.35, 1050.82, 1068.298, 1081.764, 1098.044, 1111.256, 1127.532, 1146.96, 1157.656,
            1172.266, 1188.412, 1200.084, 1216.834, 1235.226, 1249.768, 1264.316, 1279.382, 1296.372, 1312.394, 1328.032, 1346.16, 1363.492, 1383.226, 1394.798, 1408.966, 1423.83,
            1441.808, 1462.546, 1479.822, 1489.714, 1509.042, 1522.374, 1545.284, 1557.652, 1580.138, 1598.45, 1616.112, 1627.698, 1643.522, 1666.49, 1684.924, 1701.194, 1724.148,
            1733.214, 1758.412, 1771.088, 1797.094, 1812.762, 1833.348, 1854.278, 1871.93, 1886.596, 1905.928, 1927.252, 1942.49, 1966.88, 1983.312, 2000.442, 2023.56, 2039.062,
            2062.678, 2083.068, 2105.072, 2120.706, 2138.808, 2168.57, 2183.936, 2199.936, 2226.798, 2245.812, 2266.584, 2285.928, 2309.734, 2328.444, 2352.354, 2376.536, 2402.294,
            2418.094, 2443.444, 2461.576, 2484.828, 2507.922, 2536.254, 2555.074, 2571.58, 2593.866, 2617.216, 2642.154, 2667.33, 2692.672, 2719.056, 2735.272, 2766.434, 2785.966,
            2807.468, 2835.996, 2859.238, 2888.196, 2909.236, 2937.988, 2958.628, 2984.464, 3017.96, 3033.648, 3060.132, 3088.574, 3115.622, 3141.564, 3165.196, 3200.648, 3222.186,
            3244.452, 3281.082, 3303.538, 3336.022, 3360.414, 3379.63, 3415.534, 3438.518, 3471.764, 3489.45, 3531.036, 3560.114, 3582.172, 3620.742, 3646.766, 3669.764, 3714.644,
            3747.624, 3776.196, 3805.862, 3825.116, 3869.156, 3898.632, 3921.91, 3962.276, 3994.01, 4028.342, 4059.09, 4085.262, 4117.49, 4157.104, 4188.836, 4226.378, 4268.47,
            4287.872, 4324.898, 4360.632, 4401.764, 4440.782, 4461.8, 4504.878, 4547.902, 4576.956, 4622.028, 4647.788, 4680.408, 4730.908, 4770.966, 4799.152, 4840.854, 4873.268,
            4929.096, 4956.518, 5010.552, 5044.25, 5070.934, 5106.284, 5165.32, 5192.742, 5231.81, 5283.418, 5318.836, 5361.454, 5418.306, 5448.182, 5507.672, 5537.532, 5585.002,
            5631.39, 5676.066, 5723.068, 5777.99, 5811.952, 5877.724, 5910.128, 5959.008, 6010.048, 6046.754, 6106.794, 6160.474, 6196.208, 6254.038, 6295.128, 6363.37, 6420.524,
            6457.304, 6520.848, 6561.016, 6621.352, 6683.314, 6726.154, 6799.27, 6844.538, 6903.418, 6963.012, 7019.088, 7095.776, 7130.266, 7207.904, 7252.17, 7311.628, 7365.094,
            7417.7, 7493.552, 7589.334, 7636.184, 7667.78, 7728.74, 7802.302, 7896.14, 7944.382, 8016.17, 8085.87, 8140.248, 8229.72, 8300.336, 8389.782, 8441.068, 8503.372,
            8592.394, 8662.81, 8716.386, 8847.04, 8890.432, 8937.702, 9021.488, 9135.3, 9217.924, 9262.18, 9366.716, 9452.71, 9540.108, 9624.502, 9690.812, 9800.146, 9851.196,
            9930.4, 10057.3, 10129.036, 10236.74, 10330.326, 10401.868, 10513.706, 10607.216, 10723.482, 10822.446, 10924.742, 10998.71, 11149.108, 11221.61, 11343.252, 11435.766,
            11543.124, 11643.298, 11792.074, 11880.884, 12047.458, 12090.936, 12215.204, 12342.962, 12475.982, 12582.538, 12747.408, 12817.378, 12942.814, 13079.614, 13190.086,
            13386.21, 13485.636, 13737.612, 13807.57, 13956.228, 14058.272, 14236.11, 14370.324, 14488.936, 14722.836, 14760.486, 14903.2, 15198.738, 15314.532, 15456.462,
            15680.98, 15792.084, 15994.078, 16148.108, 16332.378, 16518.304, 16721.042, 16880.698, 17197.852, 17247.04, 17552.978, 17689.292, 17889.01, 18092.816, 18320.444,
            18549.708, 18823.568, 18965.39, 19244.95, 19451.438, 19680.554, 19883.106, 20209.678, 20505.858, 20774.358, 20938.646, 21340.412, 21556.452, 21827.972, 22195.188,
            22305.75, 22574.176, 23048.314, 23387.916, 23603.244, 24037.3, 24353.368, 24669.308, 25040.574, 25434.616, 25665.626, 26124.556, 26483.002, 26741.316, 27504.694,
            27708.254, 28251.774, 28666.246, 29162.8, 29604.87, 30132.266, 30624.938, 31021.528, 31748.74, 32063.354, 32596.09, 33183.048, 33740.516, 34308.398, 34921.296,
            35578.168, 36443.842, 37171.19, 37729.726, 38558.536, 39188.07, 40275.304, 41004.746, 41633.674, 42534.212, 43852.122, 44657.702, 45656.31, 46661.722, 47618.776,
            48545.842, 50044.82, 51307.894, 52354.092, 53055.88, 54802.442, 57160.244, 57733.814, 60011.928, 61456.304, 62662.36, 64731.336, 67579.776, 69946.338, 71842.044,
            74065.938, 76336.674, 79927.376, 82401.81, 84608.874, 87426.684, 89748.418, 94656.746, 99003.738, 101930.618, 108859.888, 111733.806, 119535.11, 126429.324, 130021.28,
            139796.972, 146213.836, 155609.432, 167002.178, 178611.796, 191060.022, 210490.314, 220287.618, 247066.318, 270205.778, 290782.456, 321920.396, 360384.238, 404437.374,
            456302.552, 532254.102, 619384.78
    };

    private HashCollisionsEstimator() {}

    public static double estimateNumberOfHashCollisions(int numberOfValues, int hashSize)
    {
        checkState(0 <= numberOfValues && numberOfValues <= hashSize);

        if (hashSize == 0) {
            return 0d;
        }

        double estimateRescaleFactor = (double) numberOfValues / NUMBER_OF_VALUES;
        double estimateIndex = (double) NUMBER_OF_ESTIMATES * numberOfValues / hashSize;
        int lowerEstimateIndex = (int) floor(estimateIndex);
        int upperEstimateIndex = (int) ceil(estimateIndex);

        if (lowerEstimateIndex == upperEstimateIndex) {
            return COLLISION_ESTIMATES[lowerEstimateIndex] * estimateRescaleFactor;
        }

        double lowerEstimation = COLLISION_ESTIMATES[lowerEstimateIndex];
        double upperEstimation = COLLISION_ESTIMATES[upperEstimateIndex];
        double estimationIndexDistanceFromLower = estimateIndex - lowerEstimateIndex;

        return (lowerEstimation * (1 - estimationIndexDistanceFromLower) + upperEstimation * estimationIndexDistanceFromLower) * estimateRescaleFactor;
    }
}
