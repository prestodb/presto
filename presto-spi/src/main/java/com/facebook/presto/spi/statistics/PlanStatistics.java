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
package com.facebook.presto.spi.statistics;

import static java.util.Objects.requireNonNull;

public class PlanStatistics
{
    private static final PlanStatistics EMPTY = new PlanStatistics(Estimate.unknown(), Estimate.unknown(), 0);

    private final Estimate rowCount;
    private final Estimate outputSize;
    // A number ranging between 0 and 1, reflecting our confidence in the statistics
    private final double confidence;

    public static PlanStatistics empty()
    {
        return EMPTY;
    }

    public PlanStatistics(Estimate rowCount, Estimate outputSize, double confidence)
    {
        this.rowCount = requireNonNull(rowCount, "rowCount can not be null");
        this.outputSize = requireNonNull(outputSize, "rowCount can not be null");
        checkArgument(confidence >= 0 && confidence <= 1, "confidence should be between 0 and 1");
        this.confidence = confidence;
    }

    public Estimate getRowCount()
    {
        return rowCount;
    }

    public Estimate getOutputSize()
    {
        return outputSize;
    }

    public double getConfidence()
    {
        return confidence;
    }

    private static void checkArgument(boolean condition, String message)
    {
        if (!condition) {
            throw new IllegalArgumentException(message);
        }
    }
}
