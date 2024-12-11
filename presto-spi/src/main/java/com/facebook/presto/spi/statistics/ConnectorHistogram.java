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

import com.fasterxml.jackson.annotation.JsonTypeInfo;

/**
 * This interface contains functions which the Presto optimizer can use to
 * answer questions about a particular column's data distribution. These
 * functions will be used to return answers to the query optimizer to build
 * more realistic cost models for joins and filter predicates.
 * <br>
 * Currently, this interface supports representing histograms of columns whose
 * domains map to real values.
 * <br>
 * Null values should not be represented in underlying histogram implementation.
 * When calculating filter statistics using the {@link ColumnStatisticType#NUMBER_OF_NON_NULL_VALUES}
 * are used to account for nulls in cost-based calculations.
 *
 * @see ColumnStatisticType#NUMBER_OF_NON_NULL_VALUES
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.MINIMAL_CLASS, property = "@class")
public interface ConnectorHistogram
{
    /**
     * Calculates an estimate for the percentile at which a particular value
     * falls in a distribution.
     * <br>
     * Put another way, this function returns the value of F(x) where F(x)
     * represents the CDF of this particular distribution. Traditionally, the
     * true CDF of a random variable X is represented by F(x) = P(x <= X). This
     * function signature allows for a slight modification by using the
     * {@code inclusive} parameter to return the value for F(x) = P(x < X)
     * should the underlying implementation support it.
     *
     * @param value the value to calculate percentile
     * @param inclusive whether this calculation should be inclusive or exclusive of the value (<= or <)
     * @return an {@link Estimate} of the percentile
     */
    Estimate cumulativeProbability(double value, boolean inclusive);

    /**
     * Calculates the value which occurs at a particular percentile in the given
     * distribution.
     * <br>
     * Put another way, calculates the inverse CDF. Given F(x) is the CDF of
     * a particular distribution, this function computes F^(-1)(x).
     *
     * @param percentile the percentile. Must be in the range [0.0, 1.0]
     * @return the value in the distribution corresponding to the percentile
     */
    Estimate inverseCumulativeProbability(double percentile);

    /**
     * @return an approximation of the memory utilization of this histogram.
     */
    long getEstimatedSize();
}
