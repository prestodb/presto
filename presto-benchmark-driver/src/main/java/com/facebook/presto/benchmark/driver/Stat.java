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
package com.facebook.presto.benchmark.driver;

import org.apache.commons.math3.stat.descriptive.moment.Mean;
import org.apache.commons.math3.stat.descriptive.moment.StandardDeviation;
import org.apache.commons.math3.stat.descriptive.rank.Median;

import static com.google.common.base.MoreObjects.toStringHelper;

public class Stat
{
    private final double mean;
    private final double standardDeviation;
    private final double median;

    public Stat(double[] values)
    {
        mean = new Mean().evaluate(values);
        standardDeviation = new StandardDeviation().evaluate(values);
        median = new Median().evaluate(values);
    }

    public double getMean()
    {
        return mean;
    }

    public double getStandardDeviation()
    {
        return standardDeviation;
    }

    public double getMedian()
    {
        return median;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("mean", mean)
                .add("standardDeviation", standardDeviation)
                .add("median", median)
                .toString();
    }
}
