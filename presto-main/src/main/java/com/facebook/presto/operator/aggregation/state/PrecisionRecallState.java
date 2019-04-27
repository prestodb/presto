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
package com.facebook.presto.operator.aggregation.state;

import com.facebook.presto.spi.function.AccumulatorState;

import java.math.BigInteger;

public interface PrecisionRecallState
        extends AccumulatorState
{
    void setNull(boolean Null);

    boolean getNull();

    void setNumBins(BigInteger numBins);

    BigInteger getNumBins();

    void setWeight(double weight);

    double getWeight();

    ArrayList<double> getTrueWeights();

    void setTrue(ArrayList<double> trueWeights);

    ArrayList<double> getFalseWeights();

    void setTrues(ArrayList<double> falseWeights);
}
