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
package com.facebook.presto.common.predicate.vector;

import jdk.incubator.vector.DoubleVector;
import jdk.incubator.vector.VectorMask;
import jdk.incubator.vector.VectorSpecies;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;

public class TestTupleDomainFilterVector
{
    @Test
    public void testDoubleRangeVector()
    {
        VectorSpecies<Double> speciesDouble = DoubleVector.SPECIES_MAX;
        double[] values = new double[speciesDouble.length()];
        for (int i = 0; i < speciesDouble.length(); i++) {
            values[i] = (double) i;
        }
        DoubleVector valuesVector = DoubleVector.fromArray(speciesDouble, values, 0);
        TupleDomainFilterVector tupleDomainFilterVector = DoubleRangeVector.of((double) 1, false, false, (double) 1, false, false, false);
        VectorMask<Double> result = tupleDomainFilterVector.testDoubleVector(valuesVector);
        for (int i = 0; i < speciesDouble.length(); i++) {
            assertEquals(tupleDomainFilterVector.testDouble(values[i]), result.laneIsSet(i));
        }
        tupleDomainFilterVector = DoubleRangeVector.of((double) 1, false, false, (double) 10, false, false, false);
        result = tupleDomainFilterVector.testDoubleVector(valuesVector);
        for (int i = 0; i < speciesDouble.length(); i++) {
            assertEquals(tupleDomainFilterVector.testDouble(values[i]), result.laneIsSet(i));
        }
    }
}
