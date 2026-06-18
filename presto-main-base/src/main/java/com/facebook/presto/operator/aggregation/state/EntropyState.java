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

/**
 * This is a state class for calculating the log-2 entropy of counts.
 *
 *  Given counts $c_1, c_2, ..., cn$ ($c_i \geq 0)$, this aggregation calculates $\sum_i [ p_i log(-p_i) ]$,
 *      where $p_i = {c_i \over \sum_i [c_i]}$. Note that
 *      $$
 *      \sum_i [ p_i log( -p_i ) ] =
 *      \sum_i [ {c_i \over \sum_j [c_j]} \log( {\sum_j [c_j] \over c_i} ) ] =
 *      {
 *          \sum_i [ c_i \log( \sum_j [c_j] ) - c_i \log( c_i )]
 *          \over
 *          \sum_j [c_j]
 *      } =
 *      {
 *          \sum_i [ c_i ] \log( \sum_i [c_i] )  -
 *          \sum_i [ c_i \log( c_i ) ]
 *          \over
 *          \sum_i [c_i]
 *      } =
 *      \log( \sum_i [c_i] )  -
 *      { \sum_i [ c_i \log( c_i ) ] \over \sum_i [c_i] }
 *      .
 *      $$
 *      This shows that we just need to accumulate $\sum_i [c_i]$ and $\sum_i [ c_i \log( c_i ) ]$.
 *      In the following, they are termed sumC and sumCLogC, respectively (technically,
 *      the latter is $\sum_i [ c_i \ln( c_i ) ]$, and we change bases only at the end of the calculation).
 */
public interface EntropyState
        extends AccumulatorState
{
    // $\sum_i [c_i]$
    void setSumC(double sumC);

    // $\sum_i [c_i]$
    double getSumC();

    // $\sum_i [ c_i \log (c_i) ]$
    void setSumCLogC(double sumCLogC);

    // $\sum_i [ c_i \log (c_i) ]$
    double getSumCLogC();
}
