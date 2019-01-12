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
package io.prestosql.operator;

import io.prestosql.execution.Lifespan;

public interface OperatorFactory
{
    Operator createOperator(DriverContext driverContext);

    /**
     * Declare that createOperator will not be called any more and release
     * any resources associated with this factory.
     * <p>
     * This method will be called only once.
     * Implementation doesn't need to worry about duplicate invocations.
     * <p>
     * It is guaranteed that this will only be invoked after {@link #noMoreOperators(Lifespan)}
     * has been invoked for all applicable driver groups.
     */
    void noMoreOperators();

    /**
     * Declare that createOperator will not be called any more for the specified Lifespan,
     * and release any resources associated with this factory.
     * <p>
     * This method will be called only once for each Lifespan.
     * Implementation doesn't need to worry about duplicate invocations.
     * <p>
     * It is guaranteed that this method will be invoked for all applicable driver groups
     * before {@link #noMoreOperators()} is invoked.
     */
    default void noMoreOperators(Lifespan lifespan)
    {
        // do nothing
    }

    OperatorFactory duplicate();
}
