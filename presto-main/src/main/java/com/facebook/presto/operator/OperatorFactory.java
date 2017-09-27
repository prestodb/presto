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
package com.facebook.presto.operator;

import com.facebook.presto.spi.type.Type;

import java.util.List;

public interface OperatorFactory
{
    List<Type> getTypes();

    Operator createOperator(DriverContext driverContext);

    /**
     * Declare that createOperator will not be called any more and release
     * any resources associated with this factory.
     * <p>
     * This method will be called only once.
     * Implementation doesn't need to worry about duplicate invocations.
     */
    void noMoreOperators();

    OperatorFactory duplicate();
}
