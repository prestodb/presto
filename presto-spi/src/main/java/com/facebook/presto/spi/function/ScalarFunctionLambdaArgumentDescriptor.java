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
package com.facebook.presto.spi.function;

import java.lang.annotation.Retention;
import java.lang.annotation.Target;

import static java.lang.annotation.ElementType.METHOD;
import static java.lang.annotation.ElementType.TYPE;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

@Retention(RUNTIME)
@Target({METHOD, TYPE})
public @interface ScalarFunctionLambdaArgumentDescriptor
{
    /**
     * Index of the argument in the lambda argument list. Example: for the lambda (x, y) -> x + y, lambdaArgumentIndex for x and y are 0 and 1 respectively.
     */
    int lambdaArgumentIndex();

    /**
     * Index of the function argument that contains the function input (Array or Map), to which this lambda argument relate to.
     */
    int callArgumentIndex();

    /**
     * Contains the transformation function between the subfields of this lambda argument and the input of the function.
     *
     * The reason this transformation is needed because the input of the function is the Array or Map, while the lambda arguments are the element of the array or key/value of
     * the map. Specifically for map, the transformation function for the lambda argument of the map key will be different from the transformation of the map value.
     * If transformation succeeded, then the returned value contains the transformed set of lambda subfields. Otherwise, the function must return <code>Optional.empty()</code>
     * value.
     */
    StaticMethodPointer lambdaArgumentToInputTransformationFunction() default @StaticMethodPointer(
            clazz = ComplexTypeFunctionDescriptor.class, method = "prependAllSubscripts");
}
