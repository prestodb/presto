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
     * Index of the function argument that contains the function input (Array or Map), to which this lambda argument relate to.
     */
    int callArgumentIndex();

    /**
     * Contains the name of the subfield path transformation function that needs to be applied ti the subfields of this lambda argument.
     * The acceptable list of names include all static methods of SubfieldPathTransformationFunctions class.
     */
    String lambdaArgumentToInputTransformationFunction() default "prependAllSubscripts";
}
