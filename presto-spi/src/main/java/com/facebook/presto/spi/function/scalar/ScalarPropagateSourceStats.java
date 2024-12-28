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
package com.facebook.presto.spi.function.scalar;

import java.lang.annotation.Retention;
import java.lang.annotation.Target;

import static java.lang.annotation.ElementType.PARAMETER;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

@Retention(RUNTIME)
@Target(PARAMETER)
public @interface ScalarPropagateSourceStats
{
    boolean propagateAllStats() default true;

    ScalarFunctionStatsPropagationBehavior minValue() default ScalarFunctionStatsPropagationBehavior.UNKNOWN;
    ScalarFunctionStatsPropagationBehavior maxValue() default ScalarFunctionStatsPropagationBehavior.UNKNOWN;
    ScalarFunctionStatsPropagationBehavior distinctValuesCount() default ScalarFunctionStatsPropagationBehavior.UNKNOWN;
    ScalarFunctionStatsPropagationBehavior avgRowSize() default ScalarFunctionStatsPropagationBehavior.UNKNOWN;
    ScalarFunctionStatsPropagationBehavior nullFraction() default ScalarFunctionStatsPropagationBehavior.UNKNOWN;
}
