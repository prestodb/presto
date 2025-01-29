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

import static java.lang.annotation.ElementType.METHOD;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

/**
 * This annotation is used for specifying constant values for stats propagation for scalar functions.
 * It takes precedence over any per-argument {@link ScalarPropagateSourceStats}.
 * Use this annotation to provide information regarding how this function impacts following query statistics.
 * <p>
 * A function may take one or more input column or a constant as parameters. Precise stats may depend on the input
 * parameters. This annotation does not cover all the possible cases and allows constant values for the following fields.
 * Value Double.NaN implies unknown.
 * </p>
 * <p>
 * For example, `is_null(Slice)` can set null fraction value to a constant value of 0.0.
 * `md5(value)` can set a constant value for row size.
 * </p>
 */
@Retention(RUNTIME)
@Target(METHOD)
public @interface ScalarFunctionConstantStats
{
    // Min max value is Infinity if unknown.
    double minValue() default Double.NEGATIVE_INFINITY;

    double maxValue() default Double.POSITIVE_INFINITY;

    /**
     * A constant value for Distinct values count.
     */
    double distinctValuesCount() default Double.NaN;

    /**
     * A constant value for nullFraction.
     */
    double nullFraction() default Double.NaN;

    /**
     * A constant value for `avgRowSize`.
     */
    double avgRowSize() default Double.NaN;
}
