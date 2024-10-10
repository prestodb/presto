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
import static java.lang.annotation.RetentionPolicy.RUNTIME;

/**
 * By default, a function is just a “black box” that the database system knows very little about the behavior of.
 * However, that means that queries using the function may be executed much less efficiently than they could be.
 * It is possible to supply additional knowledge that helps the planner optimize function calls.
 * Scalar functions are straight forward to optimize and can have impact on the overall query performance.
 * Use this annotation to provide information regarding how this function impacts following query statistics.
 * <p>
 * A function may take one or more input column or a constant as parameters. Precise stats may depend on the input
 * parameters. This annotation does not cover all the possible cases and allows constant values for the following fields.
 * Value Double.NaN implies unknown.
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
     * A constant value for Distinct values count regardless of `input column`'s source stats.
     * e.g. a perfectly random generator may result in distinctValuesCount of `ScalarFunctionStatsUtils.ROW_COUNT`.
     */
    double distinctValuesCount() default Double.NaN;

    /**
     * A constant value for nullFraction, e.g. is_null(Slice) will alter column's null fraction
     * value to 0.0, regardless of input column's source stats.
     */
    double nullFraction() default Double.NaN;

    /**
     * A constant value for  `avgRowSize` e.g. a function like md5 may produce a
     * constant row size.
     */
    double avgRowSize() default Double.NaN;
}
