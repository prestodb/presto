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
package io.prestosql.cost;

import io.prestosql.util.MoreMath;

import static java.lang.Double.isNaN;
import static java.lang.String.format;

public final class EstimateAssertion
{
    private EstimateAssertion() {}

    private static final double TOLERANCE = 0.0000001;

    public static void assertEstimateEquals(double actual, double expected, String messageFormat, Object... messageObjects)
    {
        if (isNaN(actual) && isNaN(expected)) {
            return;
        }

        if (!MoreMath.nearlyEqual(actual, expected, TOLERANCE)) {
            throw new AssertionError(format(messageFormat, messageObjects) + format(", expected [%f], but got [%f]", expected, actual));
        }
    }
}
