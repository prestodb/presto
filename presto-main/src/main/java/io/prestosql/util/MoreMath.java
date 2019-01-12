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
package io.prestosql.util;

import java.util.stream.DoubleStream;

import static java.lang.Double.isNaN;

public final class MoreMath
{
    private MoreMath() {}

    /**
     * See http://floating-point-gui.de/errors/comparison/
     */
    public static boolean nearlyEqual(double a, double b, double epsilon)
    {
        double absA = Math.abs(a);
        double absB = Math.abs(b);
        double diff = Math.abs(a - b);

        if (a == b) { // shortcut, handles infinities
            return true;
        }
        else if (a == 0 || b == 0 || diff < Double.MIN_NORMAL) {
            // a or b is zero or both are extremely close to it
            // relative error is less meaningful here
            return diff < (epsilon * Double.MIN_NORMAL);
        }
        else { // use relative error
            return diff / Math.min((absA + absB), Double.MAX_VALUE) < epsilon;
        }
    }

    /**
     * See http://floating-point-gui.de/errors/comparison/
     */
    public static boolean nearlyEqual(float a, float b, float epsilon)
    {
        float absA = Math.abs(a);
        float absB = Math.abs(b);
        float diff = Math.abs(a - b);

        if (a == b) { // shortcut, handles infinities
            return true;
        }
        else if (a == 0 || b == 0 || diff < Float.MIN_NORMAL) {
            // a or b is zero or both are extremely close to it
            // relative error is less meaningful here
            return diff < (epsilon * Float.MIN_NORMAL);
        }
        else { // use relative error
            return diff / Math.min((absA + absB), Float.MAX_VALUE) < epsilon;
        }
    }

    public static double min(double... values)
    {
        return DoubleStream.of(values)
                .min()
                .getAsDouble();
    }

    public static double max(double... values)
    {
        return DoubleStream.of(values)
                .max()
                .getAsDouble();
    }

    public static double rangeMin(double left, double right)
    {
        if (isNaN(left)) {
            return right;
        }
        if (isNaN(right)) {
            return left;
        }
        return min(left, right);
    }

    public static double rangeMax(double left, double right)
    {
        if (isNaN(left)) {
            return right;
        }
        if (isNaN(right)) {
            return left;
        }
        return max(left, right);
    }

    public static double firstNonNaN(double... values)
    {
        for (double value : values) {
            if (!isNaN(value)) {
                return value;
            }
        }
        throw new IllegalArgumentException("All values are NaN");
    }
}
