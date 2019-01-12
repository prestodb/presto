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
package io.prestosql.geospatial;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.openjdk.jol.info.ClassLayout;

import java.util.Objects;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.Math.max;
import static java.lang.Math.min;
import static java.util.Objects.requireNonNull;

public final class Rectangle
{
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(Rectangle.class).instanceSize();

    private final double xMin;
    private final double yMin;
    private final double xMax;
    private final double yMax;

    @JsonCreator
    public Rectangle(
            @JsonProperty("xmin") double xMin,
            @JsonProperty("ymin") double yMin,
            @JsonProperty("xmax") double xMax,
            @JsonProperty("ymax") double yMax)
    {
        checkArgument(xMin <= xMax, "xMin is greater than xMax");
        checkArgument(yMin <= yMax, "yMin is greater than yMax");
        this.xMin = xMin;
        this.yMin = yMin;
        this.xMax = xMax;
        this.yMax = yMax;
    }

    @JsonProperty
    public double getXMin()
    {
        return xMin;
    }

    @JsonProperty
    public double getYMin()
    {
        return yMin;
    }

    @JsonProperty
    public double getXMax()
    {
        return xMax;
    }

    @JsonProperty
    public double getYMax()
    {
        return yMax;
    }

    public double getWidth()
    {
        return xMax - xMin;
    }

    public double getHeight()
    {
        return yMax - yMin;
    }

    public boolean intersects(Rectangle other)
    {
        requireNonNull(other, "other is null");
        return this.xMin <= other.xMax && this.xMax >= other.xMin && this.yMin <= other.yMax && this.yMax >= other.yMin;
    }

    public Rectangle intersection(Rectangle other)
    {
        requireNonNull(other, "other is null");
        if (!intersects(other)) {
            return null;
        }

        return new Rectangle(max(this.xMin, other.xMin), max(this.yMin, other.yMin), min(this.xMax, other.xMax), min(this.yMax, other.yMax));
    }

    public Rectangle merge(Rectangle other)
    {
        return new Rectangle(min(this.xMin, other.xMin), min(this.yMin, other.yMin), max(this.xMax, other.xMax), max(this.yMax, other.yMax));
    }

    public int estimateMemorySize()
    {
        return INSTANCE_SIZE;
    }

    @Override
    public boolean equals(Object obj)
    {
        if (obj == null) {
            return false;
        }

        if (!(obj instanceof Rectangle)) {
            return false;
        }

        Rectangle other = (Rectangle) obj;
        return other.xMin == this.xMin && other.yMin == this.yMin && other.xMax == this.xMax && other.yMax == this.yMax;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(xMin, yMin, xMax, yMax);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("xMin", xMin)
                .add("yMin", yMin)
                .add("xMax", xMax)
                .add("yMax", yMax)
                .toString();
    }
}
