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
package com.facebook.presto.geospatial;

import com.esri.core.geometry.Envelope2D;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

public class KDBTreeSpec
{
    public static final class KDBTreeNode
    {
        private final Double x;
        private final Double y;
        private final KDBTreeNode[] children;
        private final Integer leafId;

        @JsonCreator
        public KDBTreeNode(@JsonProperty("x") Double x,
                @JsonProperty("y") Double y,
                @JsonProperty("children") KDBTreeNode[] children,
                @JsonProperty("leafId") Integer leafId)
        {
            if (leafId == null) {
                checkState(x == null || y == null);
                checkState(x != null || y != null);
            }
            else {
                checkState(x == null && y == null);
            }

            this.x = x;
            this.y = y;
            this.children = children;
            this.leafId = leafId;
        }

        public KDBTreeNode(int leafId)
        {
            this.x = null;
            this.y = null;
            this.children = null;
            this.leafId = leafId;
        }

        @JsonProperty("x")
        public Double getX()
        {
            return x;
        }

        @JsonProperty("y")
        public Double getY()
        {
            return y;
        }

        @JsonProperty("children")
        public KDBTreeNode[] getChildren()
        {
            return children;
        }

        @JsonProperty("leafId")
        public Integer getLeafId()
        {
            return leafId;
        }
    }

    private final Envelope2D extent;
    private final KDBTreeNode rootNode;

    @JsonCreator
    public KDBTreeSpec(@JsonProperty("xMin") double xMin,
            @JsonProperty("yMin") double yMin,
            @JsonProperty("xMax") double xMax,
            @JsonProperty("yMax") double yMax,
            @JsonProperty("root") KDBTreeNode rootNode)
    {
        requireNonNull(rootNode, "rootNode is null");
        this.extent = new Envelope2D(xMin, yMin, xMax, yMax);
        this.rootNode = rootNode;
    }

    @JsonProperty("xMin")
    public double getXMin()
    {
        return extent.xmin;
    }

    @JsonProperty("yMin")
    public double getYMin()
    {
        return extent.ymin;
    }

    @JsonProperty("xMax")
    public double getXMax()
    {
        return extent.xmax;
    }

    @JsonProperty("yMax")
    public double getYMax()
    {
        return extent.ymax;
    }

    @JsonProperty("root")
    public KDBTreeNode getRootNode()
    {
        return rootNode;
    }

    public Envelope2D getExtent()
    {
        return extent;
    }
}
