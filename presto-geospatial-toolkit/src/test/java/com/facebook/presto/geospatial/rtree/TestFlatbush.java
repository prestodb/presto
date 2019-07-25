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
package com.facebook.presto.geospatial.rtree;

import com.esri.core.geometry.Point;
import com.esri.core.geometry.ogc.OGCGeometry;
import com.esri.core.geometry.ogc.OGCPoint;
import com.facebook.presto.geospatial.GeometryUtils;
import com.facebook.presto.geospatial.Rectangle;
import com.google.common.collect.ImmutableList;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Random;

import static com.facebook.presto.geospatial.rtree.Flatbush.ENVELOPE_SIZE;
import static com.facebook.presto.geospatial.rtree.RtreeTestUtils.makeRectangles;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.lang.Double.NEGATIVE_INFINITY;
import static java.lang.Double.POSITIVE_INFINITY;
import static java.util.stream.Collectors.toList;
import static org.testng.Assert.assertEquals;

public class TestFlatbush
{
    private static final Rectangle EVERYTHING = new Rectangle(NEGATIVE_INFINITY, NEGATIVE_INFINITY, POSITIVE_INFINITY, POSITIVE_INFINITY);

    private static final Comparator<Rectangle> RECTANGLE_COMPARATOR = Comparator
            .comparing(Rectangle::getXMin)
            .thenComparing(Rectangle::getYMin)
            .thenComparing(Rectangle::getXMax)
            .thenComparing(Rectangle::getYMax);

    //  2 intersecting polygons: A and B
    private static final OGCGeometry POLYGON_A = OGCGeometry.fromText("POLYGON ((0 0, -0.5 2.5, 0 5, 2.5 5.5, 5 5, 5.5 2.5, 5 0, 2.5 -0.5, 0 0))");
    private static final OGCGeometry POLYGON_B = OGCGeometry.fromText("POLYGON ((4 4, 3.5 7, 4 10, 7 10.5, 10 10, 10.5 7, 10 4, 7 3.5, 4 4))");

    // A set of points: X in A, Y in A and B, Z in B, W outside of A and B
    private static final OGCGeometry POINT_X = new OGCPoint(new Point(1.0, 1.0), null);
    private static final OGCGeometry POINT_Y = new OGCPoint(new Point(4.5, 4.5), null);
    private static final OGCGeometry POINT_Z = new OGCPoint(new Point(6.0, 6.0), null);
    private static final OGCGeometry POINT_W = new OGCPoint(new Point(20.0, 20.0), null);

    @Test
    public void testEmptyFlatbush()
    {
        Flatbush<Rectangle> rtree = new Flatbush<>(new Rectangle[] {});
        assertEquals(findIntersections(rtree, EVERYTHING), ImmutableList.of());
    }

    @Test
    public void testSingletonFlatbush()
    {
        List<Rectangle> items = ImmutableList.of(new Rectangle(0, 0, 1, 1));
        Flatbush<Rectangle> rtree = new Flatbush<>(items.toArray(new Rectangle[] {}));

        assertEquals(findIntersections(rtree, EVERYTHING), items);
        // hit
        assertEquals(findIntersections(rtree, new Rectangle(1, 1, 2, 2)), items);
        // miss
        assertEquals(findIntersections(rtree, new Rectangle(-1, -1, -0.1, -0.1)), ImmutableList.of());
    }

    @Test
    public void testSingletonFlatbushXY()
    {
        // Because mixing up x and y is easy to do...
        List<Rectangle> items = ImmutableList.of(new Rectangle(0, 10, 1, 11));
        Flatbush<Rectangle> rtree = new Flatbush<>(items.toArray(new Rectangle[] {}));

        // hit
        assertEquals(findIntersections(rtree, new Rectangle(1, 11, 2, 12)), items);
        // miss
        assertEquals(findIntersections(rtree, new Rectangle(11, 1, 12, 2)), ImmutableList.of());
    }

    @Test
    public void testDoubletonFlatbush()
    {
        // This is the smallest Rtree with height > 1
        // Also test for some degeneracies
        Rectangle rect0 = new Rectangle(1, 1, 1, 1);
        Rectangle rect1 = new Rectangle(-1, -2, -1, -1);
        List<Rectangle> items = ImmutableList.of(rect0, rect1);

        Flatbush<Rectangle> rtree = new Flatbush<>(items.toArray(new Rectangle[] {}));

        List<Rectangle> allResults = findIntersections(rtree, EVERYTHING);
        assertEqualsSorted(allResults, items, RECTANGLE_COMPARATOR);

        assertEquals(findIntersections(rtree, new Rectangle(1, 1, 2, 2)), ImmutableList.of(rect0));
        assertEquals(findIntersections(rtree, new Rectangle(-2, -2, -1, -2)), ImmutableList.of(rect1));
        // This should test missing at the root level
        assertEquals(findIntersections(rtree, new Rectangle(10, 10, 12, 12)), ImmutableList.of());
        // This should test missing at the leaf level
        assertEquals(findIntersections(rtree, new Rectangle(0, 0, 0, 0)), ImmutableList.of());
    }

    @Test
    public void testTwoLevelFlatbush()
    {
        // This is the smallest Rtree with height > 2
        // Also test for NaN behavior
        Rectangle rect0 = new Rectangle(1, 1, 1, 1);
        Rectangle rect1 = new Rectangle(-1, -1, -1, -1);
        Rectangle rect2 = new Rectangle(1, -1, 1, -1);
        List<Rectangle> items = ImmutableList.of(rect0, rect1, rect2);

        Flatbush<Rectangle> rtree = new Flatbush<>(items.toArray(new Rectangle[] {}), 2);

        List<Rectangle> allResults = findIntersections(rtree, EVERYTHING);
        assertEqualsSorted(allResults, items, RECTANGLE_COMPARATOR);

        assertEquals(findIntersections(rtree, new Rectangle(1, 1, 1, 1)), ImmutableList.of(rect0));
        assertEquals(findIntersections(rtree, new Rectangle(-1, -1, -1, -1)), ImmutableList.of(rect1));
        assertEquals(findIntersections(rtree, new Rectangle(1, -1, 1, -1)), ImmutableList.of(rect2));
        // Test hitting across parent nodes
        List<Rectangle> results12 = findIntersections(rtree, new Rectangle(-1, -1, 1, -1));
        assertEqualsSorted(results12, ImmutableList.of(rect1, rect2), RECTANGLE_COMPARATOR);

        // This should test missing at the root level
        assertEquals(findIntersections(rtree, new Rectangle(10, 10, 12, 12)), ImmutableList.of());
        // This should test missing at the leaf level
        assertEquals(findIntersections(rtree, new Rectangle(0, 0, 0, 0)), ImmutableList.of());
    }

    @Test
    public void testOctagonQuery()
    {
        OGCGeometryWrapper octagonA = new OGCGeometryWrapper(POLYGON_A);
        OGCGeometryWrapper octagonB = new OGCGeometryWrapper(POLYGON_B);

        OGCGeometryWrapper pointX = new OGCGeometryWrapper(POINT_X);
        OGCGeometryWrapper pointY = new OGCGeometryWrapper(POINT_Y);
        OGCGeometryWrapper pointZ = new OGCGeometryWrapper(POINT_Z);
        OGCGeometryWrapper pointW = new OGCGeometryWrapper(POINT_W);

        Flatbush<OGCGeometryWrapper> rtree = new Flatbush<>(new OGCGeometryWrapper[] {pointX, pointY, pointZ, pointW});

        List<OGCGeometryWrapper> resultsA = findIntersections(rtree, octagonA.getExtent());
        assertEqualsSorted(resultsA, ImmutableList.of(pointX, pointY), Comparator.naturalOrder());

        List<OGCGeometryWrapper> resultsB = findIntersections(rtree, octagonB.getExtent());
        assertEqualsSorted(resultsB, ImmutableList.of(pointY, pointZ), Comparator.naturalOrder());
    }

    @Test
    public void testOctagonTree()
    {
        OGCGeometryWrapper octagonA = new OGCGeometryWrapper(POLYGON_A);
        OGCGeometryWrapper octagonB = new OGCGeometryWrapper(POLYGON_B);

        OGCGeometryWrapper pointX = new OGCGeometryWrapper(POINT_X);
        OGCGeometryWrapper pointY = new OGCGeometryWrapper(POINT_Y);
        OGCGeometryWrapper pointZ = new OGCGeometryWrapper(POINT_Z);
        OGCGeometryWrapper pointW = new OGCGeometryWrapper(POINT_W);

        Flatbush<OGCGeometryWrapper> rtree = new Flatbush<>(new OGCGeometryWrapper[] {octagonA, octagonB});

        assertEquals(findIntersections(rtree, pointX.getExtent()), ImmutableList.of(octagonA));

        List<OGCGeometryWrapper> results = findIntersections(rtree, pointY.getExtent());
        assertEqualsSorted(results, ImmutableList.of(octagonA, octagonB), Comparator.naturalOrder());

        assertEquals(findIntersections(rtree, pointZ.getExtent()), ImmutableList.of(octagonB));

        assertEquals(findIntersections(rtree, pointW.getExtent()), ImmutableList.of());
    }

    @DataProvider(name = "rectangle-counts")
    private Object[][] rectangleCounts()
    {
        return new Object[][] {{100, 1000, 42}, {1000, 10_000, 123}, {5000, 50_000, 321}};
    }

    @Test(dataProvider = "rectangle-counts")
    public void testRectangleCollection(int numBuildRectangles, int numProbeRectangles, int seed)
    {
        Random random = new Random(seed);
        List<Rectangle> buildRectangles = makeRectangles(random, numBuildRectangles);
        List<Rectangle> probeRectangles = makeRectangles(random, numProbeRectangles);

        Flatbush<Rectangle> rtree = new Flatbush<>(buildRectangles.toArray(new Rectangle[] {}));
        for (Rectangle query : probeRectangles) {
            List<Rectangle> actual = findIntersections(rtree, query);
            List<Rectangle> expected = buildRectangles.stream()
                    .filter(rect -> rect.intersects(query))
                    .collect(toList());
            assertEqualsSorted(actual, expected, RECTANGLE_COMPARATOR);
        }
    }

    @Test
    public void testChildrenOffsets()
    {
        int numRectangles = 10;
        int degree = 8;
        Random random = new Random(122);

        int firstParentIndex = 2 * degree * ENVELOPE_SIZE;
        int secondParentIndex = firstParentIndex + ENVELOPE_SIZE;
        int grandparentIndex = 3 * degree * ENVELOPE_SIZE;

        List<Rectangle> rectangles = makeRectangles(random, numRectangles);
        Flatbush<Rectangle> rtree = new Flatbush<>(rectangles.toArray(new Rectangle[] {}), degree);
        assertEquals(rtree.getHeight(), 3);
        assertEquals(rtree.getChildrenOffset(firstParentIndex, 1), 0);
        assertEquals(rtree.getChildrenOffset(secondParentIndex, 1), degree * ENVELOPE_SIZE);
        assertEquals(rtree.getChildrenOffset(grandparentIndex, 2), 2 * degree * ENVELOPE_SIZE);
    }

    private static <T extends HasExtent> List<T> findIntersections(Flatbush<T> rtree, Rectangle rectangle)
    {
        List<T> results = new ArrayList<>();
        rtree.findIntersections(rectangle, results::add);
        return results;
    }

    /*
     * Asserts the two lists of Rectangles are equal after sorting.
     */
    private static <T> void assertEqualsSorted(List<T> actual, List<T> expected, Comparator<T> comparator)
    {
        List<T> actualSorted = actual.stream().sorted(comparator).collect(toImmutableList());
        List<T> expectedSorted = expected.stream().sorted(comparator).collect(toImmutableList());

        assertEquals(actualSorted, expectedSorted);
    }

    private static final class OGCGeometryWrapper
            implements HasExtent, Comparable<OGCGeometryWrapper>
    {
        private final OGCGeometry geometry;
        private final Rectangle extent;

        public OGCGeometryWrapper(OGCGeometry geometry)
        {
            this.geometry = geometry;
            this.extent = GeometryUtils.getExtent(geometry);
        }

        public OGCGeometry getGeometry()
        {
            return geometry;
        }

        @Override
        public Rectangle getExtent()
        {
            return extent;
        }

        @Override
        public long getEstimatedSizeInBytes()
        {
            return geometry.estimateMemorySize();
        }

        @Override
        public int compareTo(OGCGeometryWrapper other)
        {
            return RECTANGLE_COMPARATOR.compare(this.getExtent(), other.getExtent());
        }
    }
}
