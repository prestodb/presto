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

import com.facebook.presto.geospatial.Rectangle;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class TestHilbertIndex
{
    @Test
    public void testOrder()
    {
        HilbertIndex hilbert = new HilbertIndex(new Rectangle(0, 0, 4, 4));
        long h0 = hilbert.indexOf(0., 0.);
        long h1 = hilbert.indexOf(1., 1.);
        long h2 = hilbert.indexOf(1., 3.);
        long h3 = hilbert.indexOf(3., 3.);
        long h4 = hilbert.indexOf(3., 1.);
        assertTrue(h0 < h1);
        assertTrue(h1 < h2);
        assertTrue(h2 < h3);
        assertTrue(h3 < h4);
    }

    @Test
    public void testOutOfBounds()
    {
        HilbertIndex hilbert = new HilbertIndex(new Rectangle(0, 0, 1, 1));
        assertEquals(hilbert.indexOf(2., 2.), Long.MAX_VALUE);
    }

    @Test
    public void testDegenerateRectangle()
    {
        HilbertIndex hilbert = new HilbertIndex(new Rectangle(0, 0, 0, 0));
        assertEquals(hilbert.indexOf(0., 0.), 0);
        assertEquals(hilbert.indexOf(2., 2.), Long.MAX_VALUE);
    }

    @Test
    public void testDegenerateHorizontalRectangle()
    {
        HilbertIndex hilbert = new HilbertIndex(new Rectangle(0, 0, 4, 0));
        assertEquals(hilbert.indexOf(0., 0.), 0);
        assertTrue(hilbert.indexOf(1., 0.) < hilbert.indexOf(2., 0.));
        assertEquals(hilbert.indexOf(0., 2.), Long.MAX_VALUE);
        assertEquals(hilbert.indexOf(2., 2.), Long.MAX_VALUE);
    }

    @Test
    public void testDegenerateVerticalRectangle()
    {
        HilbertIndex hilbert = new HilbertIndex(new Rectangle(0, 0, 0, 4));
        assertEquals(hilbert.indexOf(0., 0.), 0);
        assertTrue(hilbert.indexOf(0., 1.) < hilbert.indexOf(0., 2.));
        assertEquals(hilbert.indexOf(2., 0.), Long.MAX_VALUE);
        assertEquals(hilbert.indexOf(2., 2.), Long.MAX_VALUE);
    }
}
