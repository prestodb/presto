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
package com.facebook.presto.operator.scalar;

import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import org.testng.annotations.Test;

import static com.facebook.presto.operator.scalar.ColorFunctions.color;
import static com.facebook.presto.operator.scalar.ColorFunctions.getBlue;
import static com.facebook.presto.operator.scalar.ColorFunctions.getGreen;
import static com.facebook.presto.operator.scalar.ColorFunctions.getRed;
import static com.facebook.presto.operator.scalar.ColorFunctions.parseRgb;
import static com.facebook.presto.operator.scalar.ColorFunctions.render;
import static com.facebook.presto.operator.scalar.ColorFunctions.rgb;
import static com.google.common.base.Charsets.UTF_8;
import static org.testng.Assert.assertEquals;

public class TestColorFunctions
{
    @Test
    public void testParseRgb()
            throws Exception
    {
        assertEquals(parseRgb(toSlice("#000")), 0x00_00_00);
        assertEquals(parseRgb(toSlice("#FFF")), 0xFF_FF_FF);
        assertEquals(parseRgb(toSlice("#F00")), 0xFF_00_00);
        assertEquals(parseRgb(toSlice("#0F0")), 0x00_FF_00);
        assertEquals(parseRgb(toSlice("#00F")), 0x00_00_FF);
        assertEquals(parseRgb(toSlice("#700")), 0x77_00_00);
        assertEquals(parseRgb(toSlice("#070")), 0x00_77_00);
        assertEquals(parseRgb(toSlice("#007")), 0x00_00_77);

        assertEquals(parseRgb(toSlice("#cde")), 0xCC_DD_EE);
    }

    @Test
    public void testGetComponent()
            throws Exception
    {
        assertEquals(getRed(parseRgb(toSlice("#789"))), 0x77);
        assertEquals(getGreen(parseRgb(toSlice("#789"))), 0x88);
        assertEquals(getBlue(parseRgb(toSlice("#789"))), 0x99);
    }

    @Test
    public void testToRgb()
            throws Exception
    {
        assertEquals(rgb(0xFF, 0, 0), 0xFF_00_00);
        assertEquals(rgb(0, 0xFF, 0), 0x00_FF_00);
        assertEquals(rgb(0, 0, 0xFF), 0x00_00_FF);
    }

    @Test
    public void testColor()
            throws Exception
    {
        assertEquals(color(toSlice("black")), -1);
        assertEquals(color(toSlice("red")), -2);
        assertEquals(color(toSlice("green")), -3);
        assertEquals(color(toSlice("yellow")), -4);
        assertEquals(color(toSlice("blue")), -5);
        assertEquals(color(toSlice("magenta")), -6);
        assertEquals(color(toSlice("cyan")), -7);
        assertEquals(color(toSlice("white")), -8);

        assertEquals(color(toSlice("#f00")), 0xFF_00_00);
        assertEquals(color(toSlice("#0f0")), 0x00_FF_00);
        assertEquals(color(toSlice("#00f")), 0x00_00_FF);
    }

    @Test
    public void testRenderString()
            throws Exception
    {
        assertEquals(render(toSlice("hello"), color(toSlice("red"))), toSlice("\u001b[38;5;1mhello\u001b[0m"));

        assertEquals(render(toSlice("hello"), color(toSlice("#f00"))), toSlice("\u001b[38;5;196mhello\u001b[0m"));
        assertEquals(render(toSlice("hello"), color(toSlice("#0f0"))), toSlice("\u001b[38;5;46mhello\u001b[0m"));
        assertEquals(render(toSlice("hello"), color(toSlice("#00f"))), toSlice("\u001b[38;5;21mhello\u001b[0m"));
    }

    @Test
    public void testRenderLong()
            throws Exception
    {
        assertEquals(render(1234, color(toSlice("red"))), toSlice("\u001b[38;5;1m1234\u001b[0m"));

        assertEquals(render(1234, color(toSlice("#f00"))), toSlice("\u001b[38;5;196m1234\u001b[0m"));
        assertEquals(render(1234, color(toSlice("#0f0"))), toSlice("\u001b[38;5;46m1234\u001b[0m"));
        assertEquals(render(1234, color(toSlice("#00f"))), toSlice("\u001b[38;5;21m1234\u001b[0m"));
    }

    @Test
    public void testRenderDouble()
            throws Exception
    {
        assertEquals(render(1234.5678, color(toSlice("red"))), toSlice("\u001b[38;5;1m1234.5678\u001b[0m"));

        assertEquals(render(1234.5678, color(toSlice("#f00"))), toSlice("\u001b[38;5;196m1234.5678\u001b[0m"));
        assertEquals(render(1234.5678, color(toSlice("#0f0"))), toSlice("\u001b[38;5;46m1234.5678\u001b[0m"));
        assertEquals(render(1234.5678, color(toSlice("#00f"))), toSlice("\u001b[38;5;21m1234.5678\u001b[0m"));
    }

    @Test
    public void testInterpolate()
            throws Exception
    {
        assertEquals(color(0, 0, 255, toSlice("#000"), toSlice("#fff")), 0x00_00_00);
        assertEquals(color(128, 0, 255, toSlice("#000"), toSlice("#fff")), 0x80_80_80);
        assertEquals(color(255, 0, 255, toSlice("#000"), toSlice("#fff")), 0xFF_FF_FF);

        assertEquals(color(0.0, toSlice("#000"), toSlice("#fff")), 0x00_00_00);
        assertEquals(color(0.5, toSlice("#000"), toSlice("#fff")), 0x80_80_80);
        assertEquals(color(1.0, toSlice("#000"), toSlice("#fff")), 0xFF_FF_FF);
    }

    private static Slice toSlice(String string)
    {
        return Slices.copiedBuffer(string, UTF_8);
    }
}
