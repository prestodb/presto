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
package com.facebook.presto.sql.gen;

import com.facebook.presto.block.BlockAssertions;
import com.facebook.presto.operator.PagePositionEqualitor;
import com.facebook.presto.operator.SimplePagePositionEqualitor;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.type.Type;
import com.google.common.collect.ImmutableList;
import com.google.common.primitives.Ints;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.List;

import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;

public class TestPageCompiler
{
    @Test
    public void testSingleChannel()
            throws Exception
    {
        List<Type> types = ImmutableList.<Type>of(VARCHAR);
        List<Integer> channels = Ints.asList(0);

        PagePositionEqualitor equalitor = PageCompiler.INSTANCE.compilePagePositionEqualitor(types, channels);
        SimplePagePositionEqualitor simpleEqualitor = new SimplePagePositionEqualitor(types, channels);

        String[] strings = {"a", "a", "b", "a", "c", "b"};
        Page page = new Page(BlockAssertions.createStringsBlock(strings));
        for (int i = 0; i < page.getPositionCount(); i++) {
            Assert.assertEquals(equalitor.hashRow(i, page), simpleEqualitor.hashRow(i, page));

            for (int j = 0; j < page.getPositionCount(); j++) {
                Assert.assertEquals(equalitor.rowEqualsRow(i, page, j, page), strings[i].equals(strings[j]));
                Assert.assertEquals(equalitor.rowEqualsRow(i, page, j, page), simpleEqualitor.rowEqualsRow(i, page, j, page));

                if (strings[i].equals(strings[j])) {
                    Assert.assertEquals(equalitor.hashRow(i, page), equalitor.hashRow(j, page));
                }
                else {
                    Assert.assertNotEquals(equalitor.hashRow(i, page), equalitor.hashRow(j, page));
                }
            }
        }
    }

    @Test
    public void testMultiChannelAndPages()
            throws Exception
    {
        List<Type> types = ImmutableList.<Type>of(VARCHAR, BIGINT);
        List<Integer> channels = Ints.asList(2, 0);

        PagePositionEqualitor equalitor = PageCompiler.INSTANCE.compilePagePositionEqualitor(types, channels);
        SimplePagePositionEqualitor simpleEqualitor = new SimplePagePositionEqualitor(types, channels);

        String[] strings1 = {"a", "a", "b", "a", "c", "b"};
        int[] ints1 = {1, 2, 0, 1, 2, 3};
        Page page1 = new Page(
                BlockAssertions.createLongsBlock(ints1),
                BlockAssertions.createBooleansBlock(true, strings1.length),
                BlockAssertions.createStringsBlock(strings1));

        String[] strings2 = {"a", "b", "c", "d", "e", "c"};
        int[] ints2 = {1, 3, 2, 1, 2, 2};
        Page page2 = new Page(
                BlockAssertions.createLongsBlock(ints2),
                BlockAssertions.createBooleansBlock(false, strings2.length),
                BlockAssertions.createStringsBlock(strings2));

        for (int i = 0; i < page1.getPositionCount(); i++) {
            for (int j = 0; j < page2.getPositionCount(); j++) {
                Assert.assertEquals(equalitor.rowEqualsRow(i, page1, j, page2), strings1[i].equals(strings2[j]) && ints1[i] == ints2[j]);
                Assert.assertEquals(equalitor.rowEqualsRow(i, page1, j, page2), simpleEqualitor.rowEqualsRow(i, page1, j, page2));

                if (strings1[i].equals(strings2[j]) && ints1[i] == ints2[j]) {
                    Assert.assertEquals(equalitor.hashRow(i, page1), equalitor.hashRow(j, page2));
                    Assert.assertEquals(equalitor.hashRow(i, page1), simpleEqualitor.hashRow(j, page2));
                }
                else {
                    Assert.assertNotEquals(equalitor.hashRow(i, page1), equalitor.hashRow(j, page2));
                    Assert.assertNotEquals(equalitor.hashRow(i, page1), simpleEqualitor.hashRow(j, page2));
                }
            }
        }
    }
}
