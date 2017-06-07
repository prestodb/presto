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
package com.facebook.presto.spi.resourceGroups;

import org.testng.annotations.Test;

import java.util.Optional;

import static com.facebook.presto.spi.resourceGroups.ResourceGroupId.fromSegmentedName;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.AssertJUnit.assertTrue;

public class TestResourceGroupId
{
    @Test
    public void testBasic()
    {
        new ResourceGroupId("test.test");
        new ResourceGroupId(new ResourceGroupId("test"), "test");
    }

    @Test
    public void testIsAncestor()
    {
        ResourceGroupId root = new ResourceGroupId("root");
        ResourceGroupId rootA = new ResourceGroupId(root, "a");
        ResourceGroupId rootAFoo = new ResourceGroupId(rootA, "foo");
        ResourceGroupId rootBar = new ResourceGroupId(root, "bar");
        assertTrue(root.isAncestorOf(rootA));
        assertTrue(root.isAncestorOf(rootAFoo));
        assertTrue(root.isAncestorOf(rootBar));
        assertTrue(rootA.isAncestorOf(rootAFoo));
        assertFalse(rootA.isAncestorOf(rootBar));
        assertFalse(rootAFoo.isAncestorOf(rootBar));
        assertFalse(rootBar.isAncestorOf(rootAFoo));
        assertFalse(rootAFoo.isAncestorOf(root));
        assertFalse(root.isAncestorOf(root));
        assertFalse(rootAFoo.isAncestorOf(rootAFoo));
    }

    @Test
    public void testCreateFromSegmentedName()
    {
        ResourceGroupId rootAFoo = fromSegmentedName("root.a.foo");
        ResourceGroupId rootA = fromSegmentedName("root.a");
        ResourceGroupId root = fromSegmentedName("root");
        assertEquals(rootAFoo.getParent(), Optional.of(rootA));
        assertEquals(rootA.getParent(), Optional.of(root));
        assertTrue(root.isAncestorOf(rootAFoo));
    }
}
