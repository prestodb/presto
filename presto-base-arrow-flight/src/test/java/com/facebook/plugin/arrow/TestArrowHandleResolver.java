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
package com.facebook.plugin.arrow;

import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;

public class TestArrowHandleResolver
{
    @Test
    public void testGetTableHandleClass()
    {
        ArrowHandleResolver resolver = new ArrowHandleResolver();
        assertEquals(
                resolver.getTableHandleClass(),
                ArrowTableHandle.class,
                "getTableHandleClass should return ArrowTableHandle class.");
    }
    @Test
    public void testGetTableLayoutHandleClass()
    {
        ArrowHandleResolver resolver = new ArrowHandleResolver();
        assertEquals(
                resolver.getTableLayoutHandleClass(),
                ArrowTableLayoutHandle.class,
                "getTableLayoutHandleClass should return ArrowTableLayoutHandle class.");
    }
    @Test
    public void testGetColumnHandleClass()
    {
        ArrowHandleResolver resolver = new ArrowHandleResolver();
        assertEquals(
                resolver.getColumnHandleClass(),
                ArrowColumnHandle.class,
                "getColumnHandleClass should return ArrowColumnHandle class.");
    }
    @Test
    public void testGetSplitClass()
    {
        ArrowHandleResolver resolver = new ArrowHandleResolver();
        assertEquals(
                resolver.getSplitClass(),
                ArrowSplit.class,
                "getSplitClass should return ArrowSplit class.");
    }
    @Test
    public void testGetTransactionHandleClass()
    {
        ArrowHandleResolver resolver = new ArrowHandleResolver();
        assertEquals(
                resolver.getTransactionHandleClass(),
                ArrowTransactionHandle.class,
                "getTransactionHandleClass should return ArrowTransactionHandle class.");
    }
}
