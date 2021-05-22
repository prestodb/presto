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
package com.facebook.presto.tablestore;

import com.alicloud.openservices.tablestore.model.TimeRange;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.testing.TestingConnectorSession;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static com.facebook.presto.tablestore.TablestoreConnectorId.TABLESTORE_CONNECTOR;
import static com.facebook.presto.tablestore.TablestoreConstants.session;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

public class TestTablestoreSessionProperties
        extends TablestoreSessionProperties
{
    @Test
    public void testHints()
    {
        AtomicInteger ac = new AtomicInteger();
        Set<String> sets = Arrays.stream(TablestoreSessionProperties.class.getDeclaredFields())
                .filter(field -> field.getName().startsWith("HINT_"))
                .map(f -> {
                    ac.incrementAndGet();
                    f.setAccessible(true);
                    try {
                        String x = (String) f.get(null);
                        assertTrue(x.startsWith(TABLESTORE_CONNECTOR + "-"));
                        assertEquals(x.length(), x.trim().length());
                        return x;
                    }
                    catch (IllegalAccessException e) {
                        throw new RuntimeException(e);
                    }
                }).collect(Collectors.toSet());
        assertEquals(ac.get(), sets.size());
    }

    @Test
    public void testEnableRangeBlockQuery()
    {
        String hint = HINT_QUERY_VERSION;

        ConnectorSession cs = session();
        int x = getQueryVersion(cs);
        assertEquals(2, x);

        cs = session(hint, 1);
        x = getQueryVersion(cs);
        assertEquals(1, x);

        cs = session(hint, 2);
        x = getQueryVersion(cs);
        assertEquals(2, x);

        cs = session(hint, "TrUexx");
        try {
            getQueryVersion(cs);
            fail();
        }
        catch (Exception e) {
            e.printStackTrace();
            assertEquals("java.lang.String cannot be cast to java.lang.Number", e.getMessage());
        }
    }

    @Test
    public void testEnableSplitOptimization()
    {
        String hint = HINT_SPLIT_OPTIMIZE;

        TestingConnectorSession cs = session();
        boolean x = enableSplitOptimization(cs);
        assertFalse(x);

        cs = session(hint, false);
        x = enableSplitOptimization(cs);
        assertFalse(x);

        cs = session(hint, true);
        x = enableSplitOptimization(cs);
        assertTrue(x);

        cs = session(hint, "TrUexx");
        try {
            enableSplitOptimization(cs);
            fail();
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testEnablePartitionPruning()
    {
        String hint = HINT_PARTITION_PRUNE;

        TestingConnectorSession cs = session();
        boolean x = enablePartitionPruning(cs);
        assertTrue(x);

        cs = session(hint, false);
        x = enablePartitionPruning(cs);
        assertFalse(x);

        cs = session(hint, true);
        x = enablePartitionPruning(cs);
        assertTrue(x);
    }

    @SuppressWarnings("ConstantConditions")
    @Test
    public void testGetSplitUnitBytes()
    {
        long min = MIN_SPLIT_UNIT_MB;
        long max = MAX_SPLIT_UNIT_MB;
        long def = DEFAULT_SPLIT_UNIT_MB;
        String hint = HINT_SPLIT_UNIT_MB;
        assertTrue(hint.length() > 0);
        assertTrue(def > 0);
        assertTrue(min > 0);
        assertTrue(max >= def && def >= min);

        TestingConnectorSession cs = session();
        long x = getSplitUnitBytes(cs);
        assertEquals(def * 1024 * 1024, x);
        assertTrue(x >= 10 * 1024 * 1024);

        cs = session(hint, min);
        x = getSplitUnitBytes(cs);
        assertEquals(min * 1024 * 1024, x);

        cs = session(hint, max);
        x = getSplitUnitBytes(cs);
        assertEquals(max * 1024 * 1024, x);

        cs = session(hint, min - 1);
        try {
            getSplitUnitBytes(cs);
            fail();
        }
        catch (Exception e) {
            assertEquals("Invalid value '0' for hint 'tablestore-split-unit-mb', TYPE=long and DEFAULT=10 and MIN=1 and MAX=102400", e.getMessage());
        }

        cs = session(hint, max + 1);
        try {
            getSplitUnitBytes(cs);
            fail();
        }
        catch (Exception e) {
            assertEquals("Invalid value '102401' for hint 'tablestore-split-unit-mb', TYPE=long and DEFAULT=10 and MIN=1 and MAX=102400", e.getMessage());
        }

        cs = session(hint, "xxx");
        try {
            getSplitUnitBytes(cs);
            fail();
        }
        catch (Exception e) {
            assertEquals(e.getMessage(), "java.lang.String cannot be cast to java.lang.Number");
        }
    }

    @SuppressWarnings("ConstantConditions")
    @Test
    public void testGetSplitSizeRatio()
    {
        double min = MIN_SPLIT_SIZE_RATIO;
        double max = MAX_SPLIT_SIZE_RATIO;
        double def = DEFAULT_SPLIT_SIZE_RATIO;
        String hint = HINT_SPLIT_SIZE_RATIO;
        assertTrue(hint.length() > 0);
        assertTrue(def > 0);
        assertTrue(min > 0);
        assertTrue(max >= def && def >= min);

        TestingConnectorSession cs = session();
        double x = getSplitSizeRatio(cs);
        assertEquals(def, x, 0.0000005);
        assertEquals(0.5, x, 0.0000005);

        cs = session(hint, min);
        x = getSplitSizeRatio(cs);
        assertEquals(min, x, 0.0000005);

        cs = session(hint, max);
        x = getSplitSizeRatio(cs);
        assertEquals(max, x, 0.0000005);

        cs = session(hint, 0);
        try {
            getSplitSizeRatio(cs);
            fail();
        }
        catch (Exception e) {
            assertEquals("Invalid value '0.0000' for hint 'tablestore-split-size-ratio', TYPE=double and DEFAULT=0.5000 and MIN=0.0001 and MAX=1.0000", e.getMessage());
        }

        cs = session(hint, max + 1);
        try {
            getSplitSizeRatio(cs);
            fail();
        }
        catch (Exception e) {
            assertEquals("Invalid value '2.0000' for hint 'tablestore-split-size-ratio', TYPE=double and DEFAULT=0.5000 and MIN=0.0001 and MAX=1.0000", e.getMessage());
        }

        cs = session(hint, "xxx");
        try {
            getSplitSizeRatio(cs);
            fail();
        }
        catch (Exception e) {
            assertEquals(e.getMessage(), "java.lang.String cannot be cast to java.lang.Number");
        }
    }

    @SuppressWarnings("ConstantConditions")
    @Test
    public void testGetFetchSize()
    {
        long min = MIN_FETCH_SIZE;
        long max = MAX_FETCH_SIZE;
        long def = DEFAULT_FETCH_SIZE;
        String hint = HINT_FETCH_SIZE;

        assertTrue(hint.length() > 0);
        assertTrue(def > 0);
        assertTrue(min > 0);
        assertTrue(max >= def && def >= min);

        TestingConnectorSession cs = session();
        int x = getFetchSize(cs);
        assertEquals(def, x);

        cs = session(hint, min);
        x = getFetchSize(cs);
        assertEquals(min, x);

        cs = session(hint, max);
        x = getFetchSize(cs);
        assertEquals(max, x);

        cs = session(hint, min - 1);
        try {
            getFetchSize(cs);
            fail();
        }
        catch (Exception e) {
            assertEquals("Invalid value '999' for hint 'tablestore-fetch-size', TYPE=int and DEFAULT=10000 and MIN=1000 and MAX=100000", e.getMessage());
        }

        cs = session(hint, max + 1);
        try {
            getFetchSize(cs);
            fail();
        }
        catch (Exception e) {
            assertEquals("Invalid value '100001' for hint 'tablestore-fetch-size', TYPE=int and DEFAULT=10000 and MIN=1000 and MAX=100000", e.getMessage());
        }

        cs = session(hint, "xxx");
        try {
            getFetchSize(cs);
            fail();
        }
        catch (Exception e) {
            assertEquals(e.getMessage(), "java.lang.String cannot be cast to java.lang.Number");
        }
    }

    @Test
    public void testGetTimeRange()
    {
        String start = HINT_START_VERSION;
        String end = HINT_END_VERSION;

        TestingConnectorSession cs = session();
        Optional<TimeRange> x = getTimeRange(cs);
        assertFalse(x.isPresent());

        cs = session(start, 0);
        x = getTimeRange(cs);
        assertTrue(x.isPresent());
        assertEquals(0L, x.get().getStart());

        cs = session(start, 0, end, 1);
        x = getTimeRange(cs);
        assertTrue(x.isPresent());
        assertEquals(1L, x.get().getEnd());

        cs = session(start, 0, end, 0);
        try {
            getTimeRange(cs);
            fail();
        }
        catch (Exception e) {
            assertEquals("end is smaller than start", e.getMessage());
        }

        cs = session(end, 0);
        try {
            getTimeRange(cs);
            fail();
        }
        catch (Exception e) {
            assertEquals("You can't only specify end version but no start version, it's invalid.", e.getMessage());
        }

        cs = session(end, -1);
        x = getTimeRange(cs);
        assertFalse(x.isPresent());

        cs = session(start, -1);
        x = getTimeRange(cs);
        assertFalse(x.isPresent());

        cs = session(start, "xxx");
        try {
            getTimeRange(cs);
            fail();
        }
        catch (Exception e) {
            assertEquals(e.getMessage(), "java.lang.String cannot be cast to java.lang.Number");
        }
    }

    @Test
    public void testEnableParallelScanMode()
    {
        TestingConnectorSession session = session();
        assertFalse(isParallelScanMode(session));

        session = session(HINT_INDEX_PARALLEL_SCAN_MODE, false);
        assertFalse(isParallelScanMode(session));

        session = session(HINT_INDEX_PARALLEL_SCAN_MODE, true);
        assertTrue(isParallelScanMode(session));

        session = session(HINT_INDEX_PARALLEL_SCAN_MODE, "xxx");
        try {
            isParallelScanMode(session);
        }
        catch (Exception e) {
            assertEquals(e.getMessage(), "Cannot cast java.lang.String to java.lang.Boolean");
        }
    }
}
