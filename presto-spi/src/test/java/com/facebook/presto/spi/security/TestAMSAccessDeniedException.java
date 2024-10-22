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
package com.facebook.presto.spi.security;

import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.Optional;

import static com.facebook.presto.spi.security.AMSAccessDeniedException.allowSelectColumns;
import static com.facebook.presto.spi.security.AMSAccessDeniedException.denySelectColumns;
import static org.testng.Assert.assertThrows;

public class TestAMSAccessDeniedException
{
    private static final String TEST_TABLE = "test_table";
    private static final String TEST_COLUMN = "test_column";
    private static final String TEST_COLUMN2 = "test_column2";
    private static final Optional<String> EXTRA_INFO = Optional.of("extra_info");
    private static final Optional<String> EMPTY_EXTRA_INFO = Optional.empty();

    @Test
    public void testDenySelectColumnsWithoutExtraInfo()
    {
        assertThrows(AMSAccessDeniedException.class, () -> denySelectColumns(TEST_TABLE));
    }

    @Test
    public void testDenySelectColumnsWithExtraInfo()
    {
        assertThrows(AMSAccessDeniedException.class, () -> denySelectColumns(TEST_TABLE, EXTRA_INFO));
    }

    @Test
    public void testDenySelectColumnsWithEmptyExtraInfo()
    {
        assertThrows(AMSAccessDeniedException.class, () -> denySelectColumns(TEST_TABLE, EMPTY_EXTRA_INFO));
    }

    @Test
    public void testAllowSelectColumnsWithoutExtraInfo()
    {
        assertThrows(AMSAccessDeniedException.class, () -> allowSelectColumns(TEST_TABLE, Collections.singletonList(TEST_COLUMN)));
    }

    @Test
    public void testAllowSelectColumnsWithExtraInfo()
    {
        assertThrows(AMSAccessDeniedException.class, () -> allowSelectColumns(TEST_TABLE, Arrays.asList(TEST_COLUMN, TEST_COLUMN2), EXTRA_INFO));
    }

    @Test
    public void testAllowSelectColumnsWithEmptyExtraInfo()
    {
        assertThrows(AMSAccessDeniedException.class, () -> allowSelectColumns(TEST_TABLE, Arrays.asList(TEST_COLUMN, TEST_COLUMN2), EMPTY_EXTRA_INFO));
    }

    @Test
    public void testAllowSelectColumnsWithNullColumnNames()
    {
        assertThrows(AMSAccessDeniedException.class, () -> allowSelectColumns(TEST_TABLE, null));
    }

    @Test
    public void testAllowSelectColumnsWithEmptyColumnNames()
    {
        assertThrows(AMSAccessDeniedException.class, () -> allowSelectColumns(TEST_TABLE, Collections.emptyList()));
    }
}
