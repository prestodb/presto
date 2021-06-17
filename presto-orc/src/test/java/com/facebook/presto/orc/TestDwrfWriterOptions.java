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
package com.facebook.presto.orc;

import com.facebook.presto.orc.metadata.DwrfStripeCacheMode;
import io.airlift.units.DataSize;
import org.testng.annotations.Test;

import static com.facebook.presto.orc.metadata.DwrfStripeCacheMode.INDEX_AND_FOOTER;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class TestDwrfWriterOptions
{
    private static final DwrfStripeCacheMode STRIPE_CACHE_MODE = INDEX_AND_FOOTER;
    private static final DataSize STRIPE_CACHE_MAX_SIZE = new DataSize(27, MEGABYTE);

    @Test
    public void testProperties()
    {
        DwrfWriterOptions options = DwrfWriterOptions.builder()
                .withStripeCacheEnabled(true)
                .withStripeCacheMode(STRIPE_CACHE_MODE)
                .withStripeCacheMaxSize(STRIPE_CACHE_MAX_SIZE)
                .build();

        assertTrue(options.isStripeCacheEnabled());
        assertEquals(options.getStripeCacheMode(), STRIPE_CACHE_MODE);
        assertEquals(options.getStripeCacheMaxSize(), STRIPE_CACHE_MAX_SIZE);
    }

    @Test
    public void testToString()
    {
        DwrfWriterOptions options = DwrfWriterOptions.builder()
                .withStripeCacheEnabled(true)
                .withStripeCacheMode(STRIPE_CACHE_MODE)
                .withStripeCacheMaxSize(STRIPE_CACHE_MAX_SIZE)
                .build();

        String expectedString = "DwrfWriterOptions{stripeCacheEnabled=true, " +
                "stripeCacheMode=INDEX_AND_FOOTER, stripeCacheMaxSize=27MB}";
        assertEquals(options.toString(), expectedString);
    }
}
