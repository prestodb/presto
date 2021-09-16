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

import io.airlift.units.DataSize;
import org.testng.annotations.Test;

import static com.facebook.presto.orc.metadata.DwrfStripeCacheMode.INDEX_AND_FOOTER;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static org.testng.Assert.assertEquals;

public class TestDwrfStripeCacheOptions
{
    @Test
    public void testToString()
    {
        OrcWriterOptions options = OrcWriterOptions.builder()
                .withDwrfStripeCacheEnabled(true)
                .withDwrfStripeCacheMode(INDEX_AND_FOOTER)
                .withDwrfStripeCacheMaxSize(new DataSize(27, MEGABYTE))
                .build();

        DwrfStripeCacheOptions dwrfStripeCacheOptions = options.getDwrfStripeCacheOptions().get();

        String expectedString = "DwrfStripeCacheOptions{stripeCacheMode=INDEX_AND_FOOTER, stripeCacheMaxSize=27MB}";
        assertEquals(dwrfStripeCacheOptions.toString(), expectedString);
    }
}
