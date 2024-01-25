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
package com.facebook.presto.sessionpropertyproviders;

import org.testng.annotations.Test;

import java.net.URI;
import java.net.URISyntaxException;

import static org.testng.Assert.assertEquals;

public class TestNativeSessionPropertyProvider
{
    @Test
    public void testGetSessionProperties()
            throws URISyntaxException
    {
        URI uri = new URI("http://localhost:7777");
        // Validate size of session properties for now.
        assertEquals(new NativeSystemSessionPropertyProvider(uri).getSessionProperties().size(), 3);
    }
}
