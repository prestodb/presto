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

import com.facebook.presto.spi.session.SessionPropertyMetadata;
import com.google.common.collect.ImmutableList;
import org.testng.annotations.Test;

import java.net.URISyntaxException;
import java.util.List;

import static com.facebook.presto.common.type.BooleanType.BOOLEAN;
import static com.facebook.presto.common.type.DoubleType.DOUBLE;
import static com.facebook.presto.common.type.IntegerType.INTEGER;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static org.testng.Assert.assertEquals;

public class TestNativeSessionPropertyProvider
{
    @Test
    public void testDeserializeSessionProperties()
            throws URISyntaxException
    {
        String responseBody =
                "[" +
                        "{\"name\":\"sample1\",\"description\":\"Sample description 1\",\"typeSignature\":\"integer\",\"defaultValue\":\"100\",\"hidden\":true}," +
                        "{\"name\":\"sample2\",\"description\":\"Sample description 2\",\"typeSignature\":\"boolean\",\"defaultValue\":\"true\",\"hidden\":false}," +
                        "{\"name\":\"sample3\",\"description\":\"Sample description 3\",\"typeSignature\":\"varchar\",\"defaultValue\":\"N/A\",\"hidden\":true}," +
                        "{\"name\":\"sample4\",\"description\":\"Sample description 4\",\"typeSignature\":\"double\",\"defaultValue\":\"3.14\",\"hidden\":false}" +
                        "]";

        // Correct object creation to match responseBody
        SessionPropertyMetadata s1 = new SessionPropertyMetadata("sample1", "Sample description 1", INTEGER.getTypeSignature(), "100", true);
        SessionPropertyMetadata s2 = new SessionPropertyMetadata("sample2", "Sample description 2", BOOLEAN.getTypeSignature(), "true", false);
        SessionPropertyMetadata s3 = new SessionPropertyMetadata("sample3", "Sample description 3", VARCHAR.getTypeSignature(), "N/A", true);
        SessionPropertyMetadata s4 = new SessionPropertyMetadata("sample4", "Sample description 4", DOUBLE.getTypeSignature(), "3.14", false);

        List<SessionPropertyMetadata> expected = ImmutableList.of(s1, s2, s3, s4);
        List<SessionPropertyMetadata> decoded = NativeSystemSessionPropertyProvider.deserializeSessionProperties(responseBody);

        assertEquals(decoded.toArray(), expected.toArray());
    }
}
