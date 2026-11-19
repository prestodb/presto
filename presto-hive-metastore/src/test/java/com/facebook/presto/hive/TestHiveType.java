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
package com.facebook.presto.hive;

import com.facebook.presto.common.type.TypeSignature;
import org.testng.annotations.Test;

import static com.facebook.presto.common.type.TypeSignature.parseTypeSignature;
import static org.testng.Assert.assertEquals;

public class TestHiveType
{
    @Test
    public void testStructFieldNamesStartingWithDigitRoundTrip()
    {
        HiveType hiveType = HiveType.valueOf("struct<region:string,2021IDefinitions:struct<adId:string,incstatus:string>>");

        TypeSignature signature = hiveType.getTypeSignature();

        assertEquals(signature.toString(), "row(region varchar,\"2021idefinitions\" row(adid varchar,incstatus varchar))");
        assertEquals(parseTypeSignature(signature.toString()), signature);
    }

    @Test
    public void testStructFieldNamesSimpleNotDelimited()
    {
        HiveType hiveType = HiveType.valueOf("struct<a:int,_b1:string>");

        TypeSignature signature = hiveType.getTypeSignature();

        assertEquals(signature.toString(), "row(a integer,_b1 varchar)");
        assertEquals(parseTypeSignature(signature.toString()), signature);
    }
}
