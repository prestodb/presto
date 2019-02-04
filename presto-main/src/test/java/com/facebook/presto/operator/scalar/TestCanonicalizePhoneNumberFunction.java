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

package com.facebook.presto.operator.scalar;

import com.facebook.presto.spi.type.VarcharType;
import com.google.i18n.phonenumbers.PhoneNumberUtil;
import org.testng.annotations.Test;

import java.util.Arrays;

public class TestCanonicalizePhoneNumberFunction
        extends AbstractTestFunctions
{
    @Test
    public void testCanonicalizePhoneNumber()
    {
        assertFunction("CANONICALIZE_PHONE_NUMBER('412 9 150.000 x 500', 'US')", VarcharType.VARCHAR, "tel:+1-412-915-0000;ext=500");
        assertFunction("CANONICALIZE_PHONE_NUMBER('412 9 150.000 x 500', 'us')", VarcharType.VARCHAR, "tel:+1-412-915-0000;ext=500");
        assertFunction("CANONICALIZE_PHONE_NUMBER('+14129150000')", VarcharType.VARCHAR, "tel:+1-412-915-0000");
        assertFunction("CANONICALIZE_PHONE_NUMBER('+1412 9 150.000 x 500', NULL, 'E164')", VarcharType.VARCHAR, "+14129150000");
        assertFunction("CANONICALIZE_PHONE_NUMBER('412 9 150.000 x 500', 'US', 'E164')", VarcharType.VARCHAR, "+14129150000");
        assertFunction("CANONICALIZE_PHONE_NUMBER('412 9 150.000 x 500', 'US', 'e164')", VarcharType.VARCHAR, "+14129150000");

        assertInvalidFunction(
                "CANONICALIZE_PHONE_NUMBER('412 9 150.000 x 500', 'US', 'I_DONT_EXIST')",
                "Invalid format provided: I_DONT_EXIST. Allowed values: " + Arrays.toString(PhoneNumberUtil.PhoneNumberFormat.values()));

        assertInvalidFunction("CANONICALIZE_PHONE_NUMBER('I_DONT_EXIST_NUMBER', 'US')", "Unable to parse phone number: I_DONT_EXIST_NUMBER");
    }
}
