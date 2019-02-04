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

import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.function.ScalarFunction;
import com.facebook.presto.spi.function.SqlNullable;
import com.facebook.presto.spi.function.SqlType;
import com.facebook.presto.spi.type.StandardTypes;
import com.google.i18n.phonenumbers.NumberParseException;
import com.google.i18n.phonenumbers.PhoneNumberUtil;
import com.google.i18n.phonenumbers.PhoneNumberUtil.PhoneNumberFormat;
import com.google.i18n.phonenumbers.Phonenumber.PhoneNumber;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;

import java.util.Arrays;

import static com.facebook.presto.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static com.google.i18n.phonenumbers.PhoneNumberUtil.PhoneNumberFormat.RFC3966;

public class CanonicalizePhoneNumberFunction
{
    private static final PhoneNumberUtil PHONE_NUMBER_UTIL = PhoneNumberUtil.getInstance();

    private CanonicalizePhoneNumberFunction() {}

    @ScalarFunction("canonicalize_phone_number")
    @SqlType(StandardTypes.VARCHAR)
    public static Slice canonicalizePhoneNumber(
            @SqlType(StandardTypes.VARCHAR) Slice phoneNumber)
    {
        return canonicalizePhoneNumber(phoneNumber, null, null);
    }

    @ScalarFunction("canonicalize_phone_number")
    @SqlType(StandardTypes.VARCHAR)
    public static Slice canonicalizePhoneNumber(
            @SqlType(StandardTypes.VARCHAR) Slice phoneNumber,
            @SqlType(StandardTypes.VARCHAR) Slice country)
    {
        return canonicalizePhoneNumber(phoneNumber, country, null);
    }

    @ScalarFunction("canonicalize_phone_number")
    @SqlType(StandardTypes.VARCHAR)
    public static Slice canonicalizePhoneNumber(
            @SqlType(StandardTypes.VARCHAR) Slice phoneNumber,
            @SqlNullable @SqlType(StandardTypes.VARCHAR) Slice country,
            @SqlType(StandardTypes.VARCHAR) Slice format)
    {
        PhoneNumberFormat numberFormat;
        if (format == null) {
            numberFormat = RFC3966;
        }
        else {
            String formatString = format.toStringUtf8().toUpperCase();
            try {
                numberFormat = PhoneNumberFormat.valueOf(formatString);
            }
            catch (IllegalArgumentException ex) {
                throw new PrestoException(INVALID_FUNCTION_ARGUMENT, "Invalid format provided: " + formatString + ". Allowed values: " +
                        Arrays.toString(PhoneNumberFormat.values()));
            }
        }

        PhoneNumber number;
        try {
            number = PHONE_NUMBER_UTIL.parse(phoneNumber.toStringUtf8(), country != null ? country.toStringUtf8().toUpperCase() : null);
        }
        catch (NumberParseException ex) {
            throw new PrestoException(INVALID_FUNCTION_ARGUMENT, "Unable to parse phone number: " + phoneNumber.toStringUtf8(), ex);
        }
        return Slices.utf8Slice(PHONE_NUMBER_UTIL.format(number, numberFormat));
    }
}
