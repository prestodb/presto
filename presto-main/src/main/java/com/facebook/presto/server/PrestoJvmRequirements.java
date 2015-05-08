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
package com.facebook.presto.server;

import com.google.common.base.StandardSystemProperty;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;

import java.nio.ByteOrder;

final class PrestoJvmRequirements
{
    private PrestoJvmRequirements() {}

    public static void verifyJvmRequirements()
    {
        String specVersion = StandardSystemProperty.JAVA_SPECIFICATION_VERSION.value();
        if ((specVersion == null) || (specVersion.compareTo("1.8") < 0)) {
            failRequirement("Presto requires Java 1.8+ (found %s)", specVersion);
        }

        String vendor = StandardSystemProperty.JAVA_VENDOR.value();
        if (!"Oracle Corporation".equals(vendor)) {
            failRequirement("Presto requires an Oracle or OpenJDK JVM (found %s)", vendor);
        }

        String dataModel = System.getProperty("sun.arch.data.model");
        if (!"64".equals(dataModel)) {
            failRequirement("Presto requires a 64-bit JVM (found %s)", dataModel);
        }

        String osName = StandardSystemProperty.OS_NAME.value();
        String osArch = StandardSystemProperty.OS_ARCH.value();
        if ("Linux".equals(osName)) {
            if (!"amd64".equals(osArch)) {
                failRequirement("Presto requires x86-64 or amd64 on Linux (found %s)", osArch);
            }
        }
        else if ("Mac OS X".equals(osName)) {
            if (!"x86_64".equals(osArch)) {
                failRequirement("Presto requires x86_64 on Mac OS X (found %s)", osArch);
            }
        }
        else {
            failRequirement("Presto requires Linux or Mac OS X (found %s)", osName);
        }

        if (!ByteOrder.nativeOrder().equals(ByteOrder.LITTLE_ENDIAN)) {
            failRequirement("Presto requires a little endian platform (found %s)", ByteOrder.nativeOrder());
        }

        verifySlice();
    }

    private static void verifySlice()
    {
        Slice slice = Slices.wrappedBuffer(new byte[5]);
        slice.setByte(4, 0xDE);
        slice.setByte(3, 0xAD);
        slice.setByte(2, 0xBE);
        slice.setByte(1, 0xEF);
        if (slice.getInt(1) != 0xDEADBEEF) {
            failRequirement("Slice library produced an unexpected result");
        }
    }

    private static void failRequirement(String format, Object... args)
    {
        System.err.println(String.format(format, args));
        System.exit(100);
    }
}
