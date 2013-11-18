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

import com.google.common.base.Splitter;
import com.google.common.base.StandardSystemProperty;
import com.google.common.collect.Iterables;

import java.nio.ByteOrder;

final class PrestoJvmRequirements
{
    private PrestoJvmRequirements() {}

    public static void verifyJvmRequirements()
    {
        String specVersion = StandardSystemProperty.JAVA_SPECIFICATION_VERSION.value();
        if (specVersion.compareTo("1.7") < 0) {
            failRequirement("Presto requires Java 1.7+ (found %s)", specVersion);
        }

        String vendor = StandardSystemProperty.JAVA_VENDOR.value();
        if (!vendor.equals("Oracle Corporation")) {
            failRequirement("Presto requires an Oracle or OpenJDK JVM (found %s)", vendor);
        }

        String version = StandardSystemProperty.JAVA_VERSION.value();
        if (jdkVersion(version).equals("1.7.0") && (jdkUpdate(version).compareTo("06") < 0)) {
            failRequirement("Presto requires Oracle JDK 7u6+ (found %s)", version);
        }

        String osName = StandardSystemProperty.OS_NAME.value();
        String osArch = StandardSystemProperty.OS_ARCH.value();
        switch (osName) {
            case "Linux":
                if (!osArch.equals("amd64")) {
                    failRequirement("Presto requires architecture amd64 on Linux (found %s)", osArch);
                }
                break;
            case "Mac OS X":
                if (!osArch.equals("x86_64")) {
                    failRequirement("Presto requires architecture x86_64 on Mac OS X (found %s)", osArch);
                }
                break;
            default:
                failRequirement("Presto requires Linux or Mac OS X (found %s)", osName);
        }

        if (!ByteOrder.nativeOrder().equals(ByteOrder.LITTLE_ENDIAN)) {
            failRequirement("Presto requires a little endian platform (found %s)", ByteOrder.nativeOrder());
        }
    }

    private static void failRequirement(String format, Object... args)
    {
        System.err.println(String.format(format, args));
        System.exit(100);
    }

    private static String jdkVersion(String version)
    {
        return Splitter.on('_').split(version).iterator().next();
    }

    private static String jdkUpdate(String version)
    {
        return Iterables.get(Splitter.on('_').split(version), 1, "");
    }
}
