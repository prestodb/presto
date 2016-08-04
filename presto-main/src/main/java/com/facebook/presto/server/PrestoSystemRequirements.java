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
import org.joda.time.DateTime;

import javax.management.MBeanServer;
import javax.management.ObjectName;

import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.nio.ByteOrder;
import java.util.List;
import java.util.Locale;
import java.util.OptionalLong;

import static com.facebook.presto.util.ImmutableCollectors.toImmutableList;
import static java.lang.management.ManagementFactory.OPERATING_SYSTEM_MXBEAN_NAME;

final class PrestoSystemRequirements
{
    private static final int MIN_FILE_DESCRIPTORS = 4096;
    private static final int RECOMMENDED_FILE_DESCRIPTORS = 8192;

    private PrestoSystemRequirements() {}

    public static void verifyJvmRequirements()
    {
        String vendor = StandardSystemProperty.JAVA_VENDOR.value();
        if (!"Oracle Corporation".equals(vendor)) {
            failRequirement("Presto requires an Oracle or OpenJDK JVM (found %s)", vendor);
        }

        verifyJavaVersion();

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

        verifyUsingG1Gc();

        verifyFileDescriptor();

        verifySlice();
    }

    private static void verifyJavaVersion()
    {
        String javaVersion = StandardSystemProperty.JAVA_VERSION.value();
        if (javaVersion == null) {
            failRequirement("Java version not defined");
        }

        JavaVersion version = JavaVersion.parse(javaVersion);
        if (version.getMajor() == 8 && version.getUpdate().isPresent() && version.getUpdate().getAsInt() >= 60) {
            return;
        }

        if (version.getMajor() == 9) {
            return;
        }

        failRequirement("Presto requires Java 8u60+ (found %s)", javaVersion);
    }

    private static void verifyUsingG1Gc()
    {
        try {
            List<String> garbageCollectors = ManagementFactory.getGarbageCollectorMXBeans().stream()
                    .map(GarbageCollectorMXBean::getName)
                    .collect(toImmutableList());

            if (garbageCollectors.stream().noneMatch(name -> name.toUpperCase(Locale.US).startsWith("G1 "))) {
                warnRequirement("Current garbage collectors are %s. Presto recommends the G1 garbage collector.", garbageCollectors);
            }
        }
        catch (RuntimeException e) {
            // This should never happen since we have verified the OS and JVM above
            failRequirement("Cannot read garbage collector information: %s", e);
        }
    }

    private static void verifyFileDescriptor()
    {
        OptionalLong maxFileDescriptorCount = getMaxFileDescriptorCount();
        if (!maxFileDescriptorCount.isPresent()) {
            // This should never happen since we have verified the OS and JVM above
            failRequirement("Cannot read OS file descriptor limit");
        }
        if (maxFileDescriptorCount.getAsLong() < MIN_FILE_DESCRIPTORS) {
            failRequirement("Presto requires at least %s file descriptors (found %s)", MIN_FILE_DESCRIPTORS, maxFileDescriptorCount.getAsLong());
        }
        if (maxFileDescriptorCount.getAsLong() < RECOMMENDED_FILE_DESCRIPTORS) {
            warnRequirement("Current OS file descriptor limit is %s. Presto recommends at least %s", maxFileDescriptorCount.getAsLong(), RECOMMENDED_FILE_DESCRIPTORS);
        }
    }

    private static OptionalLong getMaxFileDescriptorCount()
    {
        try {
            MBeanServer mbeanServer = ManagementFactory.getPlatformMBeanServer();
            Object maxFileDescriptorCount = mbeanServer.getAttribute(ObjectName.getInstance(OPERATING_SYSTEM_MXBEAN_NAME), "MaxFileDescriptorCount");
            return OptionalLong.of(((Number) maxFileDescriptorCount).longValue());
        }
        catch (Exception e) {
            return OptionalLong.empty();
        }
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

    /**
     * Perform a sanity check to make sure that the year is reasonably current, to guard against
     * issues in third party libraries.
     */
    public static void verifySystemTimeIsReasonable()
    {
        int currentYear = DateTime.now().year().get();
        if (currentYear < 2015) {
            failRequirement("Presto requires the system time to be current (found year %s)", currentYear);
        }
    }

    private static void failRequirement(String format, Object... args)
    {
        System.err.println(String.format(format, args));
        System.exit(100);
    }

    private static void warnRequirement(String format, Object... args)
    {
        System.err.println("WARNING: " + String.format(format, args));
    }
}
