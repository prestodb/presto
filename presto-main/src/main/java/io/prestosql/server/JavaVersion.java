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
package io.prestosql.server;

import com.google.common.base.StandardSystemProperty;

import java.util.Objects;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.lang.String.format;

// TODO: remove this when we upgrade to Java 9 (replace with java.lang.Runtime.getVersion())
public class JavaVersion
{
    // As described in JEP-223
    private static final String VERSION_NUMBER = "(?<MAJOR>[1-9][0-9]*)(\\.(?<MINOR>(0|[1-9][0-9]*))(\\.(?:(0|[1-9][0-9]*)))?)?";
    private static final String PRE = "(?:-(?:[a-zA-Z0-9]+))?";
    private static final String BUILD = "(?:(?:\\+)(?:0|[1-9][0-9]*)?)?";
    private static final String OPT = "(?:-(?:[-a-zA-Z0-9.]+))?";
    private static final Pattern PATTERN = Pattern.compile(VERSION_NUMBER + PRE + BUILD + OPT);

    // For Java 8 and below
    private static final Pattern LEGACY_PATTERN = Pattern.compile("1\\.(?<MAJOR>[0-9]+)(\\.(?<MINOR>(0|[1-9][0-9]*)))?(_(?<UPDATE>[1-9][0-9]*))?" + OPT);

    private final int major;
    private final int minor;
    private final OptionalInt update;

    public static JavaVersion current()
    {
        return parse(StandardSystemProperty.JAVA_VERSION.value());
    }

    public static JavaVersion parse(String version)
    {
        Matcher matcher = LEGACY_PATTERN.matcher(version);
        if (matcher.matches()) {
            int major = Integer.parseInt(matcher.group("MAJOR"));
            int minor = Optional.ofNullable(matcher.group("MINOR"))
                    .map(Integer::parseInt)
                    .orElse(0);

            String update = matcher.group("UPDATE");
            if (update == null) {
                return new JavaVersion(major, minor);
            }

            return new JavaVersion(major, minor, OptionalInt.of(Integer.parseInt(update)));
        }

        matcher = PATTERN.matcher(version);
        if (matcher.matches()) {
            int major = Integer.parseInt(matcher.group("MAJOR"));
            int minor = Optional.ofNullable(matcher.group("MINOR"))
                    .map(Integer::parseInt)
                    .orElse(0);

            return new JavaVersion(major, minor);
        }

        throw new IllegalArgumentException(format("Cannot parse version %s", version));
    }

    public JavaVersion(int major, int minor, OptionalInt update)
    {
        this.major = major;
        this.minor = minor;
        this.update = update;
    }

    public JavaVersion(int major, int minor)
    {
        this(major, minor, OptionalInt.empty());
    }

    public int getMajor()
    {
        return major;
    }

    public int getMinor()
    {
        return minor;
    }

    public OptionalInt getUpdate()
    {
        return update;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        JavaVersion that = (JavaVersion) o;
        return major == that.major &&
                minor == that.minor &&
                Objects.equals(update, that.update);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(major, minor, update);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("major", major)
                .add("minor", minor)
                .add("update", update)
                .toString();
    }
}
