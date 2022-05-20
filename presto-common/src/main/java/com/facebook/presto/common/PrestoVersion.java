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
package com.facebook.presto.common;

import java.util.Objects;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static java.util.Objects.requireNonNull;

public class PrestoVersion
        implements Comparable<PrestoVersion>
{
    private static final Pattern VERSION_REGEX = Pattern.compile("^(?<major>0|[1-9]\\d*)\\.(?<minor>0|[1-9]\\d*)(\\.(?<patch>0|[1-9]\\d*))?(?:-(?<prerelease>(?:0|[1-9]\\d*|\\d*[a-zA-Z-][0-9a-zA-Z-]*)(?:\\.(?:0|[1-9]\\d*|\\d*[a-zA-Z-][0-9a-zA-Z-]*))*))?(?:\\+(?<buildmetadata>[0-9a-zA-Z-]+(?:\\.[0-9a-zA-Z-]+)*))?$");

    private int majorVersion;
    private int minorVersion;
    private int patchVersion;
    private String prereleaseVersion = "";
    private String version;

    public PrestoVersion(String version)
    {
        this.version = requireNonNull(version, "version string is null");
        Matcher matcher = VERSION_REGEX.matcher(version);
        if (matcher.find()) {
            this.majorVersion = Integer.parseInt(Optional.ofNullable(matcher.group("major")).orElse("0"));
            this.minorVersion = Integer.parseInt(Optional.ofNullable(matcher.group("minor")).orElse("0"));
            this.patchVersion = Integer.parseInt(Optional.ofNullable(matcher.group("patch")).orElse("0"));
            this.prereleaseVersion = Optional.ofNullable(matcher.group("prerelease")).orElse("");
        }
    }

    public String getVersion()
    {
        return version;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(this.majorVersion, this.minorVersion, this.patchVersion, this.prereleaseVersion);
    }

    @Override
    public boolean equals(Object version)
    {
        if (this == version) {
            return true;
        }
        if (version == null || !(version instanceof PrestoVersion)) {
            return false;
        }
        PrestoVersion prestoVersion = (PrestoVersion) version;
        return (Objects.equals(this.majorVersion, prestoVersion.majorVersion)
                && Objects.equals(this.minorVersion, prestoVersion.minorVersion)
                && Objects.equals(this.patchVersion, prestoVersion.patchVersion)
                && Objects.equals(this.prereleaseVersion, prestoVersion.prereleaseVersion));
    }

    @Override
    public int compareTo(PrestoVersion prestoVersion)
    {
        if (this.majorVersion != prestoVersion.majorVersion) {
            return Integer.compare(this.majorVersion, prestoVersion.majorVersion);
        }
        if (this.minorVersion != prestoVersion.minorVersion) {
            return Integer.compare(this.minorVersion, prestoVersion.minorVersion);
        }
        if (this.patchVersion != prestoVersion.patchVersion) {
            return Integer.compare(this.patchVersion, prestoVersion.patchVersion);
        }
        if (!(this.prereleaseVersion.equals(prestoVersion.prereleaseVersion))) {
            if (prestoVersion.prereleaseVersion.isEmpty()) {
                return -1;
            }
            if (this.prereleaseVersion.isEmpty()) {
                return 1;
            }
            return this.prereleaseVersion.compareTo(prestoVersion.prereleaseVersion);
        }
        return 0;
    }

    public boolean equalTo(PrestoVersion prestoVersion)
    {
        return this.compareTo(prestoVersion) == 0;
    }

    public boolean greaterThan(PrestoVersion prestoVersion)
    {
        return this.compareTo(prestoVersion) > 0;
    }

    public boolean lessThan(PrestoVersion prestoVersion)
    {
        return this.compareTo(prestoVersion) < 0;
    }

    public boolean greaterThanOrEqualTo(PrestoVersion prestoVersion)
    {
        return this.compareTo(prestoVersion) >= 0;
    }

    public boolean lessThanOrEqualTo(PrestoVersion prestoVersion)
    {
        return this.compareTo(prestoVersion) <= 0;
    }
}
