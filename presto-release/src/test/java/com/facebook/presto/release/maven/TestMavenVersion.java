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
package com.facebook.presto.release.maven;

import com.google.common.io.Resources;
import org.testng.annotations.Test;

import java.io.File;

import static com.facebook.presto.release.maven.MavenVersion.fromDirectory;
import static com.facebook.presto.release.maven.MavenVersion.fromPom;
import static com.facebook.presto.release.maven.MavenVersion.fromReleaseVersion;
import static com.facebook.presto.release.maven.MavenVersion.fromSnapshotVersion;
import static org.testng.Assert.assertEquals;

public class TestMavenVersion
{
    @Test
    public void testReleaseVersion()
    {
        assertEquals(fromReleaseVersion("0.230").getVersion(), "0.230");
    }

    @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = "Invalid release version: 0\\.230-SNAPSHOT")
    public void testInvalidReleaseVersion()
    {
        fromReleaseVersion("0.230-SNAPSHOT");
    }

    @Test
    public void testSnapshotVersion()
    {
        assertEquals(fromSnapshotVersion("0.230-SNAPSHOT").getVersion(), "0.230");
    }

    @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = "Invalid snapshot version: 0\\.230")
    public void testInvalidSnapshotVersion()
    {
        fromSnapshotVersion("0.230");
    }

    @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = "Invalid release version: 0\\.0")
    public void testZeroVersion()
    {
        fromReleaseVersion("0.0");
    }

    @Test
    public void testFromPom()
    {
        File pomFile = new File(Resources.getResource("pom.xml").getFile());
        assertEquals(fromPom(pomFile).getVersion(), "0.232");
    }

    @Test
    public void testFromDirectory()
    {
        File pomFile = new File(Resources.getResource("pom.xml").getFile()).getParentFile();
        assertEquals(fromDirectory(pomFile).getVersion(), "0.232");
    }
}
