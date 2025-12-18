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
package com.facebook.presto.delta;

import org.apache.hadoop.fs.Path;

import java.net.URI;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;

public class DeltaFilePathUtils
{
    private DeltaFilePathUtils() {}

    public static String normalizePath(String inputPath)
    {
        // Normalize partially encoded paths (for example, paths containing encoded spaces)
        // before constructing Hadoop Path, to avoid double-encoding.
        inputPath = getDecodedPath(inputPath);

        // Construct Hadoop Path to ensure proper URI normalization and scheme handling.
        Path path = new Path(inputPath);
        URI uri = path.toUri();

        // Local filesystem paths must be returned as OS-native paths.
        if ("file".equalsIgnoreCase(uri.getScheme())) {
            return uri.getPath();
        }

        // Distributed filesystems and object stores (e.g., S3, HDFS):
        // return a URI-safe string while preserving required encodings.
        if (uri.getScheme() != null) {
            return getDecodedPath(uri.toString());
        }

        // Relative paths (no scheme): return the normalized path component.
        return uri.getPath();
    }

    /**
     * Partially decodes an input path by decoding space encodings while preserving
     * reserved URI characters such as colon (%3A).
     * Full URI decoding is intentionally avoided, as decoding reserved characters
     * can produce invalid URIs and break Hadoop Path or filesystem resolution.
     */
    private static String getDecodedPath(String inputPath)
    {
        String decoded = URLDecoder.decode(
                inputPath.replaceAll("%3A", "__TEMP_COLON__"),
                StandardCharsets.UTF_8);

        decoded = decoded.replace("__TEMP_COLON__", "%3A");
        return decoded;
    }
}
