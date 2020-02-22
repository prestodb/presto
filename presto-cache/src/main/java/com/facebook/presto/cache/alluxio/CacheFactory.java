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
package com.facebook.presto.cache.alluxio;

import alluxio.conf.AlluxioConfiguration;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;

import java.net.URI;

public interface CacheFactory
{
    FileSystem createCachingFileSystem(Configuration factoryConfig, URI factoryUri, FileSystem fileSystem);
    AlluxioCachingClientFileSystem getAlluxioCachingClientFileSystem(FileSystem fileSystem, AlluxioConfiguration alluxioConfiguration);
}
