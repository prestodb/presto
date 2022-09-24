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

#include "presto_cpp/main/connectors/hive/storage_adapters/FileSystems.h"

#ifdef PRESTO_ENABLE_S3
#include "velox/connectors/hive/storage_adapters/s3fs/S3FileSystem.h" // @manual
#endif

#ifdef PRESTO_ENABLE_HDFS
#include "velox/connectors/hive/storage_adapters/hdfs/HdfsFileSystem.h" // @manual
#endif

namespace facebook::presto {

void registerOptionalHiveStorageAdapters() {
#ifdef PRESTO_ENABLE_S3
  velox::filesystems::registerS3FileSystem();
#endif

#ifdef PRESTO_ENABLE_HDFS
  velox::filesystems::registerHdfsFileSystem();
#endif
}

} // namespace facebook::presto
