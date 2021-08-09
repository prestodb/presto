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

#include "velox/connectors/hive/FileHandle.h"

#include <atomic>

namespace facebook::velox {

uint64_t FileHandleSizer::operator()(const FileHandle& fileHandle) {
  // TODO: remember to add in the size of the hash map and its contents
  // when we add it later.
  return fileHandle.file->memoryUsage();
}

std::unique_ptr<FileHandle> FileHandleGenerator::operator()(
    const std::string& filename) {
  auto fileHandle = std::make_unique<FileHandle>();
  fileHandle->file = generateReadFile(filename);
  fileHandle->uuid = StringIdLease(fileIds(), filename);
  VLOG(1) << "Generating file handle for: " << filename
          << " uuid: " << fileHandle->uuid.id();
  // TODO: build the hash map/etc per file type -- presumably after reading
  // the appropriate magic number from the file, or perhaps we include the file
  // type in the file handle key.
  return fileHandle;
}

} // namespace facebook::velox
