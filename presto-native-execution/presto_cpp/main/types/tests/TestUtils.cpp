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
#include <boost/algorithm/string.hpp>
#include <boost/filesystem.hpp>

namespace facebook::presto::test::utils {

const std::string getDataPath(const std::string& fileName) {
  std::string currentPath = boost::filesystem::current_path().c_str();
  if (boost::algorithm::ends_with(currentPath, "fbcode")) {
    return currentPath +
        "/github/presto-trunk/presto-native-execution/presto_cpp/main/types/tests/data/" +
        fileName;
  }
  if (boost::algorithm::ends_with(currentPath, "fbsource")) {
    return currentPath + "/third-party/presto_cpp/main/types/tests/data/" +
        fileName;
  }

  // CLion runs the tests from cmake-build-release/ or cmake-build-debug/
  // directory. Hard-coded json files are not copied there and test fails with
  // file not found. Fixing the path so that we can trigger these tests from
  // CLion.
  boost::algorithm::replace_all(currentPath, "cmake-build-release/", "");
  boost::algorithm::replace_all(currentPath, "cmake-build-debug/", "");

  return currentPath + "/data/" + fileName;
}
} // namespace facebook::presto::test::utils
