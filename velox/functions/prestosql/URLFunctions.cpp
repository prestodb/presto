/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
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

#include "URLFunctions.h"
#include "velox/type/Type.h"

namespace facebook::velox::functions {

bool matchAuthorityAndPath(
    const boost::cmatch& urlMatch,
    boost::cmatch& authAndPathMatch,
    boost::cmatch& authorityMatch,
    bool& hasAuthority) {
  static const boost::regex kAuthorityAndPathRegex("//([^/]*)(/.*)?");
  auto authorityAndPath = submatch(urlMatch, 2);
  if (!boost::regex_match(
          authorityAndPath.begin(),
          authorityAndPath.end(),
          authAndPathMatch,
          kAuthorityAndPathRegex)) {
    // Does not start with //, doesn't have authority.
    hasAuthority = false;
    return true;
  }

  static const boost::regex kAuthorityRegex(
      "(?:([^@:]*)(?::([^@]*))?@)?" // username, password.
      "(\\[[^\\]]*\\]|[^\\[:]*)" // host (IP-literal (e.g. '['+IPv6+']',
      // dotted-IPv4, or named host).
      "(?::(\\d*))?"); // port.

  const auto authority = authAndPathMatch[1];
  if (!boost::regex_match(
          authority.first, authority.second, authorityMatch, kAuthorityRegex)) {
    return false; // Invalid URI Authority.
  }

  hasAuthority = true;

  return true;
}

} // namespace facebook::velox::functions
