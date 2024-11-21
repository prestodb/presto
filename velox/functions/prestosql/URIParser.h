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
#pragma once

#include "velox/type/StringView.h"

namespace facebook::velox::functions {
/// A struct containing the parts of the URI that were extracted during parsing.
/// If the field was not found, it is empty.
///
/// For fields that can contain percent-encoded characters, the `...HasEncoded`
/// flag indicates whether the field contains any percent-encoded characters.
struct URI {
  StringView scheme;
  StringView path;
  bool pathHasEncoded = false;
  StringView query;
  bool queryHasEncoded = false;
  StringView fragment;
  bool fragmentHasEncoded = false;
  StringView host;
  bool hostHasEncoded = false;
  StringView port;
};

/// Parse a URI string into a URI struct according to RFC 3986.
bool parseUri(const StringView& uriStr, URI& uri);
} // namespace facebook::velox::functions
