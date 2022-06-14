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
#include "velox/type/Tokenizer.h"

namespace facebook::velox::common {

Tokenizer::Tokenizer(const std::string& path) : path_(path) {
  state = State::kNotReady;
  index_ = 0;
}

bool Tokenizer::hasNext() {
  switch (state) {
    case State::kDone:
      return false;
    case State::kReady:
      return true;
    case State::kNotReady:
      break;
    case State::kFailed:
      VELOX_FAIL("Illegal state");
  }
  return tryToComputeNext();
}

std::unique_ptr<Subfield::PathElement> Tokenizer::next() {
  if (!hasNext()) {
    VELOX_FAIL("No more tokens");
  }
  state = State::kNotReady;
  return std::move(next_);
}

bool Tokenizer::hasNextCharacter() {
  return index_ < path_.length();
}

std::unique_ptr<Subfield::PathElement> Tokenizer::computeNext() {
  if (!hasNextCharacter()) {
    state = State::kDone;
    return nullptr;
  }

  if (tryMatch(DOT)) {
    std::unique_ptr<Subfield::PathElement> token = matchPathSegment();
    firstSegment = false;
    return token;
  }

  if (tryMatch(OPEN_BRACKET)) {
    std::unique_ptr<Subfield::PathElement> token = tryMatch(QUOTE)
        ? matchQuotedSubscript()
        : tryMatch(WILDCARD) ? matchWildcardSubscript()
                             : matchUnquotedSubscript();

    match(CLOSE_BRACKET);
    firstSegment = false;
    return token;
  }

  if (firstSegment) {
    std::unique_ptr<Subfield::PathElement> token = matchPathSegment();
    firstSegment = false;
    return token;
  }

  VELOX_UNREACHABLE();
}

void Tokenizer::match(char expected) {
  if (!tryMatch(expected)) {
    invalidSubfieldPath();
  }
}

bool Tokenizer::tryMatch(char expected) {
  if (!hasNextCharacter() || peekCharacter() != expected) {
    return false;
  }
  index_++;
  return true;
}

void Tokenizer::nextCharacter() {
  index_++;
}

char Tokenizer::peekCharacter() {
  return path_[index_];
}

std::unique_ptr<Subfield::PathElement> Tokenizer::matchPathSegment() {
  // seek until we see a special character or whitespace
  int start = index_;
  while (hasNextCharacter() && isUnquotedPathCharacter(peekCharacter())) {
    nextCharacter();
  }
  int end = index_;

  std::string token = path_.substr(start, end - start);

  // an empty unquoted token is not allowed
  if (token.empty()) {
    invalidSubfieldPath();
  }

  return std::make_unique<Subfield::NestedField>(token);
}

std::unique_ptr<Subfield::PathElement> Tokenizer::matchUnquotedSubscript() {
  // seek until we see a special character or whitespace
  int start = index_;
  while (hasNextCharacter() && isUnquotedSubscriptCharacter(peekCharacter())) {
    nextCharacter();
  }
  int end = index_;

  std::string token = path_.substr(start, end);

  // an empty unquoted token is not allowed
  if (token.empty()) {
    invalidSubfieldPath();
  }
  int index = 0;
  try {
    index = std::stoi(token);
  } catch (...) {
    VELOX_FAIL("Invalid index {}", token);
  }
  return std::make_unique<Subfield::LongSubscript>(index);
}

bool Tokenizer::isUnquotedPathCharacter(char c) {
  return c == ':' || c == '$' || c == '-' || c == '/' || c == '@' || c == '|' ||
      c == '#' || isUnquotedSubscriptCharacter(c);
}

bool Tokenizer::isUnquotedSubscriptCharacter(char c) {
  return c == '-' || c == '_' || isalnum(c);
}

std::unique_ptr<Subfield::PathElement> Tokenizer::matchQuotedSubscript() {
  // quote has already been matched

  // seek until we see the close quote
  std::string token;
  bool escaped = false;

  while (hasNextCharacter() && (escaped || peekCharacter() != QUOTE)) {
    if (escaped) {
      switch (peekCharacter()) {
        case '\"':
        case '\\':
          token += peekCharacter();
          break;
        default:
          invalidSubfieldPath();
      }
      escaped = false;
    } else {
      if (peekCharacter() == BACKSLASH) {
        escaped = true;
      } else {
        token += peekCharacter();
      }
    }
    nextCharacter();
  }
  if (escaped) {
    invalidSubfieldPath();
  }

  match(QUOTE);

  if (token == "*") {
    return std::make_unique<Subfield::AllSubscripts>();
  }
  return std::make_unique<Subfield::StringSubscript>(token);
}

std::unique_ptr<Subfield::PathElement> Tokenizer::matchWildcardSubscript() {
  return std::make_unique<Subfield::AllSubscripts>();
}

void Tokenizer::invalidSubfieldPath() {
  VELOX_FAIL("Invalid subfield path: {}", this->toString());
}

std::string Tokenizer::toString() {
  return path_.substr(0, index_) + UNICODE_CARET + path_.substr(index_);
}

bool Tokenizer::tryToComputeNext() {
  state = State::kFailed; // temporary pessimism
  next_ = computeNext();
  if (state != State::kDone) {
    state = State::kReady;
    return true;
  }
  return false;
}
} // namespace facebook::velox::common
