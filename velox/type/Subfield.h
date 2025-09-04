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

#include <fmt/format.h>
#include <ostream>

#include "velox/common/Enums.h"
#include "velox/common/base/Exceptions.h"
#include "velox/common/base/Macros.h"

namespace facebook::velox::common {

enum class SubfieldKind {
  kAllSubscripts,
  kNestedField,
  kStringSubscript,
  kLongSubscript,
};

VELOX_DECLARE_ENUM_NAME(SubfieldKind)

// Contains field name separators to be used in Tokenizer.
struct Separators {
  static std::shared_ptr<const Separators> get() {
    VELOX_CONSTEXPR_SINGLETON Separators kInstance;
    return {std::shared_ptr<const Separators>{}, &kInstance};
  }

  bool isSeparator(char c) const {
    return c == closeBracket || c == dot || c == openBracket || c == quote ||
        c == wildCard;
  }

  char backSlash = '\\';
  char closeBracket = ']';
  char dot = '.';
  char openBracket = '[';
  char quote = '\"';
  char wildCard = '*';
  char unicodeCaret = '^';
};

class Subfield {
 public:
  class PathElement {
   public:
    explicit PathElement(SubfieldKind kind) : kind_{kind} {}

    virtual ~PathElement() = default;

    SubfieldKind kind() const {
      return kind_;
    }

    bool isSubscript() const {
      return kind_ != SubfieldKind::kNestedField;
    }

    virtual std::string toString() const = 0;

    virtual size_t hash() const = 0;

    virtual bool operator==(const PathElement& other) const = 0;

    virtual std::unique_ptr<PathElement> clone() = 0;

    bool is(SubfieldKind k) const {
      return kind_ == k;
    }

    template <typename T>
    const T* as() const {
      return dynamic_cast<const T*>(this);
    }

   private:
    const SubfieldKind kind_;
  };

  class AllSubscripts final : public PathElement {
   public:
    AllSubscripts() : PathElement(SubfieldKind::kAllSubscripts) {}

    std::string toString() const override {
      return "[*]";
    }

    size_t hash() const override {
      return 0;
    }

    bool operator==(const PathElement& other) const override {
      return other.kind() == SubfieldKind::kAllSubscripts;
    }

    std::unique_ptr<PathElement> clone() override {
      return std::make_unique<AllSubscripts>();
    }
  };

  class NestedField final : public PathElement {
   public:
    explicit NestedField(const std::string& name)
        : PathElement(SubfieldKind::kNestedField), name_(name) {
      VELOX_USER_CHECK(
          !name.empty(), "NestedFields must have non-empty names.");
    }

    const std::string& name() const {
      return name_;
    }

    bool operator==(const PathElement& other) const override {
      if (this == &other) {
        return true;
      }
      return other.kind() == SubfieldKind::kNestedField &&
          other.as<NestedField>()->name_ == name_;
    }

    size_t hash() const override {
      return std::hash<std::string>()(name_);
    }

    std::string toString() const override {
      return "." + name_;
    }

    std::unique_ptr<PathElement> clone() override {
      return std::make_unique<NestedField>(name_);
    }

   private:
    const std::string name_;
  };

  class LongSubscript final : public PathElement {
   public:
    explicit LongSubscript(long index)
        : PathElement(SubfieldKind::kLongSubscript), index_(index) {}

    long index() const {
      return index_;
    }

    bool operator==(const PathElement& other) const override {
      if (this == &other) {
        return true;
      }
      return other.kind() == SubfieldKind::kLongSubscript &&
          other.as<LongSubscript>()->index_ == index_;
    }

    size_t hash() const override {
      return std::hash<long>()(index_);
    }

    std::string toString() const override {
      return "[" + std::to_string(index_) + "]";
    }

    std::unique_ptr<PathElement> clone() override {
      return std::make_unique<LongSubscript>(index_);
    }

   private:
    const long index_;
  };

  class StringSubscript final : public PathElement {
   public:
    explicit StringSubscript(const std::string& index)
        : PathElement(SubfieldKind::kStringSubscript), index_(index) {}

    const std::string index() const {
      return index_;
    }

    bool operator==(const PathElement& other) const override {
      if (this == &other) {
        return true;
      }
      return other.kind() == SubfieldKind::kStringSubscript &&
          other.as<StringSubscript>()->index_ == index_;
    }

    size_t hash() const override {
      return std::hash<std::string>()(index_);
    }

    std::string toString() const override;

    std::unique_ptr<PathElement> clone() override {
      return std::make_unique<StringSubscript>(index_);
    }

   private:
    const std::string index_;
  };

  // Separators: the customized separators to tokenize field name.
  explicit Subfield(
      const std::string& path,
      std::shared_ptr<const Separators> separators = Separators::get());

  explicit Subfield(std::vector<std::unique_ptr<PathElement>>&& path);

  Subfield() = default;

  static std::unique_ptr<Subfield> create(const std::string& field) {
    std::vector<std::unique_ptr<PathElement>> path;
    path.push_back(std::make_unique<NestedField>(field));
    return std::make_unique<Subfield>(std::move(path));
  }

  /// @return The name of the base column.
  const std::string& baseName() const {
    return path_[0]->as<NestedField>()->name();
  }

  const std::vector<std::unique_ptr<PathElement>>& path() const {
    return path_;
  }

  std::vector<std::unique_ptr<PathElement>>& path() {
    return path_;
  }

  bool isPrefix(const Subfield& other) const;

  std::string toString() const;

  bool operator==(const Subfield& other) const;

  size_t hash() const;

  bool valid() const {
    return !path_.empty() && path_[0]->is(SubfieldKind::kNestedField);
  }

  Subfield clone() const;

 private:
  std::vector<std::unique_ptr<PathElement>> path_;
};

inline std::ostream& operator<<(std::ostream& out, const Subfield& subfield) {
  return out << subfield.toString();
}

} // namespace facebook::velox::common

namespace std {
template <>
struct hash<::facebook::velox::common::Subfield> {
  size_t operator()(const ::facebook::velox::common::Subfield& subfield) const {
    return subfield.hash();
  }
};
} // namespace std

template <>
struct fmt::formatter<::facebook::velox::common::Subfield>
    : formatter<std::string> {
  auto format(const ::facebook::velox::common::Subfield& s, format_context& ctx)
      const {
    return formatter<std::string>::format(s.toString(), ctx);
  }
};

template <>
struct fmt::formatter<::facebook::velox::common::SubfieldKind>
    : formatter<int> {
  auto format(
      const ::facebook::velox::common::SubfieldKind& s,
      format_context& ctx) const {
    return formatter<int>::format(static_cast<int>(s), ctx);
  }
};
