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
#pragma once

#include <tuple>
#include <type_traits>
#include <unordered_map>
#include <vector>

#include "folly/Optional.h"
#include "velox/common/base/Exceptions.h"
#include "velox/core/Metaprogramming.h"
#include "velox/functions/UDFOutputString.h"
#include "velox/type/Type.h"
#include "velox/type/Variant.h"

namespace facebook {
namespace velox {
namespace core {

// A simple & efficient container/target for user strings
struct StringWriter : public UDFOutputString {
  StringWriter() noexcept : storage_{} {
    setData(storage_.data());
  }

  StringWriter(const StringWriter& rh) : storage_{rh.storage_} {
    setData(storage_.data());
    setSize(rh.size());
    setCapacity(rh.capacity());
  }

  StringWriter(StringWriter&& rh) noexcept : storage_{std::move(rh.storage_)} {
    setData(storage_.data());
    setSize(rh.size());
    setCapacity(rh.capacity());
  }

  StringWriter& operator=(const StringWriter& rh) {
    storage_ = rh.storage_;
    reserve(rh.capacity());
    resize(rh.size());
    return *this;
  }

  StringWriter& operator=(StringWriter&& rh) noexcept {
    storage_ = std::move(rh.storage_);
    setData(storage_.data());
    setSize(rh.size());
    setCapacity(rh.capacity());
    return *this;
  }

  void reserve(size_t size) override {
    // Resizing the storage not StringWriter size.
    // This allow us to write directly write into storage_.data() and assuring
    // what we wrote wont be overwritten on future resize calls.
    storage_.resize(size);
    setData(storage_.data());
    setCapacity(size);
  }

  /// Not called by the UDF but should be called internally at the end of the
  /// UDF call
  void finalize() {
    storage_.resize(size());
  }

  operator StringView() const {
    return StringView(data(), size());
  }

 private:
  folly::fbstring storage_;
};

namespace detail {

template <typename T, typename = int32_t>
struct has_velox_type : std::false_type {};

template <typename T>
struct has_velox_type<T, decltype((void)T::koskiType, 0)> : std::true_type {};
} // namespace detail

template <typename T>
struct UdfToType {
  template <
      typename Tx = T,
      typename std::enable_if_t<detail::has_velox_type<Tx>::value, int32_t> = 0>
  static std::shared_ptr<const Type> koskiType() {
    return T::koskiType();
  }

  template <
      typename Tx = T,
      typename std::enable_if_t<!detail::has_velox_type<Tx>::value, int32_t> =
          0>
  static std::shared_ptr<const Type> koskiType() {
    return CppToType<T>::create();
  }
};

template <typename VAL>
class ArrayValWriter {
 public:
  using container_t = typename std::vector<folly::Optional<VAL>>;
  using iterator = typename container_t::iterator;
  using reference = typename container_t::reference;
  using const_iterator = typename container_t::const_iterator;
  using const_reference = typename container_t::const_reference;
  using val_type = VAL;

  static std::shared_ptr<const Type> koskiType() {
    return ARRAY(UdfToType<val_type>::koskiType());
  }

  ArrayValWriter() = default;
  ArrayValWriter(const_iterator start, const_iterator end)
      : values_(start, end) {}

  void reserve(size_t size) {
    values_.reserve(size);
  }
  iterator begin() {
    return values_.begin();
  }
  iterator end() {
    return values_.end();
  }
  void append(val_type val) {
    values_.push_back(folly::Optional<val_type>{std::move(val)});
  }
  void append(folly::Optional<val_type> val) {
    values_.push_back(std::move(val));
  }
  void appendNullable() {
    append(folly::Optional<val_type>{});
  }
  void clear() {
    values_.clear();
  }

  const_iterator begin() const {
    return values_.begin();
  }
  const_iterator end() const {
    return values_.end();
  }
  const_reference at(size_t index) const {
    return values_.at(index);
  }
  size_t size() const {
    return values_.size();
  }

 private:
  container_t values_;
};

template <typename VAL>
class ArrayValReader : public ArrayValWriter<VAL> {
 public:
  ArrayValReader() = default;
  explicit ArrayValReader(std::vector<VAL> vals) {
    reserve(vals.size());
    for (auto& val : vals) {
      append(std::move(val));
    }
  }
};

template <typename... T>
struct RowWriter {
 public:
  static std::shared_ptr<const Type> koskiType() {
    return ROW({UdfToType<T>::koskiType()...});
  }

  RowWriter() {}

  template <size_t N>
  auto& at() {
    return std::get<N>(values_);
  }

  template <size_t N>
  const auto& at() const {
    return std::get<N>(values_);
  }

  void clear() {
    values_ = std::tuple<folly::Optional<T>...>();
  }

 private:
  std::tuple<folly::Optional<T>...> values_;
};

template <typename... T>
struct RowReader : public RowWriter<T...> {};

template <typename KEY, typename VAL>
struct IMapVal {
  static std::shared_ptr<const Type> koskiType() {
    return MAP(UdfToType<KEY>::koskiType(), UdfToType<VAL>::koskiType());
  }
  using container_t = typename std::unordered_map<KEY, folly::Optional<VAL>>;
  using iterator = typename container_t::iterator;
  using reference = typename container_t::reference;
  using const_iterator = typename container_t::const_iterator;
  using mapped_type = typename container_t::mapped_type;
  using key_type = KEY;
  using val_type = VAL;

  iterator begin() {
    return data_.begin();
  }
  iterator end() {
    return data_.end();
  }
  iterator find(const key_type& key) {
    return data_.find(key);
  }
  mapped_type& at(const key_type& key) {
    return data_.at(key);
  }
  val_type& append(const key_type& key) {
    auto& opt = (data_[key] = folly::make_optional(val_type{}));
    return *opt; // todo(youknowjack): avoid presence check here
  }
  folly::Optional<val_type>& appendNullable(const key_type& key) {
    return data_[key] = {};
  }
  std::pair<iterator, bool> emplace(
      const key_type& key,
      folly::Optional<val_type> value) {
    return data_.emplace(key, std::move(value));
  }
  void clear() {
    data_.clear();
  }
  void reserve(typename container_t::size_type n) {
    data_.reserve(n);
  }

  const_iterator begin() const {
    return data_.begin();
  }
  const_iterator end() const {
    return data_.end();
  }
  const_iterator find(const key_type& key) const {
    return data_.find(key);
  }
  bool contains(const key_type& key) const {
    return data_.find(key) != end();
  }
  const mapped_type& at(const key_type& key) const {
    return data_.at(key);
  }
  size_t size() const {
    return data_.size();
  }

 private:
  container_t data_;
};

template <typename KEY, typename VAL>
class SlowMapVal : public IMapVal<KEY, VAL> {};

template <typename KEY, typename VAL>
class SlowMapWriter : public IMapVal<KEY, VAL> {
 public:
  SlowMapWriter& operator=(const IMapVal<KEY, VAL>& rh) {
    IMapVal<KEY, VAL>::clear();
    for (const auto& it : rh) {
      IMapVal<KEY, VAL>::emplace(it.first, folly::Optional<VAL>(it.second));
    }
    return *this;
  }
};

namespace detail {
template <typename T, typename = void>
struct DynamicResolver {
  using in_type = typename CppToType<T>::NativeType;
  using out_type = typename CppToType<T>::NativeType;

  static variant toVariant(const in_type& /* t */) {
    VELOX_NYI();
  }
};

template <typename T>
struct DynamicResolver<T, std::enable_if_t<CppToType<T>::isFixedWidth, void>> {
  using in_type = typename CppToType<T>::NativeType;
  using out_type = typename CppToType<T>::NativeType;

  static variant toVariant(const typename CppToType<T>::NativeType& t) {
    return variant::create<T>(t);
  }

  static in_type fromVariant(const variant& v) {
    return v.value<T>();
  }
};

template <typename K, typename V>
struct DynamicResolver<Map<K, V>, void> {
  using in_type = SlowMapVal<
      typename DynamicResolver<K>::in_type,
      typename DynamicResolver<V>::in_type>;
  using out_type = SlowMapWriter<
      typename DynamicResolver<K>::out_type,
      typename DynamicResolver<V>::out_type>;

  static variant toVariant(const in_type& /* t */) {
    VELOX_NYI();
  }

  static variant toVariant(const out_type& /* t */) {
    VELOX_NYI();
  }

  static in_type fromVariant(const variant& /* t */) {
    VELOX_NYI();
  }
};

template <typename V>
struct DynamicResolver<Array<V>, void> {
  using in_type = ArrayValReader<typename DynamicResolver<V>::in_type>;
  using out_type = ArrayValWriter<typename DynamicResolver<V>::out_type>;

  static variant toVariant(const in_type& t) {
    using childType = typename DynamicResolver<V>::in_type;
    std::vector<variant> v(t.size());
    std::transform(
        t.begin(), t.end(), v.begin(), [](const folly::Optional<childType>& v) {
          return v.hasValue()
              ? DynamicResolver<V>::toVariant(v.value())
              : variant::null(UdfToType<V>::koskiType()->kind());
        });

    return variant::array(std::move(v));
  }

  static variant toVariant(const out_type& t) {
    using childType = typename DynamicResolver<V>::out_type;
    std::vector<variant> v(t.size());
    std::transform(
        t.begin(), t.end(), v.begin(), [](const folly::Optional<childType>& v) {
          return v.hasValue()
              ? DynamicResolver<V>::toVariant(v.value())
              : variant::null(UdfToType<V>::koskiType()->kind());
        });

    return variant::array(std::move(v));
  }

  static in_type fromVariant(const variant& t) {
    using childType = typename DynamicResolver<V>::in_type;
    VELOX_CHECK_EQ(t.kind(), TypeKind::ARRAY);
    const auto& values = t.array();

    in_type retVal;
    for (auto& v : values) {
      auto convertedVal = v.isNull()
          ? folly::Optional<childType>{}
          : folly::Optional<childType>{DynamicResolver<V>::fromVariant(v)};
      retVal.append(std::move(convertedVal));
    }
    return retVal;
  }
};

template <typename... Args>
struct DynamicResolver<Row<Args...>, void> {
  using in_type = RowReader<typename DynamicResolver<Args>::in_type...>;
  using out_type = RowWriter<typename DynamicResolver<Args>::out_type...>;

  static variant toVariant(const in_type& /* t */) {
    VELOX_NYI();
  }

  static variant toVariant(const out_type& /* t */) {
    VELOX_NYI();
  }

  static in_type fromVariant(const variant& /* t */) {
    VELOX_NYI();
  }
};

template <typename T>
struct DynamicResolver<
    T,
    std::enable_if_t<
        std::is_same<T, Varchar>::value || std::is_same<T, Varbinary>::value,
        void>> {
  using in_type = StringView;
  using out_type = StringWriter;

  static variant toVariant(const in_type& t) {
    return variant::create<T>(std::string{t});
  }

  static variant toVariant(const out_type& t) {
    return variant::create<T>(std::string{t.data(), t.size()});
  }

  static in_type fromVariant(const variant& v) {
    return in_type{v.value<T>()};
  }
};

template <typename T>
struct DynamicResolver<std::shared_ptr<T>> {
  using in_type = std::shared_ptr<T>;
  using out_type = std::shared_ptr<T>;

  static variant toVariant(const in_type& t) {
    return variant::opaque(t);
  }

  static in_type fromVariant(const variant& v) {
    return v.opaque<T>();
  }
};

} // namespace detail

struct DynamicExec {
  template <typename T>
  using resolver = detail::DynamicResolver<T>;
};

} // namespace core
} // namespace velox
} // namespace facebook
