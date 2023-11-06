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
#if ENABLE_CONCEPTS
#include <concepts> // @manual
#endif // concept

#include <tuple>
#include <utility>
#include "velox/expression/Expr.h"

namespace facebook {
namespace velox {
namespace codegen {

/// Expected generated code format.
/// struct  Generated1 {
///  using veloxInputType  = std::tuple<DoubleType ,DoubleType>;
///  using veloxOutputType = std::tuple<DoubleType>;
///  using State = int;
///  template<typename IN,typename OUT>
///  void operator()(IN  && input,
///                  OUT &&  output){
///    std::get<0>(output) = std::get<0>(input) + std::get<1>(input);
///  }
//};

template <typename TUPLE>
struct refTuple {};
template <typename... T>
struct refTuple<std::tuple<T...>> {
  using type = std::tuple<std::add_lvalue_reference_t<T>...>;
};
template <typename TUPLE>
using refTuple_t = typename refTuple<TUPLE>::type;

#if ENABLE_CONCEPTS
template <typename GeneratedExpression>
concept compiledExpression =
    requires(GeneratedExpression& generatedExpression) {
  typename veloxInputType = std::tuple<DoubleType, DoubleType>;
  typename veloxOutputType = std::tuple<DoubleType>;
  std::constructible_from<
      GeneratedExpression,
      const typename GeneratedExpression::State&>;
  // TOOD: this should be templated function

  requires(
      typename GeneratedExpression::inputTupleType & input,
      typename GeneratedExpression::outputTupleType & output) {
    { generatedExpression(input, output) } -> std::same_as<void>;
  };
};
#endif // concept

/// Given a sequence of compiled expressions, this class represent the
/// concatenation in order of those expressions.
/// See test classes for use samples
/// \tparam veloxInputType_ ImplType input (eg DoubleType)
/// \tparam veloxOutputType_ ImplType input (eg DoubleType)
/// \tparam T compiled expression classes. Each T has the shape of
///         std::tuple<CompiledExprression,
///                    std::index_sequence,
///                    std::index_sequence>
///        where the std::index_sequences represent the mapping between the
///        input and output of T and the concatenation of all Ts

template <
    bool hasFilter_,
    typename VeloxInputType_,
    typename VeloxOutputType_,
    typename... T> // T =
                   // std::tuple<GeneratedExpression,std::index_sequence,std::index_sequence>
#if ENABLE_CONCEPTS
requires(compiledExpression<T>&&...) // All the type parameters
                                     // are GeneratedExpression
#endif
    class ConcatExpression {
 public:
  template <typename TYPE>
  using GeneratedCode = std::tuple_element_t<0, TYPE>;

  // InputMap<T> = std::index_sequence<a,b,c,d,...> where
  // where columns a is mapped to the first input of T, b to the second etc...
  template <typename TYPE>
  using InputMap = std::tuple_element_t<1, TYPE>;

  // OutputMap<T> = std::index_sequence<a,b,c,d,...> where T produces the
  // output columns a,b,... of the final concatenation.
  template <typename TYPE>
  using OutputMap = std::tuple_element_t<2, TYPE>;

  // Transforms arg packs into a std::tuple
  using TemplateTupleHelper = std::tuple<T...>;

  template <size_t index>
  using GeneratedCodeAtIndex =
      GeneratedCode<std::tuple_element_t<index, TemplateTupleHelper>>;

  // InputMap of the  index'th expression being concatenated
  template <size_t index>
  using InputMapAtIndex =
      InputMap<std::tuple_element_t<index, TemplateTupleHelper>>;

  // OutputMap of the  index'th expression being concatenated
  template <size_t index>
  using OutputMapAtIndex =
      OutputMap<std::tuple_element_t<index, TemplateTupleHelper>>;

  // The state of the concatenation is simply the concatenation of states.
  using State = std::tuple<typename GeneratedCode<T>::State...>;

  using VeloxInputType = VeloxInputType_;
  using VeloxOutputType = VeloxOutputType_;

  static constexpr bool hasFilter = hasFilter_;

  /// TODO: It's important that this function and helper be inline
  template <typename InputTuple, typename OutputTuple>
  inline bool operator()(InputTuple&& input, OutputTuple&& output) {
    if constexpr (hasFilter) {
      std::tuple<std::optional<bool>> filterOutput({});
      this->helperFilter(input, filterOutput, std::index_sequence<0>{});
      auto passed = std::get<0>(filterOutput);
      if (!passed || !*passed) {
        return false;
      }
      helperFromInd1(input, output, std::make_index_sequence<sizeof...(T)>{});
      return true;
    } else {
      helper(input, output, std::make_index_sequence<sizeof...(T)>{});
      return true;
    }
  }

 private:
  void resetAllocators() {
    resetAllocators_(std::make_index_sequence<sizeof...(T)>{});
  }

  template <typename InputTuple, typename OutputTuple, std::size_t... Indices>
  inline void helper(
      InputTuple&& input,
      OutputTuple&& output,
      [[maybe_unused]] const std::index_sequence<Indices...>& unused) {
    (std::get<Indices>(expressions)(
         helperGetMany(input, InputMapAtIndex<Indices>{}),
         helperGetMany(output, OutputMapAtIndex<Indices>{})),
     ...);
  }

  template <typename InputTuple, typename OutputTuple, std::size_t... Indices>
  inline void helperFilter(
      InputTuple&& input,
      OutputTuple&& output,
      [[maybe_unused]] const std::index_sequence<Indices...>& unused) {
    (std::get<Indices>(expressions)(
         helperGetMany(input, InputMapAtIndex<Indices>{}), output),
     ...);
  }

  template <typename InputTuple, typename OutputTuple, std::size_t... Indices>
  inline void helperFromInd1(
      InputTuple&& input,
      OutputTuple&& output,
      [[maybe_unused]] const std::index_sequence<0, Indices...>& unused) {
    this->helper(input, output, std::index_sequence<Indices...>{});
  }

  std::tuple<GeneratedCode<T>...> expressions;
  template <size_t... Indices>
  void resetAllocators_(const std::index_sequence<Indices...>&) {
    (std::get<Indices>(expressions).state.allocator.reset(), ...);
  }

  /// Give a tuple T and an index_sequence return a new tuple with
  /// the elements from T at index in the  index_sequnce.
  /// eg (a,b,c,d,e), (2,4) -> (c,e)
  /// \tparam TUPLE Input tuple type
  /// \tparam Index Index of element to extracts
  /// \param tuple
  /// \param unused
  /// \return new tuple
  template <typename TupleType, size_t... Index>
  inline auto helperGetMany(
      TupleType&& tuple,
      [[maybe_unused]] const std::index_sequence<Index...>& unused) {
    return std::forward_as_tuple(
        std::get<Index>(std::forward<TupleType>(tuple))...);
  }
};

} // namespace codegen
} // namespace velox
}; // namespace facebook
