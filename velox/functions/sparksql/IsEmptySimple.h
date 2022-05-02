#include "velox/functions/Udf.h"
#include "velox/functions/lib/RegistrationHelpers.h"

namespace facebook::velox::functions::sparksql {

template <typename T>
struct IsEmpty {
  template <typename U>
  FOLLY_ALWAYS_INLINE void callNullable(bool& result, const U* a) {
    if (a) {
      result = *a == 0;
    } else {
      result = 1;
    }
  }

  VELOX_DEFINE_FUNCTION_TYPES(T);
  template <>
  FOLLY_ALWAYS_INLINE void callNullable(
      bool& result,
      const arg_type<Varchar>* a) {
    if (a) {
      result = a->empty() || strcasecmp(a->data(), "false") == 0 ||
          strcasecmp(a->data(), "null") == 0 || a->compare("[]") == 0 ||
          a->compare("{}") == 0;
    } else {
      result = 1;
    }
  }

  FOLLY_ALWAYS_INLINE void callNullable(
      bool& result,
      const arg_type<Array<Generic<>>>* a) {
    if (!a) {
      result = 1;

    } else {
      result = a->size() == 0;
    }
  }

  FOLLY_ALWAYS_INLINE void callNullable(
      bool& result,
      const arg_type<Map<Generic<>, Generic<>>>* a) {
    if (a) {
      result = a->size() == 0;
    } else {
      result = 1;
    }
  }

  FOLLY_ALWAYS_INLINE void callNullable(
      bool& result,
      const arg_type<Generic<>>* a) {
    if (!a) {
      result = 1;
      return;
    }

    if (a->kind() == TypeKind::ROW) {
      result = a->type()->asRow().size() == 0;
    } else {
      VELOX_UNREACHABLE("type is not supported");
    }
  }
};

} // namespace facebook::velox::functions::sparksql
