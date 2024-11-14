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

#include <folly/FBString.h>

#include <cstdlib>
#include <fstream>
#include <list>
#include <random>
#include <sstream>

#include <folly/Benchmark.h>
#include <folly/Random.h>
#include <folly/container/Foreach.h>
#include <folly/portability/GFlags.h>

#include <iostream>

#include "velox/common/base/SimdUtil.h"

/// Copy Part code from
/// https://github.com/facebook/folly/blob/ce5edfb9b08ead9e78cb46879e7b9499861f7cd2/folly/test/FBStringTestBenchmarks.cpp.h
using namespace std;
using namespace folly;
/// Fixed seed for stable benchmark result, simdStrStr is always faster than
/// std::find with different seeds.
static const int seed = 123456;
static std::mt19937 rng(seed);

namespace facebook::velox {
template <class Integral1, class Integral2>
Integral2 random(Integral1 low, Integral2 up) {
  std::uniform_int_distribution<> range(low, up);
  return range(rng);
}

enum ALG { SIMD, STD, KMP, BOYER_MOORE };

class KmpSearcher {
 public:
  KmpSearcher(const std::string& needle) : needle_(std::move(needle)) {
    next_ = new int[1 + needle.size()];
    initNextArr(needle);
  }

  ~KmpSearcher() {
    delete[] next_;
  }

  size_t search(const char* heyStack, size_t heyStackSize) const {
    int i = 0, j = 0;
    while ((i < (int32_t)heyStackSize) && (j < (int32_t)needle_.size())) {
      if (j == -1 || heyStack[i] == needle_[j]) {
        i++;
        j++;
      } else {
        j = next_[j];
      }
    }
    if (j >= needle_.size()) {
      return (i - needle_.size());
    };
    return (std::string::npos);
  }

 private:
  void initNextArr(const string& needle) {
    int j = 0, k = -1;
    next_[0] = -1;
    for (; j < needle.length();) {
      if (k == -1 || needle[j] == needle[k]) {
        j++;
        k++;
        next_[j] = k;
      } else
        k = next_[k];
    }
  }
  std::string needle_;
  int* next_;
};

class TestStringSearch {
 public:
  TestStringSearch(const std::string& heyStack, const std::string& needle)
      : heyStack_(std::move(heyStack)),
        needle_(std::move(needle)),
        searher_(needle_.begin(), needle_.end()),
        kmpSearcher_(needle_) {}

  template <ALG alg>
  void runSearching(size_t iters) const {
    if constexpr (alg == SIMD) {
      FOR_EACH_RANGE (i, 0, iters)
        doNotOptimizeAway(simd::simdStrstr(
            heyStack_.data(),
            heyStack_.size(),
            needle_.data(),
            needle_.size()));
    } else if constexpr (alg == STD) {
      FOR_EACH_RANGE (i, 0, iters)
        doNotOptimizeAway(
            std::string_view(heyStack_.data(), heyStack_.size())
                .find(std::string_view(needle_.data(), needle_.size())));
    } else if constexpr (alg == BOYER_MOORE) {
      FOR_EACH_RANGE (i, 0, iters)
        doNotOptimizeAway(
            std::search(heyStack_.begin(), heyStack_.end(), searher_));
    } else if constexpr (alg == KMP) {
      FOR_EACH_RANGE (i, 0, iters)
        doNotOptimizeAway(
            kmpSearcher_.search(heyStack_.data(), heyStack_.size()));
    }
  }

 private:
  std::string heyStack_;
  std::string needle_;
  std::boyer_moore_searcher<std::string::iterator> searher_;
  KmpSearcher kmpSearcher_;
};

TestStringSearch generateTest(int hayStackSize, int needleSize) {
  // Text courtesy (ahem) of
  // http://www.psychologytoday.com/blog/career-transitions/200906/
  // the-dreaded-writing-sample
  // 1028chars
  static const std::string s =
      "\
Even if you've mastered the art of the cover letter and the resume, \
another part of the job search process can trip up an otherwise \
qualified candidate: the writing sample.\n\
\n\
Strong writing and communication skills are highly sought after by \
most employers. Whether crafting short emails or lengthy annual \
reports, many workers use their writing skills every day. And for an \
employer seeking proof behind that ubiquitous candidate \
phrase,\"excellent communication skills\", a required writing sample \
is invaluable.\n\
\n\
Writing samples need the same care and attention given to cover \
letters and resumes. Candidates with otherwise impeccable credentials \
are routinely eliminated by a poorly chosen writing sample. Notice I \
said \"poorly chosen\" not \"poorly written.\" Because that's the rub: \
a writing sample not only reveals the individual's writing skills, it \
also offers a peek into what they consider important or relevant for \
the position. If you miss that mark with your writing sample, don't \
expect to get a call for an interview.";
  auto pos = random(0, s.size() - hayStackSize);
  auto needlePos = random(2, hayStackSize - needleSize);
  std::string haystack = s.substr(pos, hayStackSize);
  std::string needle = haystack.substr(needlePos, needleSize);
  return TestStringSearch(std::move(haystack), std::move(needle));
}

void findSuccessful(
    unsigned /*arg*/,
    ALG alg,
    size_t iters,
    const TestStringSearch& testdata) {
  switch (alg) {
    case KMP:
      testdata.runSearching<KMP>(iters);
      break;
    case STD:
      testdata.runSearching<STD>(iters);
      break;
    case SIMD:
      testdata.runSearching<SIMD>(iters);
      break;
    case BOYER_MOORE:
      testdata.runSearching<BOYER_MOORE>(iters);
      break;
  }
}

/// Folly uses random test data for each iteration, but this cannot guarantee
/// that the data for each test of different algorithms is the same, so we use
/// the same random data for each comparison benchmark here.
#define STRING_SEARCH_BENCHMARK(name, start, end, iters)             \
  TestStringSearch test##start##end = generateTest(start, end);      \
  BENCHMARK_NAMED_PARAM(                                             \
      name, simd_##start##_to_##end, SIMD, iters, test##start##end); \
  BENCHMARK_NAMED_PARAM(                                             \
      name, std_##start##_to_##end, STD, iters, test##start##end);   \
  BENCHMARK_NAMED_PARAM(                                             \
      name,                                                          \
      std_boyer_moore_##start##_to_##end,                            \
      BOYER_MOORE,                                                   \
      iters,                                                         \
      test##start##end);                                             \
                                                                     \
  BENCHMARK_NAMED_PARAM(                                             \
      name, kmp_##start##_to_##end, KMP, iters, test##start##end);

STRING_SEARCH_BENCHMARK(findSuccessful, 50, 5, 52428800)
STRING_SEARCH_BENCHMARK(findSuccessful, 100, 10, 52428800)
STRING_SEARCH_BENCHMARK(findSuccessful, 100, 20, 52428800)
STRING_SEARCH_BENCHMARK(findSuccessful, 1000, 10, 52428800)
STRING_SEARCH_BENCHMARK(findSuccessful, 1000, 100, 5242880)
STRING_SEARCH_BENCHMARK(findSuccessful, 1000, 200, 5242880)

/// std::find only handle fast-path for prefix-unmatch-char, if there is a
/// prefix-match-char(in practice, it is a high probability event that a
/// first char match is successful.), the performance of std::find drops
/// significantly in such a scenario.
TestStringSearch prefixMatch = {
    "luffily close dugouts wake about the pinto beans. pending, ironic dependencies",
    "b???"};

TestStringSearch prefixUnMatch = {
    "luffily close dugouts wake about the pinto beans. pending, ironic dependencies",
    "????"};
void findUnsuccessful(
    size_t /*arg*/,
    bool useStd,
    size_t iters,
    const TestStringSearch& test) {
  if (useStd) {
    test.runSearching<STD>(iters);
  } else {
    test.runSearching<SIMD>(iters);
  }
}

BENCHMARK_NAMED_PARAM(
    findUnsuccessful,
    std_first_char_match,
    true,
    52428800,
    prefixMatch)
BENCHMARK_NAMED_PARAM(
    findUnsuccessful,
    opt_first_char_match,
    false,
    52428800,
    prefixMatch)
BENCHMARK_NAMED_PARAM(
    findUnsuccessful,
    std_first_char_unmatch,
    true,
    52428800,
    prefixUnMatch)
BENCHMARK_NAMED_PARAM(
    findUnsuccessful,
    opt_first_char_unmatch,
    false,
    52428800,
    prefixUnMatch)
} // namespace facebook::velox

int main(int argc, char** argv) {
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  folly::runBenchmarks();
  return 0;
}
