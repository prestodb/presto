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

/*
 * Copyright (c) 2024 by Rivos Inc.
 * Licensed under the Apache License, Version 2.0, see LICENSE for details.
 * SPDX-License-Identifier: Apache-2.0
 */

#include <algorithm>
#include <fstream>
#include <iostream>
#include <limits>
#include <memory>
#include <numeric>
#include <random>
#include <regex>
#include <string>

#include "breeze/platforms/cuda.cuh"
#include "breeze/utils/trace.h"

namespace breeze {

const char* kThroughputInfoTemplate = R""""([
  {
    "name": "element_count",
    "unit": "element",
    "value": ELEMENT_COUNT
  },
  {
    "name": "global_memory_reads",
    "unit": "byte",
    "value": GLOBAL_MEMORY_READS
  },
  {
    "name": "global_memory_writes",
    "unit": "byte",
    "value": GLOBAL_MEMORY_WRITES
  }
])"""";

// LogFormat enumerates alternative methods for generating logs
enum LogFormat {
  HUMAN_READABLE,
  CSV,
  OPEN_METRICS,
};

// LogFormat istream `operator>>` implementation for `from_string`
inline std::istream& operator>>(std::istream& is, LogFormat& format) {
  std::string format_name;
  is >> format_name;
  std::transform(format_name.begin(), format_name.end(), format_name.begin(),
                 ::toupper);
  if (strcmp(format_name.c_str(), "CSV") == 0) {
    format = CSV;
  } else if (strcmp(format_name.c_str(), "OPEN_METRICS") == 0) {
    format = OPEN_METRICS;
  } else {
    format = HUMAN_READABLE;
  }
  return is;
}

// convert string to snake case
inline std::string ToSnakeCase(std::string in) {
  std::string s;
  std::for_each(in.begin(), in.end(), [&s](const char& c) {
    if (std::isupper(c)) {
      if (!s.empty() && std::isalnum(s.back())) {
        s.push_back('_');
      }
      s.push_back(std::tolower(c));
    } else {
      s.push_back(c);
    }
  });
  return s;
}

// global variable to track the log output index
static int g_log_index = 0;

template <typename ConfigType>
class PerfTest {
 public:
  template <typename SetupFunction, typename TestFunction,
            typename Logger = PerfTest<ConfigType>>
  void MeasureWithSetup(const ConfigType& config,
                        SetupFunction&& setup_function,
                        TestFunction&& test_function) {
    int num_runs = config.get("num_runs", 1);
    int num_warmup_runs = config.get("num_warmup_runs", 1);
    LogFormat log_format = config.get("log_format", HUMAN_READABLE);
    std::string output_file = config.get("output_file", std::string());
    bool l2_flush = config.get("l2_flush", true);

    // determine the size of the L2 cache if a flush is needed
    int l2_size = 0;
    if (l2_flush) {
      int dev_id = 0;
      cudaGetDevice(&dev_id);
      cudaDeviceGetAttribute(&l2_size, cudaDevAttrL2CacheSize, dev_id);
    }

    // allocate buffer used to flush L2 cache
    void* l2_buffer = nullptr;
    if (l2_size) {
      cudaMalloc(&l2_buffer, l2_size);
    }

    // create throughput info string used for trace events
    auto throughput_info = std::regex_replace(
        std::regex_replace(std::regex_replace(kThroughputInfoTemplate,
                                              std::regex("\\ELEMENT_COUNT"),
                                              std::to_string(element_count_)),
                           std::regex("\\GLOBAL_MEMORY_READS"),
                           std::to_string(global_memory_load_bytes_)),
        std::regex("\\GLOBAL_MEMORY_WRITES"),
        std::to_string(global_memory_store_bytes_));
    float time_ms = .0f;
    {
      if (num_warmup_runs) {
        TRACE_EVENT("test", "Warmup", "num_warmup_runs", num_warmup_runs,
                    "kernel_throughput_info", throughput_info);

        for (int i = 0; i < num_warmup_runs; ++i) {
          setup_function();
          test_function();
        }

        cudaDeviceSynchronize();
      }

      if (num_runs) {
        TRACE_EVENT("test", "Run", "num_runs", num_runs,
                    "kernel_throughput_info", throughput_info);

        // initialize all events
        std::vector<cudaEvent_t> starts(num_runs), stops(num_runs);
        for (int i = 0; i < num_runs; ++i) {
          cudaEventCreate(&starts[i]);
          cudaEventCreateWithFlags(&stops[i], cudaEventBlockingSync);
        }

        for (int i = 0; i < num_runs; ++i) {
          setup_function();

          // flush L2 cache using a memset
          if (l2_buffer) {
            cudaMemsetAsync(l2_buffer, 0, l2_size);
          }

          cudaEventRecord(starts[i], 0);
          test_function();
          cudaEventRecord(stops[i], 0);
        }

        // wait for last run to complete
        cudaEventSynchronize(stops[num_runs - 1]);

        // add up time for all runs
        for (int i = 0; i < num_runs; ++i) {
          float run_ms;
          cudaEventElapsedTime(&run_ms, starts[i], stops[i]);
          time_ms += run_ms;
        }
      }
    }

    if (l2_buffer) {
      cudaFree(l2_buffer);
    }

    if (output_file.empty()) {
      Logger::Log(std::cout, log_format, /*log_index=*/0, time_ms, num_runs,
                  num_warmup_runs, element_count_, element_size_,
                  elements_per_thread_, global_memory_load_bytes_,
                  global_memory_store_bytes_);
    } else {
      // use config value to determine if first log should append, subsequent
      // logging will always append
      bool append =
          g_log_index == 0 ? config.get("output_file_append", false) : true;
      std::ofstream out;
      if (append) {
        out.open(output_file, std::ofstream::out | std::ofstream::app);
        // advance initial log index if we're appending to a non-empty file
        if (g_log_index == 0) {
          out.seekp(0, std::ios::end);
          if (out.tellp() != 0) {
            g_log_index += 1;
          }
        }
      } else {
        out.open(output_file, std::ofstream::out);
      }
      ASSERT_TRUE(out.is_open())
          << "failed to open output file: " << output_file;
      Logger::Log(out, log_format, g_log_index++, time_ms, num_runs,
                  num_warmup_runs, element_count_, element_size_,
                  elements_per_thread_, global_memory_load_bytes_,
                  global_memory_store_bytes_);
      out.close();
    }
  }

  template <typename TestFunction, typename Logger = PerfTest<ConfigType>>
  void Measure(const ConfigType& config, TestFunction&& test_function) {
    MeasureWithSetup(config, []() {}, test_function);
  }

  // default log implementation
  static void Log(std::ostream& out, LogFormat format, int log_index,
                  float time_ms, int num_runs, int num_warmup_runs,
                  int64_t element_count, int64_t element_size,
                  int64_t elements_per_thread, int64_t global_memory_load_bytes,
                  int64_t global_memory_store_bytes) {
    switch (format) {
      case HUMAN_READABLE: {
        constexpr double kG = 1024.0 * 1024.0 * 1024.0;
        constexpr double kNsPerMs = 1000.0 * 1000.0;

        double items_per_ns = static_cast<double>(element_count) /
                              (time_ms * kNsPerMs) * num_runs;
        out << num_runs << " run(s) in " << time_ms << " ms (" << items_per_ns
            << " elements/ns";
        if (global_memory_load_bytes || global_memory_store_bytes) {
          double GBs = static_cast<double>(global_memory_load_bytes +
                                           global_memory_store_bytes) /
                       kG * num_runs;
          double secs = time_ms / 1000.0;

          out << ", " << GBs / secs << " GB/s";
        }
        out << ")" << std::endl;
      } break;
      case CSV:
        if (log_index == 0) {
          // first row of headers
          out << "time(ms),num_runs,num_warmup_runs,element_count,element_size"
                 ",elements_per_thread,global_memory_load_bytes,"
                 "global_memory_store_bytes"
              << std::endl;
        }
        // row of data
        out << time_ms << "," << num_runs << "," << num_warmup_runs << ","
            << element_count << "," << element_size << ","
            << elements_per_thread << "," << global_memory_load_bytes << ","
            << global_memory_store_bytes << std::endl;
        break;
      case OPEN_METRICS: {
        auto test_info =
            ::testing::UnitTest::GetInstance()->current_test_info();
        std::stringstream ss;
        ss << test_info->test_suite_name() << ":" << test_info->name();
        std::string metric = ToSnakeCase(ss.str());
        std::replace(metric.begin(), metric.end(), '.', '_');
        std::replace(metric.begin(), metric.end(), '/', '_');
        out << metric << "_bytes " << element_count * element_size * num_runs;
        out << std::endl;
        if (global_memory_load_bytes) {
          out << metric << "global_memory_load_bytes "
              << global_memory_load_bytes * num_runs;
          out << std::endl;
        }
        if (global_memory_store_bytes) {
          out << metric << "global_memory_store_bytes "
              << global_memory_store_bytes * num_runs;
          out << std::endl;
        }
        out << metric << "_seconds " << static_cast<double>(time_ms) / 1000.0;
        out << std::endl;
      } break;
    }
  }

  // throughput information setters
  void set_element_count(int64_t element_count) {
    element_count_ = element_count;
  }
  void set_element_size(int64_t element_size) { element_size_ = element_size; }
  void set_elements_per_thread(int64_t elements_per_thread) {
    elements_per_thread_ = elements_per_thread;
  }
  template <typename T = uint8_t>
  void set_global_memory_loads(int64_t global_memory_loads) {
    global_memory_load_bytes_ = global_memory_loads * sizeof(T);
  }
  template <typename T = uint8_t>
  void set_global_memory_stores(int64_t global_memory_stores) {
    global_memory_store_bytes_ = global_memory_stores * sizeof(T);
  }

  static int MaxSharedMemory() {
    int value;
    cudaDeviceGetAttribute(&value, cudaDevAttrMaxSharedMemoryPerBlockOptin, 0);
    return value;
  }

 private:
  int64_t element_count_ = 0;
  int64_t element_size_ = 0;
  int64_t elements_per_thread_ = 0;
  int64_t global_memory_load_bytes_ = 0;
  int64_t global_memory_store_bytes_ = 0;
};

// from_string implemetation for basic types
template <typename T>
inline T from_string(const std::string& s) {
  std::istringstream iss;
  iss.str(s);
  T result;
  iss >> result;
  return result;
}

// from_env implemetation for basic types
template <typename T>
inline T from_env(const char* name, T default_value) {
  char* env = getenv(name);
  if (env) {
    return from_string<T>(std::string(env));
  }
  return default_value;
}

// GenerateMethod enumerates alternative methods for generating vector contents
enum GenerateMethod {
  FILL,
  SEQUENCE,
  RANDOM,
};

// GenerateMethod istream `operator>>` implementation for `from_string`
inline std::istream& operator>>(std::istream& is, GenerateMethod& method) {
  std::string method_name;
  is >> method_name;
  std::transform(method_name.begin(), method_name.end(), method_name.begin(),
                 ::toupper);
  if (strcmp(method_name.c_str(), "RANDOM") == 0) {
    method = RANDOM;
  } else if (strcmp(method_name.c_str(), "SEQUENCE") == 0) {
    method = SEQUENCE;
  } else {
    method = FILL;
  }
  return is;
}

// InputSize enumerates available input sizes
enum InputSize {
  SHORT,
  TALL,
  GRANDE,
  VENTI,
};

// GenerateMethod istream `operator>>` implementation for `from_string`
inline std::istream& operator>>(std::istream& is, InputSize& size) {
  std::string size_name;
  is >> size_name;
  std::transform(size_name.begin(), size_name.end(), size_name.begin(),
                 ::toupper);
  if (strcmp(size_name.c_str(), "SHORT") == 0) {
    size = SHORT;
  } else if (strcmp(size_name.c_str(), "GRANDE") == 0) {
    size = GRANDE;
  } else if (strcmp(size_name.c_str(), "VENTI") == 0) {
    size = VENTI;
  } else {
    size = TALL;
  }
  return is;
}

enum RandomEngine {
  MT19937,
  MINSTD_RAND,
};

// RandomEngine istream `operator>>` implementation for `from_string`
inline std::istream& operator>>(std::istream& is, RandomEngine& random_engine) {
  std::string random_engine_name;
  is >> random_engine_name;
  std::transform(random_engine_name.begin(), random_engine_name.end(),
                 random_engine_name.begin(), ::toupper);
  if (strcmp(random_engine_name.c_str(), "MT19937") == 0) {
    random_engine = MT19937;
  } else {
    random_engine = MINSTD_RAND;
  }
  return is;
}

template <int NUM_VALUES>
struct PerfTestArrayConfig {
  // primitive config value reader that tries to find upper-case
  // environment variable key, reads config values of that fails,
  // and then fall-backs to the provided default value
  template <typename T>
  T get(const char* key, T default_value) const {
    static bool s_verbose = from_env<bool>("VERBOSE", false);
    std::string uppercase_key = std::string(key);
    std::transform(uppercase_key.begin(), uppercase_key.end(),
                   uppercase_key.begin(), ::toupper);
    const char* env_value = getenv(uppercase_key.c_str());
    if (env_value) {
      if (s_verbose) {
        std::cout << uppercase_key << "=" << env_value << std::endl;
      }
      return from_string<T>(env_value);
    }
    auto it = std::find_if(std::begin(values), std::end(values),
                           [&key](std::pair<const char*, const char*> value) {
                             return strcmp(value.first, key) == 0;
                           });
    if (it != std::end(values)) {
      if (s_verbose) {
        std::cout << uppercase_key << "=" << it->second << std::endl;
      }
      return from_string<T>(it->second);
    }
    if (s_verbose) {
      std::cout << uppercase_key << "=" << default_value << std::endl;
    }
    return default_value;
  }

  template <typename T>
  T get_sized(const char* key, T default_value) const {
    static InputSize s_size = from_env<InputSize>("SIZE", TALL);
    T unsized_value = get(key, default_value);
    switch (s_size) {
      case SHORT: {
        auto short_key = std::string(key) + "_short";
        return get(short_key.c_str(), unsized_value);
      }
      case GRANDE: {
        auto grande_key = std::string(key) + "_grande";
        return get(grande_key.c_str(), unsized_value);
      }
      case VENTI: {
        auto venti_key = std::string(key) + "_venti";
        return get(venti_key.c_str(), unsized_value);
      }
      case TALL:
        break;
    }
    return unsized_value;
  }

  // reads a column of data and returns a host vector
  template <typename T>
  std::vector<T> get_column(const char* prefix) const {
    std::vector<T> items;

    // generate values
    {
      auto num_rows_key = "num_" + std::string(prefix) + "_rows";
      auto num_rows = get_sized(num_rows_key.c_str(), 0);
      auto generate_method_key = std::string(prefix) + "_generate_method";
      auto generate_method = get(generate_method_key.c_str(), FILL);
      switch (generate_method) {
        case FILL: {
          auto value_key = std::string(prefix) + "_fill_value";
          auto value = get<T>(value_key.c_str(), 1);
          items.assign(num_rows, value);
        } break;
        case SEQUENCE: {
          auto start_key = std::string(prefix) + "_sequence_start";
          auto start = get_sized<T>(start_key.c_str(), 1);
          auto stride_key = std::string(prefix) + "_sequence_stride";
          auto stride = get_sized<int>(stride_key.c_str(), 1);
          auto max_key = std::string(prefix) + "_sequence_max";
          auto max =
              get_sized<T>(max_key.c_str(), std::numeric_limits<T>::max());
          if (max < start) {
            std::cerr << "error: max must be greater or equal to start"
                      << std::endl;
            break;
          }
          auto shuffle_key = std::string(prefix) + "_sequence_shuffle";
          auto shuffle = get<bool>(shuffle_key.c_str(), false);
          items.resize(num_rows);
          T next = 0;
          T range = max - start + 1;
          for (int j = 0; j < num_rows; j += stride) {
            T value = start + next++ % range;
            int end = std::min(num_rows, j + stride);
            for (int i = j; i < end; ++i) {
              items[i] = value;
            }
          }
          if (shuffle) {
            static std::minstd_rand rng;
            std::shuffle(items.begin(), items.end(), rng);
          }
        } break;
        case RANDOM: {
          auto min_key = std::string(prefix) + "_random_min";
          auto min = get_sized<T>(min_key.c_str(), 1);
          auto max_key = std::string(prefix) + "_random_max";
          auto max =
              get_sized<T>(max_key.c_str(), std::numeric_limits<T>::max());
          if (max < min) {
            std::cerr << "error: max must be greater or equal to min"
                      << std::endl;
            break;
          }
          auto stride_key = std::string(prefix) + "_random_stride";
          auto stride = get_sized<int>(stride_key.c_str(), 1);
          auto shuffle_key = std::string(prefix) + "_random_shuffle";
          auto shuffle = get<bool>(shuffle_key.c_str(), false);
          std::random_device dev;
          auto seed_key = std::string(prefix) + "_random_seed";
          auto seed =
              get<std::random_device::result_type>(seed_key.c_str(), dev());
          auto random_engine_key = std::string(prefix) + "_random_engine";
          auto random_engine =
              get<RandomEngine>(random_engine_key.c_str(), MT19937);

          std::variant<std::mt19937, std::minstd_rand> rng;
          switch (random_engine) {
            case MT19937:
              rng = std::mt19937(seed);
              break;
            case MINSTD_RAND:
              rng = std::minstd_rand(seed);
              break;
          }
          std::uniform_int_distribution<std::mt19937::result_type> dist(min,
                                                                        max);
          items.resize(num_rows);
          for (int j = 0; j < num_rows; j += stride) {
            T value = static_cast<T>(
                std::visit([&dist](auto& arg) { return dist(arg); }, rng));

            int end = std::min(num_rows, j + stride);
            for (int i = j; i < end; ++i) {
              items[i] = value;
            }
          }
          if (shuffle) {
            std::visit(
                [&items](auto& arg) {
                  std::shuffle(items.begin(), items.end(), arg);
                },
                rng);
          }
        } break;
      }
    }
    return items;
  }

  std::pair<const char*, const char*> values[NUM_VALUES];
};

//
// helpers for creating and combining test types
//

template <template <typename, typename> typename T, typename TupleType,
          typename TupleParam, int I>
struct MakeTestType {
  enum {
    N = std::tuple_size<TupleParam>::value,
  };
  using type = T<typename std::tuple_element<I / N, TupleType>::type,
                 typename std::tuple_element<I % N, TupleParam>::type>;
};

template <template <typename, typename> typename T, typename TupleType,
          typename TupleParam, typename Is>
struct MakeTestTypeCombinations;

template <template <typename, typename> typename T, typename TupleType,
          typename TupleParam, size_t... Is>
struct MakeTestTypeCombinations<T, TupleType, TupleParam,
                                std::index_sequence<Is...>> {
  using tuple =
      std::tuple<typename MakeTestType<T, TupleType, TupleParam, Is>::type...>;
};

template <template <typename, typename> typename T, typename TupleTypes,
          typename... Params>
using CombineTestTypes = typename MakeTestTypeCombinations<
    T, TupleTypes, std::tuple<Params...>,
    std::make_index_sequence<(std::tuple_size<TupleTypes>::value) *
                             (sizeof...(Params))>>::tuple;

template <int M, int N>
struct LaunchParams {
  enum {
    BLOCK_THREADS = M,
    ITEMS_PER_THREAD = N,
  };

  static std::string GetName() {
    return std::to_string(BLOCK_THREADS) + "x" +
           std::to_string(ITEMS_PER_THREAD);
  }
};

template <typename T>
struct ItemType {
  using type = T;

  static std::string GetName() {
    if (std::is_same<type, char>()) return "int8";
    if (std::is_same<type, unsigned char>()) return "uint8";
    if (std::is_same<type, int>()) return "int32";
    if (std::is_same<type, unsigned>()) return "uint32";
    if (std::is_same<type, long long>()) return "int64";
    if (std::is_same<type, unsigned long long>()) return "uint64";
    if (std::is_same<type, float>()) return "float32";
    if (std::is_same<type, double>()) return "float64";
    return "?";
  }
};

template <typename LaunchParamsT, typename ItemTypeT>
struct LaunchParamsAndItemType {
  using launch_params = LaunchParamsT;
  using item_type = ItemTypeT;

  static std::string GetName() {
    return launch_params::GetName() + "." + item_type::GetName();
  }
};

template <typename... T>
struct MakeItemTypes {
  using tuple = std::tuple<ItemType<T>...>;
};

template <template <typename, typename> typename T, typename TupleTypes,
          typename... Params>
using CombineLaunchParamsAndTypes = typename MakeTestTypeCombinations<
    T, TupleTypes, typename MakeItemTypes<Params...>::tuple,
    std::make_index_sequence<(std::tuple_size<TupleTypes>::value) *
                             (sizeof...(Params))>>::tuple;

template <typename T>
struct MakeTestTypes;

template <typename... T>
struct MakeTestTypes<std::tuple<T...>> {
  using types = ::testing::Types<T...>;
};

template <typename LaunchParamsTypes, typename... T>
struct MakePerfTestTypes {
  using types = typename MakeTestTypes<CombineLaunchParamsAndTypes<
      LaunchParamsAndItemType, LaunchParamsTypes, T...>>::types;
};

struct TestTypeNames {
  template <typename T>
  static std::string GetName(int) {
    return T::GetName();
  }
};

template <typename T>
struct identity {
  using type = T;
};

template <typename T>
using try_make_signed =
    typename std::conditional<std::is_integral<T>::value, std::make_signed<T>,
                              identity<T>>::type;

template <typename T>
using try_make_unsigned =
    typename std::conditional<std::is_integral<T>::value, std::make_unsigned<T>,
                              identity<T>>::type;

//
// helpers for retrieving device details
//

template <int DEVICE_ID>
struct DeviceDetails {
  // get the number of multi-processors on the device
  static int get_multi_processor_count() {
    cudaDeviceProp device_prop;
    cudaGetDeviceProperties(&device_prop, DEVICE_ID);
    return device_prop.multiProcessorCount;
  }
};

}  // namespace breeze
