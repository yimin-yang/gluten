/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include <arrow/filesystem/filesystem.h>
#include <arrow/io/interfaces.h>
#include <arrow/memory_pool.h>
#include <arrow/record_batch.h>
#include <arrow/type.h>
#include <arrow/api.h>
#include <arrow/util/io_util.h>
#include <benchmark/benchmark.h>
#include <execinfo.h>
#include <parquet/arrow/reader.h>
#include <parquet/file_reader.h>
#include <sched.h>
#include <sys/mman.h>

#include <chrono>

#include "../velox/shuffle/VeloxSplitter.h"
#include "memory/ColumnarBatch.h"
#include "memory/VeloxColumnarBatch.h"
#include "operators/shuffle/splitter.h"
#include "utils/macros.h"
#include "shuffle/VeloxSplitter.h"
#include "velox/vector/arrow/Bridge.h"
#include "arrow/Bridge.h"
#include "ComplexVector.h"


void print_trace(void) {
  char **strings;
  size_t i, size;
  enum Constexpr {
      MAX_SIZE = 1024
  };
  void *array[MAX_SIZE];
  size = backtrace(array, MAX_SIZE);
  strings = backtrace_symbols(array, size);
  for (i = 0; i < size; i++)
    printf("    %s\n", strings[i]);
  puts("");
  free(strings);
}

using arrow::RecordBatchReader;
using arrow::Status;

using gluten::GlutenException;
using gluten::SplitOptions;
using gluten::Splitter;

namespace gluten {

    std::shared_ptr<ColumnarBatch> RecordBatch2VeloxColumnarBatch(const arrow::RecordBatch &rb) {
      ArrowArray arrowArray;
      ArrowSchema arrowSchema;
      GLUTEN_THROW_NOT_OK(arrow::ExportRecordBatch(rb, &arrowArray, &arrowSchema));
      auto vp = facebook::velox::importFromArrowAsOwner(arrowSchema, arrowArray,
                                                        gluten::GetDefaultWrappedVeloxMemoryPool());
      return std::make_shared<VeloxColumnarBatch>(std::dynamic_pointer_cast<facebook::velox::RowVector>(vp));
    }

    std::shared_ptr<ColumnarBatch> setUpVeloxColumnarBatch(uint32_t row_num) {
      std::vector<std::shared_ptr<arrow::Field>> fields = {
              arrow::field("f_int8_a", arrow::int8()),
              arrow::field("f_int8_b", arrow::int8()),
              arrow::field("f_int32", arrow::int32()),
              arrow::field("f_int64", arrow::int64()),
              arrow::field("f_double", arrow::float64()),
              arrow::field("f_bool", arrow::boolean()),
              arrow::field("f_string", arrow::utf8())};

      std::shared_ptr<arrow::Schema> schema_ = arrow::schema(fields);

      srand(time(0));

      arrow::Int8Builder int8Builder;
      auto *col0 = new int8_t[row_num];
      for (int i = 0; i < row_num; i++) {
        col0[i] = (int8_t) rand();
      }
      GLUTEN_THROW_NOT_OK(int8Builder.AppendValues(col0, row_num));
      GLUTEN_ASSIGN_OR_THROW(std::shared_ptr<arrow::Array> array_col0, int8Builder.Finish());


      auto *col1 = new int8_t[row_num];
      for (int i = 0; i < row_num; i++) {
        col1[i] = (int8_t) rand();
      }
      GLUTEN_THROW_NOT_OK(int8Builder.AppendValues(col1, row_num));
      GLUTEN_ASSIGN_OR_THROW(std::shared_ptr<arrow::Array> array_col1, int8Builder.Finish());


      arrow::Int32Builder int32Builder;
      auto *col2 = new int32_t[row_num];
      for (int i = 0; i < row_num; i++) {
        col2[i] = (int32_t) rand();
      }
      GLUTEN_THROW_NOT_OK(int32Builder.AppendValues(col2, row_num));
      GLUTEN_ASSIGN_OR_THROW(std::shared_ptr<arrow::Array> array_col2, int32Builder.Finish());


      arrow::Int64Builder int64Builder;
      auto *col3 = new int64_t[row_num];
      for (int i = 0; i < row_num; i++) {
        col3[i] = (int64_t) rand();
      }
      GLUTEN_THROW_NOT_OK(int64Builder.AppendValues(col3, row_num));
      GLUTEN_ASSIGN_OR_THROW(std::shared_ptr<arrow::Array> array_col3, int64Builder.Finish());


      arrow::DoubleBuilder doubleBuilder;
      auto *col4 = new double_t[row_num];
      for (int i = 0; i < row_num; i++) {
        col4[i] = (double_t) rand();
      }
      GLUTEN_THROW_NOT_OK(doubleBuilder.AppendValues(col4, row_num));
      GLUTEN_ASSIGN_OR_THROW(std::shared_ptr<arrow::Array> array_col4, doubleBuilder.Finish());


      arrow::BooleanBuilder booleanBuilder;
      auto *col5 = new uint8_t[row_num];
      for (int i = 0; i < row_num; i++) {
        col5[i] = (bool) (rand() % 2);
      }
      GLUTEN_THROW_NOT_OK(booleanBuilder.AppendValues(col5, row_num));
      GLUTEN_ASSIGN_OR_THROW(std::shared_ptr<arrow::Array> array_col5, booleanBuilder.Finish())

      arrow::StringBuilder stringBuilder;
      char **col6 = new char *[row_num];
      for (int i = 0; i < row_num; i++) {
        int length = rand() % 1024;
        col6[i] = new char[length + 1];
        for (int j = 0; j < length; j++) {
          if (rand() % 2) {
            col6[i][j] = 'A' + rand() % 26;
          } else {
            col6[i][j] = 'a' + rand() % 26;
          }
          col6[i][length] = '\0';
        }
      }
      GLUTEN_THROW_NOT_OK(stringBuilder.AppendValues((const char **) col6, row_num));
      GLUTEN_ASSIGN_OR_THROW(std::shared_ptr<arrow::Array> array_col6, stringBuilder.Finish())

      std::vector<std::shared_ptr<arrow::Array>> array_list = {array_col0, array_col1, array_col2, array_col3,
                                                               array_col4, array_col5, array_col6};
      auto rb = arrow::RecordBatch::Make(schema_, row_num, array_list);
      return RecordBatch2VeloxColumnarBatch(*rb);
    }

#define ALIGNMENT 2 * 1024 * 1024

    const int split_buffer_size = 8192;


// #define ENABLELARGEPAGE

    class LargePageMemoryPool : public arrow::MemoryPool {
    public:
        explicit LargePageMemoryPool() {}

        ~LargePageMemoryPool() override = default;

        Status Allocate(int64_t size, int64_t alignment, uint8_t **out) override {
#ifdef ENABLELARGEPAGE
          if (size < 2 * 1024 * 1024) {
            return pool_->Allocate(size, out);
          } else {
            Status st = pool_->AlignAllocate(size, out, ALIGNMENT);
            madvise(*out, size, /*MADV_HUGEPAGE */ 14);
            //std::cout << "Allocate: size = " << size << " addr = "  \
                << std::hex << (uint64_t)*out  << " end = " << std::hex << (uint64_t)(*out+size) << std::dec << std::endl;
            return st;
          }
#else
          return pool_->Allocate(size, out);
#endif
        }

        Status Reallocate(int64_t old_size, int64_t new_size, int64_t alignment, uint8_t **ptr) override {
          return pool_->Reallocate(old_size, new_size, ptr);
#ifdef ENABLELARGEPAGE
          if (new_size < 2 * 1024 * 1024) {
            return pool_->Reallocate(old_size, new_size, ptr);
          } else {
            Status st = pool_->AlignReallocate(old_size, new_size, ptr, ALIGNMENT);
            madvise(*ptr, new_size, /*MADV_HUGEPAGE */ 14);
            return st;
          }
#else
          return pool_->Reallocate(old_size, new_size, ptr);
#endif
        }

        void Free(uint8_t *buffer, int64_t size, int64_t alignment) override {
#ifdef ENABLELARGEPAGE
          if (size < 2 * 1024 * 1024) {
            pool_->Free(buffer, size);
          } else {
            pool_->Free(buffer, size, ALIGNMENT);
          }
#else
          pool_->Free(buffer, size);
#endif
        }

        int64_t bytes_allocated() const override {
          return pool_->bytes_allocated();
        }

        int64_t max_memory() const override {
          return pool_->max_memory();
        }

        std::string backend_name() const override {
          return "LargePageMemoryPool";
        }

    private:
        MemoryPool *pool_ = arrow::default_memory_pool();
    };

    class BenchmarkSplitter {
    public:
        std::shared_ptr<Splitter> splitter;
        parquet::ArrowReaderProperties properties;
        std::shared_ptr<ColumnarBatch> cb;

        BenchmarkSplitter(std::shared_ptr<ColumnarBatch> cb) {
          this->cb = cb;
        }

        void operator()(benchmark::State &state) {
          // SetCPU(state.thread_index());
          arrow::Compression::type compression_type = (arrow::Compression::type) state.range(1);

          std::shared_ptr<arrow::MemoryPool> pool = std::make_shared<LargePageMemoryPool>();

          const int num_partitions = state.range(0);

          auto options = SplitOptions::Defaults();
          options.compression_type = compression_type;
          options.buffer_size = split_buffer_size;
          options.buffered_write = true;
          options.offheap_per_task = 128 * 1024 * 1024 * 1024L;
          options.prefer_spill = true;
          options.write_schema = false;
          options.memory_pool = pool;

          int64_t split_time = 0;
          auto start_time = std::chrono::steady_clock::now();
          Do_Split(split_time, num_partitions, options, state);
          auto end_time = std::chrono::steady_clock::now();
          auto total_time = (end_time - start_time).count();

          auto fs = std::make_shared<arrow::fs::LocalFileSystem>();
          GLUTEN_THROW_NOT_OK(fs->DeleteFile(splitter->DataFile()));

          state.SetBytesProcessed(int64_t(splitter->RawPartitionBytes()));

          state.counters["num_partitions"] =
                  benchmark::Counter(num_partitions, benchmark::Counter::kAvgThreads,
                                     benchmark::Counter::OneK::kIs1000);
          state.counters["split_buffer_size"] =
                  benchmark::Counter(split_buffer_size, benchmark::Counter::kAvgThreads,
                                     benchmark::Counter::OneK::kIs1024);
          state.counters["bytes_spilled"] = benchmark::Counter(
                  splitter->TotalBytesSpilled(), benchmark::Counter::kAvgThreads,
                  benchmark::Counter::OneK::kIs1024);
          state.counters["bytes_written"] = benchmark::Counter(
                  splitter->TotalBytesWritten(), benchmark::Counter::kAvgThreads,
                  benchmark::Counter::OneK::kIs1024);
          state.counters["bytes_raw"] = benchmark::Counter(
                  splitter->RawPartitionBytes(), benchmark::Counter::kAvgThreads,
                  benchmark::Counter::OneK::kIs1024);
          state.counters["bytes_spilled"] = benchmark::Counter(
                  splitter->TotalBytesSpilled(), benchmark::Counter::kAvgThreads,
                  benchmark::Counter::OneK::kIs1024);
          state.counters["write_time"] = benchmark::Counter(
                  splitter->TotalWriteTime(), benchmark::Counter::kAvgThreads, benchmark::Counter::OneK::kIs1000);
          state.counters["spill_time"] = benchmark::Counter(
                  splitter->TotalSpillTime(), benchmark::Counter::kAvgThreads, benchmark::Counter::OneK::kIs1000);
          state.counters["compress_time"] = benchmark::Counter(
                  splitter->TotalCompressTime(), benchmark::Counter::kAvgThreads,
                  benchmark::Counter::OneK::kIs1000);
          split_time =
                  split_time - splitter->TotalSpillTime() - splitter->TotalCompressTime() -
                  splitter->TotalWriteTime();
          state.counters["split_time"] =
                  benchmark::Counter(split_time, benchmark::Counter::kAvgThreads, benchmark::Counter::OneK::kIs1000);
          state.counters["total_time"] =
                  benchmark::Counter(total_time, benchmark::Counter::kAvgThreads, benchmark::Counter::OneK::kIs1000);
          splitter.reset();
        }

        long SetCPU(uint32_t cpuindex) {
          cpu_set_t cs;
          CPU_ZERO(&cs);
          CPU_SET(cpuindex, &cs);
          return sched_setaffinity(0, sizeof(cs), &cs);
        }

        void Do_Split(
                int64_t &split_time,
                const int num_partitions,
                SplitOptions options,
                benchmark::State &state) {
          GLUTEN_ASSIGN_OR_THROW(splitter, Splitter::Make("rr", num_partitions, std::move(options)));
          for (auto _: state) {
            TIME_NANO_OR_THROW(split_time, splitter->Split(cb.get()));
          }
          TIME_NANO_OR_THROW(split_time, splitter->Stop());
        }
    };

    class BenchmarkVeloxSplitter {
    public:
        std::shared_ptr<VeloxSplitter> splitter;
        parquet::ArrowReaderProperties properties;
        std::shared_ptr<ColumnarBatch> cb;

        BenchmarkVeloxSplitter(std::shared_ptr<ColumnarBatch> cb) {
          this->cb = cb;
        }

        void operator()(benchmark::State &state) {
          // SetCPU(state.thread_index());
          arrow::Compression::type compression_type = (arrow::Compression::type) state.range(1);

          std::shared_ptr<arrow::MemoryPool> pool = std::make_shared<LargePageMemoryPool>();

          const int num_partitions = state.range(0);

          auto options = SplitOptions::Defaults();
          options.compression_type = compression_type;
          options.buffer_size = split_buffer_size;
          options.buffered_write = true;
          options.offheap_per_task = 128 * 1024 * 1024 * 1024L;
          options.prefer_spill = true;
          options.write_schema = false;
          options.memory_pool = pool;

          int64_t split_time = 0;
          auto start_time = std::chrono::steady_clock::now();
          Do_Split(split_time, num_partitions, options, state);
          auto end_time = std::chrono::steady_clock::now();
          auto total_time = (end_time - start_time).count();

          auto fs = std::make_shared<arrow::fs::LocalFileSystem>();
          GLUTEN_THROW_NOT_OK(fs->DeleteFile(splitter->DataFile()));

          state.SetBytesProcessed(int64_t(splitter->RawPartitionBytes()));

          state.counters["num_partitions"] =
                  benchmark::Counter(num_partitions, benchmark::Counter::kAvgThreads,
                                     benchmark::Counter::OneK::kIs1000);
          state.counters["split_buffer_size"] =
                  benchmark::Counter(split_buffer_size, benchmark::Counter::kAvgThreads,
                                     benchmark::Counter::OneK::kIs1024);
          state.counters["bytes_spilled"] = benchmark::Counter(
                  splitter->TotalBytesSpilled(), benchmark::Counter::kAvgThreads,
                  benchmark::Counter::OneK::kIs1024);
          state.counters["bytes_written"] = benchmark::Counter(
                  splitter->TotalBytesWritten(), benchmark::Counter::kAvgThreads,
                  benchmark::Counter::OneK::kIs1024);
          state.counters["bytes_raw"] = benchmark::Counter(
                  splitter->RawPartitionBytes(), benchmark::Counter::kAvgThreads,
                  benchmark::Counter::OneK::kIs1024);
          state.counters["bytes_spilled"] = benchmark::Counter(
                  splitter->TotalBytesSpilled(), benchmark::Counter::kAvgThreads,
                  benchmark::Counter::OneK::kIs1024);
          state.counters["write_time"] = benchmark::Counter(
                  splitter->TotalWriteTime(), benchmark::Counter::kAvgThreads, benchmark::Counter::OneK::kIs1000);
          state.counters["spill_time"] = benchmark::Counter(
                  splitter->TotalSpillTime(), benchmark::Counter::kAvgThreads, benchmark::Counter::OneK::kIs1000);
          state.counters["compress_time"] = benchmark::Counter(
                  splitter->TotalCompressTime(), benchmark::Counter::kAvgThreads,
                  benchmark::Counter::OneK::kIs1000);
          split_time =
                  split_time - splitter->TotalSpillTime() - splitter->TotalCompressTime() -
                  splitter->TotalWriteTime();
          state.counters["split_time"] =
                  benchmark::Counter(split_time, benchmark::Counter::kAvgThreads, benchmark::Counter::OneK::kIs1000);
          state.counters["total_time"] =
                  benchmark::Counter(total_time, benchmark::Counter::kAvgThreads, benchmark::Counter::OneK::kIs1000);
          splitter.reset();
        }

        long SetCPU(uint32_t cpuindex) {
          cpu_set_t cs;
          CPU_ZERO(&cs);
          CPU_SET(cpuindex, &cs);
          return sched_setaffinity(0, sizeof(cs), &cs);
        }

        void Do_Split(
                int64_t &split_time,
                const int num_partitions,
                SplitOptions options,
                benchmark::State &state) {
          GLUTEN_ASSIGN_OR_THROW(splitter, VeloxSplitter::Make("rr", num_partitions, std::move(options)));
          for (auto _: state) {
            TIME_NANO_OR_THROW(split_time, splitter->Split(cb.get()));
          }
          TIME_NANO_OR_THROW(split_time, splitter->Stop());
        }
    };


} // namespace gluten

int main(int argc, char **argv) {
  uint32_t iterations = 1;
  uint32_t partitions = 192;
  uint32_t threads = 1;
  uint32_t row_num = 1;
  std::string datafile;
  auto compression_codec = arrow::Compression::LZ4_FRAME;

  for (int i = 0; i < argc; i++) {
    if (strcmp(argv[i], "--iterations") == 0) {
      iterations = atol(argv[i + 1]);
    } else if (strcmp(argv[i], "--partitions") == 0) {
      partitions = atol(argv[i + 1]);
    } else if (strcmp(argv[i], "--threads") == 0) {
      threads = atol(argv[i + 1]);
    } else if (strcmp(argv[i], "--row_num") == 0) {
      row_num = atol(argv[i + 1]);
    } else if (strcmp(argv[i], "--qat") == 0) {
      compression_codec = arrow::Compression::GZIP;
    }
  }
  std::cout << "iterations = " << iterations << std::endl;
  std::cout << "partitions = " << partitions << std::endl;
  std::cout << "threads = " << threads << std::endl;
  std::cout << "row_num = " << row_num << std::endl;

  auto cb = gluten::setUpVeloxColumnarBatch(row_num);
  gluten::BenchmarkSplitter bck1(cb);
  gluten::BenchmarkVeloxSplitter bck2(cb);

  benchmark::RegisterBenchmark("BenchmarkSplitter", bck1)
          ->Iterations(iterations)
          ->Args({
                         partitions,
                         compression_codec,
                 })
          ->Threads(threads)
          ->ReportAggregatesOnly(false)
          ->MeasureProcessCPUTime()
          ->Unit(benchmark::kSecond);

  benchmark::RegisterBenchmark("BenchmarkVeloxSplitter", bck2)
          ->Iterations(iterations)
          ->Args({
                         partitions,
                         compression_codec,
                 })
          ->Threads(threads)
          ->ReportAggregatesOnly(false)
          ->MeasureProcessCPUTime()
          ->Unit(benchmark::kSecond);

  benchmark::Initialize(&argc, argv);
  benchmark::RunSpecifiedBenchmarks();
  benchmark::Shutdown();
}
