# Copyright 2018 The Prometheus Authors
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

TSDB_BIN = "./tsdb"
TSDB_BENCHMARK_NUM_METRICS ?= 1000
TSDB_PROJECT_DIR = "../.."
TSDB_BENCHMARK_DATASET ?= "$(TSDB_PROJECT_DIR)/testdata/20kseries.json"
TSDB_BENCHMARK_OUTPUT_DIR ?= "./benchout"

STATICCHECK_IGNORE =
include ../../Makefile.common

# Since tsdb client yet has no tests we can at least try to run benchmark
test: bench

build:
	@$(GO) build .

bench: build
# Check if dataset exists
	@test -e $(TSDB_BENCHMARK_DATASET) >/dev/null 2>&1 || (echo "ERROR: Cannot find benchmark dataset. Specify file using the TSDB_BENCHMARK_DATASET variable"; exit 1)
	@echo ">> running benchmark"
	@$(TSDB_BIN) bench write --metrics=$(TSDB_BENCHMARK_NUM_METRICS) $(TSDB_BENCHMARK_DATASET)

bench-vizualization: bench
# Check if Graphviz is installed
	@command -v dot >/dev/null 2>&1 || (echo "ERROR: You need to have Graphviz installed to generate pprof visualization."; exit 1)
	@$(GO) tool pprof -svg ./tsdb $(TSDB_BENCHMARK_OUTPUT_DIR)/cpu.prof > $(TSDB_BENCHMARK_OUTPUT_DIR)/cpuprof.svg
	@$(GO) tool pprof --inuse_space -svg ./tsdb $(TSDB_BENCHMARK_OUTPUT_DIR)/mem.prof > $(TSDB_BENCHMARK_OUTPUT_DIR)/memprof.inuse.svg
	@$(GO) tool pprof --alloc_space -svg ./tsdb $(TSDB_BENCHMARK_OUTPUT_DIR)/mem.prof > $(TSDB_BENCHMARK_OUTPUT_DIR)/memprof.alloc.svg
	@$(GO) tool pprof -svg ./tsdb $(TSDB_BENCHMARK_OUTPUT_DIR)/block.prof > $(TSDB_BENCHMARK_OUTPUT_DIR)/blockprof.svg
	@$(GO) tool pprof -svg ./tsdb $(TSDB_BENCHMARK_OUTPUT_DIR)/mutex.prof > $(TSDB_BENCHMARK_OUTPUT_DIR)/mutexprof.svg

clean:
# Remove built binary and benchmark results
	rm -f $(TSDB_BIN)
	rm -rf $(TSDB_BENCHMARK_OUTPUT_DIR)

.PHONY: clean test
