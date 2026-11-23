#!/bin/bash
#
# Copyright (c) 2026, NVIDIA CORPORATION.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

file_path_test=path_character_test.py::test_path_characters

# Call from the integration_tests directory with a pytest node ID and its existing launch command.
# Run that node separately so the driver and standalone executors start with a UTF-8 locale.
run_with_unicode_path_test() {
  local path_test=$1
  shift
  # The fast Iceberg suite omits this test. A pinned pytest archive may also predate it.
  if [[ -z "$path_test" || ! -f "src/main/python/${path_test%%::*}" ]]; then
    "$@"
    return
  fi

  local regular_status=0 path_status=0
  "$@" --deselect="src/main/python/$path_test" || regular_status=$?
  (( regular_status == 0 || regular_status == 5 )) || return "$regular_status"

  # Keep any caller-provided file/parameter selection and keyword expression.
  LC_ALL=C.UTF-8 PYSP_TEST_spark_executorEnv_LC_ALL=C.UTF-8 \
    TESTS="${TESTS:-$path_test}" TEST="${TEST:+($TEST) and }${path_test##*::}" \
    "$@" || path_status=$?
  # Pytest returns 5 if a filter leaves one half empty.
  if (( path_status == 5 )); then return "$regular_status"; fi
  return "$path_status"
}
