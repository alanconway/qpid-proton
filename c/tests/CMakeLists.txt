#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#

configure_file(test_config.h.in test_config.h)
include_directories(${CMAKE_CURRENT_BINARY_DIR} ${CMAKE_SOURCE_DIR}/tests/include)

# NOTE: C library tests are written in C++ using the Catch2 framework.
# The tests are more concise than with a native C framework and we can
# use the same framework for C++ tests.
#
# Tests are built into a single executable and can be run individually
# or in groups by giving arguments to the test runner. CTest is set up
# to run tests in groups by tag.

# For each tag: compile ${tag}_test.cpp and add CTest target c-${tag}-test to run that files tests
set(tags object)

foreach(tag ${tags})
  set(test_src ${test_src} "${tag}_test.cpp") # Add to test sources
  set(ctest c-${tag}-test)
  add_test (NAME ${ctest}
    COMMAND ${TEST_EXE_PREFIX_CMD} "$<TARGET_FILE:c-test>" "-#" "[#${tag}_test]")
  if (WIN32)
    set_tests_properties(${ctest} PROPERTIES ENVIRONMENT PATH="$<TARGET_DIR:qpid-proton-core")
  endif(WIN32)
endforeach()

# Executable containing all the catch2 tests
add_executable(c-test c-test.cpp ${test_src})
set_target_properties(c-test PROPERTIES COMPILE_FLAGS "${CMAKE_CXX_FLAGS} ${CXX_WARNING_FLAGS}")
target_link_libraries(c-test PRIVATE qpid-proton-core ${PLATFORM_LIBS})

# Add test without any qpid-proton lib: allows choice of
# qpid-proton-core or qpid-proton
macro (pn_add_c_test_nolib test)
  add_executable (${test} ${ARGN})
  target_link_libraries (${test} ${PLATFORM_LIBS})
  set_source_files_properties (${ARGN} PROPERTIES
    COMPILE_FLAGS "${COMPILE_WARNING_FLAGS} ${COMPILE_PLATFORM_FLAGS}")
  if (BUILD_WITH_CXX)
    set_source_files_properties (${ARGN} PROPERTIES LANGUAGE CXX)
  endif (BUILD_WITH_CXX)
  add_test(NAME ${test} COMMAND ${test_env} $<TARGET_FILE:${test}>)
endmacro (pn_add_c_test_nolib)

# Add test with qpid-proton-core linked
macro (pn_add_c_test test)
  pn_add_c_test_nolib (${test} ${ARGN})
  target_link_libraries (${test} qpid-proton-core ${PLATFORM_LIBS})
endmacro (pn_add_c_test)

pn_add_c_test (c-message-tests message.c)
pn_add_c_test (c-engine-tests engine.c)
pn_add_c_test (c-refcount-tests refcount.c)
pn_add_c_test (c-event-tests event.c)
pn_add_c_test (c-data-tests data.c)
pn_add_c_test (c-condition-tests condition.c)
pn_add_c_test (c-connection-driver-tests connection_driver.c)
pn_add_c_test (c-ssl-tests ssl.c)

pn_add_c_test_nolib (c-parse-url-tests parse-url.c)
target_link_libraries (c-parse-url-tests qpid-proton)

if(HAS_PROACTOR)
  pn_add_c_test (c-proactor-tests proactor.c)
  target_link_libraries (c-proactor-tests qpid-proton-proactor)

  # TODO Enable by default when races and xcode problems are cleared up
  option(THREADERCISER "Run the threaderciser concurrency tests" OFF)
  if (THREADERCISER)
    pn_add_c_test(c-threaderciser threaderciser.c)
    target_link_libraries (c-threaderciser qpid-proton-proactor)
    find_library(Pthread_LIBRARY pthread)
    if (Pthread_LIBRARY)
      target_link_libraries (c-threaderciser ${Pthread_LIBRARY})
    endif()
  endif()

  if(WIN32)
    set(path "$<TARGET_FILE_DIR:c-broker>\\;$<TARGET_FILE_DIR:qpid-proton>")
  else(WIN32)
    set(path "$<TARGET_FILE_DIR:c-broker>:$ENV{PATH}")
  endif(WIN32)

  add_test(NAME c-fdlimit-tests COMMAND ${PN_ENV_SCRIPT} -- "PATH=${path}" "PYTHONPATH=${pypath}" ${PYTHON_EXECUTABLE} ${CMAKE_CURRENT_SOURCE_DIR}/fdlimit.py)
endif(HAS_PROACTOR)

# fuzz tests: tests/fuzz
if (ENABLE_FUZZ_TESTING)
  add_subdirectory(fuzz)
endif (ENABLE_FUZZ_TESTING)

