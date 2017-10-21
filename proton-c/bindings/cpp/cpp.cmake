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
# under the License.0
#

# Check C++ capabilities.

include(CheckCXXSourceCompiles)
set(CMAKE_REQUIRED_FLAGS "${CMAKE_CXX_FLAGS} ${CXX_STANDARD} ${CXX_WARNING_FLAGS}")

# prog is a compile test program, name is the HAS_XXX/PN_CPP_HAS_XXX name, feature is
# the CMake compile feature name or "" if there is no compile feature for this check
macro (cxx_test prog name feature)
  if (HAS_CPP11)               # C++11 enables all features, no check needed
    set(HAS_${name} ON)
  elseif (DEFINED CMAKE_CXX_COMPILE_FEATURES AND NOT "${feature}" STREQUAL "")
    # Use compile features if available
    list(FIND CMAKE_CXX_COMPILE_FEATURES ${feature} N)
    if(${N} GREATER -1)
      set(HAS_${name} ON)
    endif()
  else()                        # Last resort - a compile test
    check_cxx_source_compiles("${prog}" HAS_${name})
  endif()
  if (HAS_${name})
    add_definitions(-DPN_CPP_HAS_${name}=1)
  endif()
endmacro()

# Test for C++11 first, if set it short-cuts all the other tests
cxx_test("#if defined(__cplusplus) && __cplusplus >= 201103\nint main(int, char**) { return 0; }\n#endif" CPP11 cxx_std_11)

cxx_test("#include <string>\nvoid blah(std::string&&) {} int main(int, char**) { blah(\"hello\"); return 0; }" RVALUE_REFERENCES cxx_rvalue_references)
cxx_test("class x {explicit operator int(); }; int main(int, char**) { return 0; }" EXPLICIT_CONVERSIONS cxx_explicit_conversions)
cxx_test("int main(int, char**) { int a=[](){return 42;}(); return a; }" LAMBDAS cxx_lambdas)
cxx_test("template <class... X> void x(X... a) {} int main(int, char**) { x(1); x(43, \"\"); return 0; }" VARIADIC_TEMPLATES cxx_variadic_templates)

# These tests don't have a CMake compile feature
cxx_test("#include <random>\nint main(int, char**) { return 0; }" HEADER_RANDOM "")
cxx_test("#include <memory>\nstd::unique_ptr<int> u; int main(int, char**) { return 0; }" STD_UNIQUE_PTR "")
cxx_test("#include <thread>\nstd::thread t; int main(int, char**) { return 0; }" STD_THREAD "")
cxx_test("#include <mutex>\nstd::mutex m; int main(int, char**) { return 0; }" STD_MUTEX "")
cxx_test("#include <atomic>\nstd::atomic<int> a; int main(int, char**) { return 0; }" STD_ATOMIC "")

# Special case: thread_local is not defined on some MSVC that claim to be C++11, always check
set(thread_local_prog "struct x {x() {}}; int main(int, char**) { static thread_local x foo; return 0; }")
if(MSVC)
  check_cxx_source_compiles("${thread_local_prog}" HAS_THREAD_LOCAL)
  if (HAS_THREAD_LOCAL)
    add_definitions(-DPN_CPP_HAS_THRAD_LOCAL=1)
    message(STATUS "C++ feature HAS_THREAD_LOCAL=${HAS_THREAD_LOCAL}")
  endif()
else()
  cxx_test("${thread_local_prog}" THREAD_LOCAL cxx_thread_local)
endif()

unset(CMAKE_REQUIRED_FLAGS) # Don't contaminate later C tests with C++ flags

