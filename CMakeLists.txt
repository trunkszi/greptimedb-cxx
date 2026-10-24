cmake_minimum_required(VERSION 3.28)
project(greptimedb-cxx VERSION 0.1.0 LANGUAGES C CXX)

option(GREPTIMEDB_BUILD_TESTING "Build GreptimeDB protocol and SDK tests" ON)
option(GREPTIMEDB_BUILD_BENCHMARKS "Build GreptimeDB SDK benchmarks" OFF)
option(GREPTIMEDB_BUILD_EXAMPLES "Build runnable GreptimeDB SDK examples" ON)
set(GRPCXX_SOURCE_DIR "${CMAKE_CURRENT_SOURCE_DIR}/../grpcxx" CACHE PATH "grpcxx source checkout")
set(BENCHMARK_ENABLE_WERROR OFF CACHE BOOL "Disable upstream benchmark warnings as errors" FORCE)
set(CMAKE_CXX_EXTENSIONS OFF)
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/bin")

# Use the selected compiler's runtime for C++26 library symbols during local execution.
if(CMAKE_CXX_COMPILER_ID STREQUAL "GNU")
  execute_process(COMMAND "${CMAKE_CXX_COMPILER}" -print-file-name=libstdc++.so.6
    OUTPUT_VARIABLE greptimedb_cxx_runtime OUTPUT_STRIP_TRAILING_WHITESPACE
    RESULT_VARIABLE greptimedb_cxx_runtime_result)
  if(greptimedb_cxx_runtime_result EQUAL 0 AND
      IS_ABSOLUTE "${greptimedb_cxx_runtime}" AND EXISTS "${greptimedb_cxx_runtime}")
    get_filename_component(greptimedb_cxx_runtime_dir "${greptimedb_cxx_runtime}" DIRECTORY)
    list(APPEND CMAKE_BUILD_RPATH "${greptimedb_cxx_runtime_dir}")
  endif()
endif()

if(CMAKE_CXX_COMPILER_ID STREQUAL "GNU")
  set(greptimedb_reflection_flag -freflection)
elseif(CMAKE_CXX_COMPILER_ID MATCHES "Clang")
  set(greptimedb_reflection_flag -freflection-latest)
else()
  message(FATAL_ERROR "greptimedb-cxx requires GCC or Clang with C++26 reflection support")
endif()

include(CheckCXXSourceCompiles)
set(greptimedb_required_flags "${CMAKE_REQUIRED_FLAGS}")
set(CMAKE_REQUIRED_FLAGS "${CMAKE_REQUIRED_FLAGS} -std=c++26 ${greptimedb_reflection_flag} -fno-exceptions")
check_cxx_source_compiles("#include <meta>
#ifndef __cpp_impl_reflection
#error C++26 reflection is required
#endif
int main() { constexpr auto type = ^^int; static_assert(std::meta::is_type(type)); }"
  GREPTIMEDB_HAS_REFLECTION)
set(CMAKE_REQUIRED_FLAGS "${greptimedb_required_flags}")
if(NOT GREPTIMEDB_HAS_REFLECTION)
  message(FATAL_ERROR "Use a GCC 16 or Clang P2996 reflection toolchain through ./build.sh")
endif()

add_library(greptimedb_compile_options INTERFACE)
if(CMAKE_VERSION VERSION_GREATER_EQUAL 3.30)
  target_compile_features(greptimedb_compile_options INTERFACE cxx_std_26)
else()
  target_compile_options(greptimedb_compile_options INTERFACE -std=c++26)
endif()
target_compile_options(greptimedb_compile_options INTERFACE
  ${greptimedb_reflection_flag} -fno-exceptions -Wall -Wextra -Wswitch)
add_compile_options(-fno-exceptions -Wall -Wextra -Wswitch)

find_package(Protobuf REQUIRED)

if(NOT TARGET grpcxx::core)
  if(NOT EXISTS "${GRPCXX_SOURCE_DIR}/CMakeLists.txt")
    message(FATAL_ERROR "GRPCXX_SOURCE_DIR must name an existing grpcxx checkout")
  endif()
  set(GRPCXX_BUILD_TESTING OFF CACHE BOOL "Build dependency tests" FORCE)
  set(GRPCXX_BUILD_BENCHMARKS OFF CACHE BOOL "Build dependency benchmarks" FORCE)
  set(GRPCXX_BUILD_EXAMPLES OFF CACHE BOOL "Build dependency examples" FORCE)
  set(GRPCXX_ENABLE_PROTOBUF ON CACHE BOOL "Enable the GreptimeDB wire codec" FORCE)
  set(GRPCXX_ENABLE_FORY OFF CACHE BOOL "Enable unrelated Fory codec" FORCE)
  set(GRPCXX_ENABLE_CAPNP OFF CACHE BOOL "Enable unrelated CapnProto codec" FORCE)
  add_subdirectory("${GRPCXX_SOURCE_DIR}" "${CMAKE_BINARY_DIR}/_deps/grpcxx" EXCLUDE_FROM_ALL)
endif()
foreach(required_target IN ITEMS grpcxx::codec_protobuf grpcxx::reflection protobuf::protoc protoc-gen-grpcxx)
  if(NOT TARGET ${required_target})
    message(FATAL_ERROR "grpcxx must provide ${required_target}; enable its Protobuf codec and tools")
  endif()
endforeach()

include(cmake/GreptimeProtoGen.cmake)
add_subdirectory(src/greptimedb/proto)
add_subdirectory(src/greptimedb/core)
add_subdirectory(src/greptimedb/batch)
add_subdirectory(src/greptimedb/transport)
add_subdirectory(src/greptimedb/flight)
add_subdirectory(src/greptimedb/row)
add_subdirectory(src/greptimedb/client)
add_subdirectory(src/greptimedb/failover)
add_subdirectory(src/greptimedb/threading)

if(GREPTIMEDB_BUILD_TESTING)
  include(CTest)
  enable_testing()
  find_package(GTest CONFIG REQUIRED)
  add_subdirectory(tests)
endif()

if(GREPTIMEDB_BUILD_BENCHMARKS)
  add_subdirectory(benches)
endif()

if(GREPTIMEDB_BUILD_EXAMPLES)
  add_subdirectory(examples)
endif()
