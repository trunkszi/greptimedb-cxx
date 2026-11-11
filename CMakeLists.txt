cmake_minimum_required(VERSION 4.0)
file(MAKE_DIRECTORY "${CMAKE_BINARY_DIR}/tmp")
if(DEFINED ENV{TMPDIR} AND NOT "$ENV{TMPDIR}" STREQUAL "")
  file(MAKE_DIRECTORY "$ENV{TMPDIR}")
endif()
project(greptimedb VERSION 0.1.0 LANGUAGES C CXX)

option(GREPTIMEDB_BUILD_TESTING "Build GreptimeDB protocol and SDK tests" ON)
option(GREPTIMEDB_BUILD_BENCHMARKS "Build GreptimeDB SDK benchmarks" OFF)
option(GREPTIMEDB_BUILD_EXAMPLES "Build runnable GreptimeDB SDK examples" ON)
set(BENCHMARK_ENABLE_WERROR OFF CACHE BOOL "Disable upstream benchmark warnings as errors" FORCE)
set(CMAKE_CXX_STANDARD 26)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/bin")
set(CMAKE_CXX_SCAN_FOR_MODULES OFF)

# Use the selected compiler's runtime for C++26 library symbols during local execution.
if(CMAKE_CXX_COMPILER_ID STREQUAL "GNU")
  set(CMAKE_C_IMPLICIT_LINK_DIRECTORIES "${CMAKE_CXX_IMPLICIT_LINK_DIRECTORIES}")
  execute_process(COMMAND "${CMAKE_CXX_COMPILER}" -print-file-name=libstdc++.so.6
    OUTPUT_VARIABLE greptimedb_cxx_runtime OUTPUT_STRIP_TRAILING_WHITESPACE
    RESULT_VARIABLE greptimedb_cxx_runtime_result)
  if(greptimedb_cxx_runtime_result EQUAL 0 AND
      IS_ABSOLUTE "${greptimedb_cxx_runtime}" AND EXISTS "${greptimedb_cxx_runtime}")
    get_filename_component(greptimedb_cxx_runtime_dir "${greptimedb_cxx_runtime}" DIRECTORY)
    list(APPEND CMAKE_BUILD_RPATH "${greptimedb_cxx_runtime_dir}")
    add_link_options("-L${greptimedb_cxx_runtime_dir}")
  endif()
endif()

foreach(runtime_dir IN LISTS CMAKE_CXX_IMPLICIT_LINK_DIRECTORIES)
  if(NOT runtime_dir MATCHES "/sysroot(/|$)")
    list(APPEND CMAKE_BUILD_RPATH "${runtime_dir}")
  endif()
endforeach()

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
int main() { constexpr auto type = ^^int; static_assert(std::meta::is_type(type)); }"
  GREPTIMEDB_HAS_REFLECTION)
set(CMAKE_REQUIRED_FLAGS "${greptimedb_required_flags}")
if(NOT GREPTIMEDB_HAS_REFLECTION)
  message(FATAL_ERROR "Use a GCC 16 or Clang P2996 reflection toolchain through ./build.py")
endif()

add_library(greptimedb_compile_options INTERFACE)
target_compile_features(greptimedb_compile_options INTERFACE cxx_std_26)
target_compile_options(greptimedb_compile_options INTERFACE
  ${greptimedb_reflection_flag} -fno-exceptions -Wall -Wextra -Wswitch)
if(CMAKE_CXX_COMPILER_ID MATCHES "Clang")
  target_compile_options(greptimedb_compile_options INTERFACE -fconstexpr-steps=10000000)
endif()
if(CMAKE_SYSTEM_PROCESSOR MATCHES "^(x86_64|AMD64|amd64)$")
  target_compile_options(greptimedb_compile_options INTERFACE -mpopcnt)
endif()
add_compile_options(-fno-exceptions -Wall -Wextra -Wswitch)

set(protobuf_MODULE_COMPATIBLE ON)
find_package(Protobuf CONFIG QUIET)
if(NOT Protobuf_FOUND)
  find_package(Protobuf REQUIRED)
endif()

find_package(uarch CONFIG REQUIRED)
find_package(cares CONFIG REQUIRED)
find_package(nexora CONFIG REQUIRED)
find_package(grpcxx CONFIG REQUIRED)
include(cmake/GreptimeHostTools.cmake)
greptimedb_find_host_tool(_greptimedb_host_plugin tools/grpcxx/protoc-gen-grpcxx)
greptimedb_find_host_tool(_greptimedb_host_protoc tools/protobuf/protoc)
foreach(required_target IN ITEMS grpcxx::sync grpcxx::codec_protobuf grpcxx::reflection)
  if(NOT TARGET ${required_target})
    message(FATAL_ERROR "grpcxx must provide ${required_target}; enable its Protobuf codec and tools")
  endif()
endforeach()
if(NOT _greptimedb_host_protoc AND NOT TARGET protobuf::protoc)
  message(FATAL_ERROR "Protobuf must provide a host protoc executable")
endif()
if(NOT _greptimedb_host_plugin AND NOT TARGET protoc-gen-grpcxx AND NOT TARGET grpcxx::protoc-gen-grpcxx)
  find_program(_greptimedb_grpcxx_plugin NAMES protoc-gen-grpcxx)
  if(NOT _greptimedb_grpcxx_plugin)
    message(FATAL_ERROR "grpcxx must provide protoc-gen-grpcxx; enable its Protobuf codec and tools")
  endif()
endif()

include(cmake/GreptimeProtoGen.cmake)
include(cmake/GreptimeCompressionDependencies.cmake)
add_subdirectory(src/greptimedb/proto)
add_subdirectory(src/greptimedb/core)
target_link_libraries(greptimedb_core PUBLIC uarch::uarch)
add_subdirectory(src/greptimedb/batch)
add_subdirectory(src/greptimedb/flight)
add_subdirectory(src/greptimedb/row)
add_subdirectory(src/greptimedb/client)
add_subdirectory(src/greptimedb/threading)

if(GREPTIMEDB_BUILD_TESTING)
  include(CTest)
  enable_testing()
  find_package(GTest CONFIG REQUIRED)
  add_subdirectory(tests)
  set_tests_properties(unit.library.batch.column_buf_and_buffer_policy
    DIRECTORY tests/units/batch
    PROPERTIES ENVIRONMENT "ASAN_OPTIONS=allocator_may_return_null=1")
endif()

if(GREPTIMEDB_BUILD_BENCHMARKS)
  add_subdirectory(benches)
endif()

if(GREPTIMEDB_BUILD_EXAMPLES)
  add_subdirectory(examples)
endif()

include(GNUInstallDirs)
include(CMakePackageConfigHelpers)
add_library(greptimedb INTERFACE)
add_library(greptimedb::greptimedb ALIAS greptimedb)
target_link_libraries(greptimedb INTERFACE
  greptimedb::client greptimedb::row greptimedb::threading)
add_library(greptimedb_flight INTERFACE)
add_library(greptimedb::flight ALIAS greptimedb_flight)
target_link_libraries(greptimedb_flight INTERFACE greptimedb::flight_ipc)

set(greptimedb_export_targets
  greptimedb
  greptimedb_compile_options
  greptimedb_proto
  greptimedb_core
  greptimedb_row
  greptimedb_batch
  greptimedb_flight_ipc
  greptimedb_flight
  greptimedb_streaming
  greptimedb_threading
  greptimedb_client)

set_target_properties(greptimedb_proto PROPERTIES
  EXPORT_NAME proto
  INTERFACE_INCLUDE_DIRECTORIES "$<BUILD_INTERFACE:${CMAKE_BINARY_DIR}/src/greptimedb/proto/generated>;$<BUILD_INTERFACE:${PROJECT_SOURCE_DIR}/src>;$<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>")
foreach(target IN ITEMS greptimedb_core greptimedb_row greptimedb_batch
    greptimedb_flight_ipc greptimedb_flight
    greptimedb_streaming greptimedb_threading
    greptimedb_client)
  string(REGEX REPLACE "^greptimedb_" "" export_name "${target}")
  set_target_properties(${target} PROPERTIES
    EXPORT_NAME "${export_name}"
    INTERFACE_INCLUDE_DIRECTORIES "$<BUILD_INTERFACE:${PROJECT_SOURCE_DIR}/src>;$<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>")
endforeach()

install(TARGETS ${greptimedb_export_targets}
  EXPORT greptimedb-targets
  ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR})
install(DIRECTORY "${CMAKE_CURRENT_SOURCE_DIR}/src/greptimedb/"
  DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}/greptimedb FILES_MATCHING
  PATTERN "probes" EXCLUDE
  PATTERN "*.h" PATTERN "*.hpp" PATTERN "*.inc" PATTERN "*.tpp")
install(DIRECTORY "${CMAKE_BINARY_DIR}/src/greptimedb/proto/generated/"
  DESTINATION ${CMAKE_INSTALL_INCLUDEDIR} FILES_MATCHING
  PATTERN "*.pb.h" PATTERN "*.grpcxx.h")
configure_package_config_file(cmake/greptimedbConfig.cmake.in
  "${CMAKE_CURRENT_BINARY_DIR}/greptimedbConfig.cmake"
  INSTALL_DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/greptimedb)
write_basic_package_version_file(
  "${CMAKE_CURRENT_BINARY_DIR}/greptimedbConfigVersion.cmake"
  VERSION ${PROJECT_VERSION} COMPATIBILITY SameMinorVersion)
install(FILES
  "${PROJECT_SOURCE_DIR}/cmake/GreptimeCompressionDependencies.cmake"
  "${CMAKE_CURRENT_BINARY_DIR}/greptimedbConfig.cmake"
  "${CMAKE_CURRENT_BINARY_DIR}/greptimedbConfigVersion.cmake"
  DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/greptimedb)
install(EXPORT greptimedb-targets
  NAMESPACE greptimedb::
  DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/greptimedb)
