include_guard(GLOBAL)

set(GREPTIME_PROTO_REVISION "acb247415b8f4c2f0750783dab4e1f38c71f5860")
set(GREPTIME_PROTO_DIR "" CACHE PATH "Pinned greptime-proto checkout or its proto directory")

function(greptimedb_clone_proto source)
  string(RANDOM LENGTH 16 ALPHABET 0123456789abcdef suffix)
  set(staging "${source}.download-${suffix}")
  execute_process(COMMAND "${GIT_EXECUTABLE}" clone --depth 1 --no-checkout
    https://github.com/GreptimeTeam/greptime-proto.git "${staging}"
    RESULT_VARIABLE result ERROR_VARIABLE details)
  if(result EQUAL 0)
    execute_process(COMMAND "${GIT_EXECUTABLE}" -C "${staging}" fetch --depth 1 origin
      "${GREPTIME_PROTO_REVISION}" RESULT_VARIABLE result ERROR_VARIABLE details)
  endif()
  if(result EQUAL 0)
    execute_process(COMMAND "${GIT_EXECUTABLE}" -C "${staging}" checkout --detach
      "${GREPTIME_PROTO_REVISION}" RESULT_VARIABLE result ERROR_VARIABLE details)
  endif()
  if(NOT result EQUAL 0)
    file(REMOVE_RECURSE "${staging}")
    message(FATAL_ERROR "Cannot populate pinned greptime-proto ${source}: ${details}")
  endif()
  file(RENAME "${staging}" "${source}" RESULT result NO_REPLACE)
  if(NOT result STREQUAL "0")
    file(REMOVE_RECURSE "${staging}")
    message(FATAL_ERROR "Cannot publish pinned greptime-proto ${source}: ${result}")
  endif()
endfunction()

function(greptimedb_resolve_proto_root output)
  if(GREPTIME_PROTO_DIR)
    set(source "${GREPTIME_PROTO_DIR}")
  elseif(DEFINED ENV{GREPTIME_PROTO_DIR} AND NOT "$ENV{GREPTIME_PROTO_DIR}" STREQUAL "")
    set(source "$ENV{GREPTIME_PROTO_DIR}")
  elseif(DEFINED ENV{HOME} AND NOT "$ENV{HOME}" STREQUAL "")
    set(source "$ENV{HOME}/.cache/greptime-proto")
  else()
    message(FATAL_ERROR "Set GREPTIME_PROTO_DIR: HOME is unset and no protocol source was supplied")
  endif()
  get_filename_component(source "${source}" ABSOLUTE BASE_DIR "${CMAKE_SOURCE_DIR}")
  find_package(Git REQUIRED)
  if(NOT EXISTS "${source}")
    get_filename_component(parent "${source}" DIRECTORY)
    file(MAKE_DIRECTORY "${parent}")
    file(LOCK "${source}.lock" GUARD FUNCTION TIMEOUT 120 RESULT_VARIABLE lock_result)
    if(NOT lock_result STREQUAL "0")
      message(FATAL_ERROR "Cannot lock greptime-proto cache ${source}: ${lock_result}")
    endif()
    if(NOT EXISTS "${source}")
      greptimedb_clone_proto("${source}")
    endif()
  endif()
  if(IS_DIRECTORY "${source}/proto/greptime/v1")
    set(root "${source}/proto")
  elseif(IS_DIRECTORY "${source}/greptime/v1")
    set(root "${source}")
  else()
    message(FATAL_ERROR "GREPTIME_PROTO_DIR=${source} must contain proto/greptime/v1 or greptime/v1")
  endif()
  execute_process(COMMAND "${GIT_EXECUTABLE}" -C "${root}" rev-parse HEAD
    RESULT_VARIABLE result OUTPUT_VARIABLE actual OUTPUT_STRIP_TRAILING_WHITESPACE
    ERROR_VARIABLE details)
  if(NOT result EQUAL 0 OR NOT actual STREQUAL GREPTIME_PROTO_REVISION)
    message(FATAL_ERROR
      "greptime-proto ${source} must be a Git checkout at ${GREPTIME_PROTO_REVISION}; "
      "found '${actual}'. The supplied checkout has not been modified. ${details}")
  endif()
  execute_process(COMMAND "${CMAKE_COMMAND}" -E env GIT_OPTIONAL_LOCKS=0
    "${GIT_EXECUTABLE}" -C "${root}" status --porcelain -- .
    RESULT_VARIABLE result OUTPUT_VARIABLE changes ERROR_VARIABLE details)
  if(NOT result EQUAL 0 OR NOT changes STREQUAL "")
    message(FATAL_ERROR "greptime-proto sources must be clean at the pinned revision: ${changes}${details}")
  endif()
  execute_process(COMMAND "${GIT_EXECUTABLE}" -C "${root}" symbolic-ref -q HEAD
    RESULT_VARIABLE symbolic_result OUTPUT_VARIABLE symbolic_ref OUTPUT_STRIP_TRAILING_WHITESPACE)
  set(provenance HEAD index packed-refs)
  if(symbolic_result EQUAL 0)
    list(APPEND provenance "${symbolic_ref}")
  endif()
  foreach(entry IN LISTS provenance)
    execute_process(COMMAND "${GIT_EXECUTABLE}" -C "${root}" rev-parse --git-path "${entry}"
      RESULT_VARIABLE result OUTPUT_VARIABLE path OUTPUT_STRIP_TRAILING_WHITESPACE)
    if(NOT result EQUAL 0)
      message(FATAL_ERROR "Cannot resolve greptime-proto provenance ${entry}")
    endif()
    get_filename_component(path "${path}" ABSOLUTE BASE_DIR "${root}")
    if(EXISTS "${path}")
      set_property(DIRECTORY APPEND PROPERTY CMAKE_CONFIGURE_DEPENDS "${path}")
    endif()
  endforeach()
  set(${output} "${root}" PARENT_SCOPE)
endfunction()

function(greptimedb_generate_proto_library)
  foreach(required protobuf::protoc protoc-gen-grpcxx grpcxx::codec_protobuf grpcxx::reflection)
    if(NOT TARGET "${required}")
      message(FATAL_ERROR "Greptime protocol generation requires target ${required}")
    endif()
  endforeach()
  greptimedb_resolve_proto_root(proto_root)
  set(local_root "${PROJECT_SOURCE_DIR}/proto")
  set(generated "${CMAKE_CURRENT_BINARY_DIR}/generated")
  file(MAKE_DIRECTORY "${generated}")
  file(GLOB_RECURSE upstream CONFIGURE_DEPENDS
    "${proto_root}/greptime/v1/*.proto"
    "${proto_root}/substrait_extension/*.proto"
    "${proto_root}/prometheus/*.proto")
  if(NOT upstream)
    message(FATAL_ERROR "No Greptime protocol files found in ${proto_root}")
  endif()
  list(SORT upstream)
  set_property(DIRECTORY APPEND PROPERTY CMAKE_CONFIGURE_DEPENDS ${upstream})
  set(inputs ${upstream} "${local_root}/Flight.proto")
  get_target_property(protobuf_includes protobuf::libprotobuf INTERFACE_INCLUDE_DIRECTORIES)
  find_path(wellknown_include google/protobuf/duration.proto
    HINTS ${Protobuf_INCLUDE_DIRS} ${protobuf_includes} NO_CACHE REQUIRED)
  set(include_arguments "--proto_path=${proto_root}" "--proto_path=${local_root}"
    "--proto_path=${wellknown_include}")
  set(sources)
  set(headers)
  foreach(input IN LISTS inputs)
    if(input STREQUAL "${local_root}/Flight.proto")
      set(relative "Flight.proto")
    else()
      file(RELATIVE_PATH relative "${proto_root}" "${input}")
    endif()
    string(REGEX REPLACE "\\.proto$" "" stem "${relative}")
    set(source "${generated}/${stem}.pb.cc")
    set(message_header "${generated}/${stem}.pb.h")
    set(contract_header "${generated}/${stem}.grpcxx.h")
    get_filename_component(directory "${source}" DIRECTORY)
    add_custom_command(OUTPUT "${source}" "${message_header}" "${contract_header}"
      COMMAND "${CMAKE_COMMAND}" -E make_directory "${directory}"
      COMMAND "$<TARGET_FILE:protobuf::protoc>" ${include_arguments}
        "--cpp_out=${generated}"
        "--plugin=protoc-gen-grpcxx=$<TARGET_FILE:protoc-gen-grpcxx>"
        "--grpcxx_out=${generated}" "${input}"
      DEPENDS ${inputs} "${wellknown_include}/google/protobuf/duration.proto"
        protobuf::protoc protoc-gen-grpcxx
      COMMENT "Generating Greptime messages and contracts: ${relative}"
      VERBATIM COMMAND_EXPAND_LISTS)
    list(APPEND sources "${source}")
    list(APPEND headers "${message_header}" "${contract_header}")
  endforeach()
  add_custom_target(greptimedb_proto_codegen DEPENDS ${sources} ${headers})
  add_library(greptimedb_proto STATIC ${sources} ${headers})
  add_library(greptimedb::proto ALIAS greptimedb_proto)
  add_dependencies(greptimedb_proto greptimedb_proto_codegen)
  target_include_directories(greptimedb_proto PUBLIC
    "$<BUILD_INTERFACE:${generated}>" "$<BUILD_INTERFACE:${PROJECT_SOURCE_DIR}/src>")
  target_link_libraries(greptimedb_proto PUBLIC grpcxx::codec_protobuf grpcxx::reflection)
  if(TARGET greptimedb_compile_options)
    target_link_libraries(greptimedb_proto PUBLIC greptimedb_compile_options)
  endif()
endfunction()
