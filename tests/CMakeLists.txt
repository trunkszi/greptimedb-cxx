find_package(GTest CONFIG REQUIRED)
add_subdirectory(units/proto)
add_subdirectory(units/core)
add_subdirectory(units/batch)
add_subdirectory(units/transport)

add_test(NAME check_source_contracts
  COMMAND "${PROJECT_SOURCE_DIR}/.github/scripts/check-source-contracts.sh")
set_tests_properties(check_source_contracts PROPERTIES
  LABELS "source;library;contracts;guardrails")
add_subdirectory(units/flight)
add_subdirectory(units/row)
add_subdirectory(units/streaming)
add_subdirectory(units/client)
add_subdirectory(units/adversarial)
add_subdirectory(e2e)
add_subdirectory(units/failover)
