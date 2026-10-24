find_package(Threads REQUIRED)
foreach(name IN ITEMS failover_and_threading failover_adversarial failover_production)
  add_executable(test_${name} test_${name}.cc)
  target_link_libraries(test_${name} PRIVATE greptimedb::threading
    greptimedb::core greptimedb::proto GTest::gtest_main Threads::Threads)
  add_test(NAME unit.library.${name} COMMAND test_${name})
  set_tests_properties(unit.library.${name} PROPERTIES
    LABELS "unit;library;failover;threading;scenario;lifecycle" TIMEOUT 60)
endforeach()
