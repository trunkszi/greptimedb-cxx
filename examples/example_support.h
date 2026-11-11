#pragma once

#include <cstdio>
#include <print>

// Runtime checks remain active in release builds, so CTest verifies behavior.
#define EXAMPLE_REQUIRE(expression)                                                                                    \
  do {                                                                                                                 \
    if (!(expression)) {                                                                                               \
      std::println(stderr, "{}:{}: {}", __FILE__, __LINE__, #expression);                                              \
      return 1;                                                                                                        \
    }                                                                                                                  \
  } while (false)
