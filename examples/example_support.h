#pragma once

#include <iostream>

// Runtime checks remain active in release builds, so CTest verifies behavior.
#define EXAMPLE_REQUIRE(expression)                                            \
  do {                                                                         \
    if (!(expression)) {                                                       \
      std::cerr << __FILE__ << ':' << __LINE__ << ": " << #expression << '\n'; \
      return 1;                                                                \
    }                                                                          \
  } while (false)
