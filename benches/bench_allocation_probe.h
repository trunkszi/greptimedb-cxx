#pragma once

#include <algorithm>
#include <cstddef>
#include <cstdlib>
#include <new>

/** Replace C++ allocation only in the reflected-row benchmark executable.
 * Counting is enabled solely around warmed Clear/PushRef ingestion cycles. */
namespace greptimedb::bench_allocation {
inline thread_local bool enabled = false;
inline thread_local std::size_t count = 0;

inline void *
Allocate(std::size_t size,
         std::size_t alignment = alignof(std::max_align_t)) noexcept {
  if (enabled)
    ++count;
  size = std::max(size, std::size_t{1});
  if (alignment <= alignof(std::max_align_t))
    return std::malloc(size);
  void *memory = nullptr;
  return posix_memalign(&memory, alignment, size) == 0 ? memory : nullptr;
}

inline void *Require(void *memory) noexcept {
  if (!memory)
    std::abort();
  return memory;
}
} // namespace greptimedb::bench_allocation

void *operator new(std::size_t size) {
  return greptimedb::bench_allocation::Require(
      greptimedb::bench_allocation::Allocate(size));
}
void *operator new[](std::size_t size) { return ::operator new(size); }
void *operator new(std::size_t size, const std::nothrow_t &) noexcept {
  return greptimedb::bench_allocation::Allocate(size);
}
void *operator new[](std::size_t size, const std::nothrow_t &tag) noexcept {
  return ::operator new(size, tag);
}
void *operator new(std::size_t size, std::align_val_t alignment) {
  return greptimedb::bench_allocation::Require(
      greptimedb::bench_allocation::Allocate(
          size, static_cast<std::size_t>(alignment)));
}
void *operator new[](std::size_t size, std::align_val_t alignment) {
  return ::operator new(size, alignment);
}
void *operator new(std::size_t size, std::align_val_t alignment,
                   const std::nothrow_t &) noexcept {
  return greptimedb::bench_allocation::Allocate(
      size, static_cast<std::size_t>(alignment));
}
void *operator new[](std::size_t size, std::align_val_t alignment,
                     const std::nothrow_t &tag) noexcept {
  return ::operator new(size, alignment, tag);
}
void operator delete(void *memory) noexcept { std::free(memory); }
void operator delete[](void *memory) noexcept { std::free(memory); }
void operator delete(void *memory, std::size_t) noexcept { std::free(memory); }
void operator delete[](void *memory, std::size_t) noexcept {
  std::free(memory);
}
void operator delete(void *memory, const std::nothrow_t &) noexcept {
  std::free(memory);
}
void operator delete[](void *memory, const std::nothrow_t &) noexcept {
  std::free(memory);
}
void operator delete(void *memory, std::align_val_t) noexcept {
  std::free(memory);
}
void operator delete[](void *memory, std::align_val_t) noexcept {
  std::free(memory);
}
void operator delete(void *memory, std::size_t, std::align_val_t) noexcept {
  std::free(memory);
}
void operator delete[](void *memory, std::size_t, std::align_val_t) noexcept {
  std::free(memory);
}
void operator delete(void *memory, std::align_val_t,
                     const std::nothrow_t &) noexcept {
  std::free(memory);
}
void operator delete[](void *memory, std::align_val_t,
                       const std::nothrow_t &) noexcept {
  std::free(memory);
}
