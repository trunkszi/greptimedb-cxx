// Deterministic allocation failures require overrides shared by every allocation form.
#pragma once
#include <algorithm>
#include <cstddef>
#include <cstdlib>
#include <new>

namespace allocation_probe {
thread_local bool enabled = false;
thread_local std::size_t count = 0;
thread_local bool fail_nothrow = false;
thread_local std::size_t nothrow_remaining = 0;
bool RejectNothrow() noexcept {
  if (!fail_nothrow) return false;
  if (nothrow_remaining == 0) return true;
  --nothrow_remaining;
  return false;
}

void *Allocate(std::size_t size, std::size_t alignment = alignof(std::max_align_t)) noexcept {
  if (enabled) ++count;
  size = std::max(size, std::size_t{1});
  if (alignment <= alignof(std::max_align_t)) return std::malloc(size);
  void *result = nullptr;
  if (posix_memalign(&result, alignment, size) != 0) return nullptr;
  return result;
}
void *Require(void *memory) noexcept {
  if (!memory) std::abort();
  return memory;
}
} // namespace allocation_probe

void *operator new(std::size_t size) { return allocation_probe::Require(allocation_probe::Allocate(size)); }
void *operator new[](std::size_t size) { return ::operator new(size); }
void *operator new(std::size_t size, const std::nothrow_t &) noexcept {
  if (allocation_probe::RejectNothrow()) return nullptr;
  return allocation_probe::Allocate(size);
}
void *operator new[](std::size_t size, const std::nothrow_t &tag) noexcept { return ::operator new(size, tag); }
void *operator new(std::size_t size, std::align_val_t alignment) {
  return allocation_probe::Require(allocation_probe::Allocate(size, static_cast<std::size_t>(alignment)));
}
void *operator new[](std::size_t size, std::align_val_t alignment) { return ::operator new(size, alignment); }
void *operator new(std::size_t size, std::align_val_t alignment, const std::nothrow_t &) noexcept {
  if (allocation_probe::RejectNothrow()) return nullptr;
  return allocation_probe::Allocate(size, static_cast<std::size_t>(alignment));
}
void *operator new[](std::size_t size, std::align_val_t alignment, const std::nothrow_t &tag) noexcept {
  return ::operator new(size, alignment, tag);
}
void operator delete(void *memory) noexcept { std::free(memory); }
void operator delete[](void *memory) noexcept { std::free(memory); }
void operator delete(void *memory, std::size_t) noexcept { std::free(memory); }
void operator delete[](void *memory, std::size_t) noexcept { std::free(memory); }
void operator delete(void *memory, const std::nothrow_t &) noexcept { std::free(memory); }
void operator delete[](void *memory, const std::nothrow_t &) noexcept { std::free(memory); }
void operator delete(void *memory, std::align_val_t) noexcept { std::free(memory); }
void operator delete[](void *memory, std::align_val_t) noexcept { std::free(memory); }
void operator delete(void *memory, std::size_t, std::align_val_t) noexcept { std::free(memory); }
void operator delete[](void *memory, std::size_t, std::align_val_t) noexcept { std::free(memory); }
void operator delete(void *memory, std::align_val_t, const std::nothrow_t &) noexcept { std::free(memory); }
void operator delete[](void *memory, std::align_val_t, const std::nothrow_t &) noexcept { std::free(memory); }
