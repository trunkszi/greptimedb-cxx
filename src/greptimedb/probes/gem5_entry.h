/** gem5 SE-mode ROI fences. Stats outside the fences include process startup. */
#pragma once

#include <cstdint>

namespace greptimedb::probes {

inline void Gem5ResetStats() noexcept {
#if defined(GREPTIME_GEM5_ROI)
  // gem5 writes the m5 result through rax. Clobber it so the checksum stays intact.
  asm volatile("mfence; lfence; .byte 0x0f, 0x04; .word 0x40; mfence; lfence"
               :
               : "D"(0ULL), "S"(0ULL)
               : "rax", "rcx", "r11", "cc", "memory");
#endif
}

inline void Gem5DumpStats() noexcept {
#if defined(GREPTIME_GEM5_ROI)
  asm volatile("mfence; lfence; .byte 0x0f, 0x04; .word 0x41; mfence; lfence"
               :
               : "D"(0ULL), "S"(0ULL)
               : "rax", "rcx", "r11", "cc", "memory");
#endif
}

} // namespace greptimedb::probes
