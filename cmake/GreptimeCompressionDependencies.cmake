include_guard(GLOBAL)

foreach(codec IN ITEMS lz4 zstd)
  if(NOT TARGET greptimedb_dep::${codec})
    if(codec STREQUAL "lz4")
      set(header lz4frame.h)
    else()
      set(header zstd.h)
    endif()
    find_path(GREPTIMEDB_${codec}_INCLUDE_DIR NAMES ${header} REQUIRED)
    find_library(GREPTIMEDB_${codec}_LIBRARY NAMES ${codec} REQUIRED)
    add_library(greptimedb_dep::${codec} UNKNOWN IMPORTED GLOBAL)
    set_target_properties(greptimedb_dep::${codec} PROPERTIES
      IMPORTED_LOCATION "${GREPTIMEDB_${codec}_LIBRARY}"
      INTERFACE_INCLUDE_DIRECTORIES "${GREPTIMEDB_${codec}_INCLUDE_DIR}")
  endif()
endforeach()
