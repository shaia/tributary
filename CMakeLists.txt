cmake_minimum_required(VERSION 3.20)

project(tributary
  VERSION 0.1.0
  DESCRIPTION "Header-only C++20 fan-in pipeline: many producers, bounded SPSC rings, one consumer"
  LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)   # so clangd sees the right flags

# Only the top-level project gets to impose these; as a subdirectory we stay quiet.
if(PROJECT_IS_TOP_LEVEL)
  if(NOT CMAKE_BUILD_TYPE AND NOT CMAKE_CONFIGURATION_TYPES)
    set(CMAKE_BUILD_TYPE Release CACHE STRING "" FORCE)
  endif()
  # Binaries land in <project>/bin regardless of which build directory produced
  # them, so the thing you run is in a predictable place.
  set(TRIBUTARY_BIN_DIR ${CMAKE_CURRENT_SOURCE_DIR}/bin)
  set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${TRIBUTARY_BIN_DIR})
  foreach(cfg IN LISTS CMAKE_CONFIGURATION_TYPES)
    string(TOUPPER ${cfg} cfg_upper)
    set(CMAKE_RUNTIME_OUTPUT_DIRECTORY_${cfg_upper} ${TRIBUTARY_BIN_DIR})
  endforeach()
endif()

option(TRIBUTARY_BUILD_TESTS "Build the test suite" ${PROJECT_IS_TOP_LEVEL})
option(TRIBUTARY_BUILD_BENCH "Build the benchmarks" ${PROJECT_IS_TOP_LEVEL})
set(TRIBUTARY_SANITIZER "" CACHE STRING "address | thread | undefined | address,undefined")

# Layout constant. It is part of the ABI, which is why the inline namespace tag
# encodes it -- mixing a 64- and a 128-byte build is then a link error rather
# than silent memory corruption.
set(TRIBUTARY_CACHE_LINE "" CACHE STRING "Override the cache-line size used for layout")

find_package(Threads REQUIRED)

# --- the library -----------------------------------------------------------
#
# Header-only: the pipeline is templated on its channel and its sink, so there
# is nothing to compile separately.
add_library(tributary INTERFACE)
add_library(tributary::tributary ALIAS tributary)

target_include_directories(tributary INTERFACE
  $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
  $<INSTALL_INTERFACE:include>)
target_compile_features(tributary INTERFACE cxx_std_20)
target_link_libraries(tributary INTERFACE Threads::Threads)

if(TRIBUTARY_CACHE_LINE)
  target_compile_definitions(tributary INTERFACE TRIBUTARY_CACHE_LINE=${TRIBUTARY_CACHE_LINE})
endif()

# --- shared settings for our own targets -----------------------------------
#
# INTERFACE-only so nothing here leaks to consumers of the installed package.
add_library(tributary_dev_flags INTERFACE)

if(CMAKE_CXX_COMPILER_ID MATCHES "MSVC" OR CMAKE_CXX_COMPILER_FRONTEND_VARIANT STREQUAL "MSVC")
  target_compile_options(tributary_dev_flags INTERFACE /W4 /permissive-)
else()
  target_compile_options(tributary_dev_flags INTERFACE -Wall -Wextra -Wpedantic -Wshadow)
endif()

if(TRIBUTARY_SANITIZER)
  if(MSVC AND NOT CMAKE_CXX_COMPILER_ID MATCHES "Clang")
    message(FATAL_ERROR "TRIBUTARY_SANITIZER needs clang or gcc")
  endif()
  target_compile_options(tributary_dev_flags INTERFACE
    -fsanitize=${TRIBUTARY_SANITIZER} -fno-omit-frame-pointer -g)
  target_link_options(tributary_dev_flags INTERFACE -fsanitize=${TRIBUTARY_SANITIZER})
  # A sanitized and a plain build share one bin/, and silently overwriting each
  # other there is how you end up benchmarking an ASan binary.
  string(REPLACE "," "_" TRIBUTARY_SAN_SUFFIX ${TRIBUTARY_SANITIZER})
  set(TRIBUTARY_SUFFIX "_${TRIBUTARY_SAN_SUFFIX}")
  if(WIN32 AND TRIBUTARY_SANITIZER MATCHES "address")
    # ASan on Windows does not support the debug CRT. Mixing them aborts during
    # CRT teardown with a bad-free whose stack contains no frames from this
    # project at all -- a confusing failure that looks like a real lifetime bug
    # and is not.
    set(CMAKE_MSVC_RUNTIME_LIBRARY "MultiThreadedDLL")
  endif()
else()
  set(TRIBUTARY_SUFFIX "")
endif()

# --- tests and benchmarks --------------------------------------------------

if(TRIBUTARY_BUILD_TESTS)
  enable_testing()
  if(EXISTS ${CMAKE_CURRENT_SOURCE_DIR}/test/CMakeLists.txt)
    add_subdirectory(test)
  endif()
endif()

if(TRIBUTARY_BUILD_BENCH AND EXISTS ${CMAKE_CURRENT_SOURCE_DIR}/bench/CMakeLists.txt)
  add_subdirectory(bench)
endif()

# --- install ---------------------------------------------------------------

include(GNUInstallDirs)
include(CMakePackageConfigHelpers)

install(TARGETS tributary EXPORT tributary-targets)
install(DIRECTORY include/tributary DESTINATION ${CMAKE_INSTALL_INCLUDEDIR})
install(EXPORT tributary-targets
  FILE tributary-targets.cmake
  NAMESPACE tributary::
  DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/tributary)

write_basic_package_version_file(
  ${CMAKE_CURRENT_BINARY_DIR}/tributary-config-version.cmake
  VERSION ${PROJECT_VERSION}
  COMPATIBILITY SameMajorVersion
  ARCH_INDEPENDENT)

if(EXISTS ${CMAKE_CURRENT_SOURCE_DIR}/cmake/tributary-config.cmake.in)
  configure_package_config_file(
    ${CMAKE_CURRENT_SOURCE_DIR}/cmake/tributary-config.cmake.in
    ${CMAKE_CURRENT_BINARY_DIR}/tributary-config.cmake
    INSTALL_DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/tributary)
  install(FILES
    ${CMAKE_CURRENT_BINARY_DIR}/tributary-config.cmake
    ${CMAKE_CURRENT_BINARY_DIR}/tributary-config-version.cmake
    DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/tributary)
endif()
