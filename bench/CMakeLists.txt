# One binary, one translation unit per phase: a hang or a crash names the phase
# without bisecting, and each phase's argument stands on its own.
#
# Deliberately NOT registered with add_test(). The test suite runs in ~2 s and
# has to stay that way to be worth running on every edit; this is minutes long,
# and it is the Phase A gate rather than a unit test. Run it explicitly:
#
#   cmake --build build && ./bin/tributary_bench

add_executable(tributary_bench
  main.cpp
  harness.cpp
  correctness.cpp
  throughput.cpp
  backpressure.cpp
  scan_ab.cpp)

# Shares bin/ with the test binaries, so the sanitizer suffix matters here for
# the same reason: benchmarking an ASan binary by accident is an easy mistake.
set_target_properties(tributary_bench PROPERTIES
  OUTPUT_NAME tributary_bench${TRIBUTARY_SUFFIX})

target_link_libraries(tributary_bench PRIVATE tributary::tributary tributary_dev_flags)
target_include_directories(tributary_bench PRIVATE ${CMAKE_CURRENT_SOURCE_DIR})
