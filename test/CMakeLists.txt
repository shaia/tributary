# One binary per suite: a hang or a crash then tells you which area broke
# without bisecting a single monolithic test.

set(TRIBUTARY_TESTS
  test_ring
  test_pipeline
  test_lifetime
  test_backpressure
  test_shutdown
  test_sharding
  test_numa)

foreach(name IN LISTS TRIBUTARY_TESTS)
  add_executable(${name} ${name}.cpp)
  set_target_properties(${name} PROPERTIES OUTPUT_NAME ${name}${TRIBUTARY_SUFFIX})
  target_link_libraries(${name} PRIVATE tributary::tributary tributary_dev_flags)
  target_include_directories(${name} PRIVATE ${CMAKE_CURRENT_SOURCE_DIR})
  add_test(NAME ${name} COMMAND ${name})
  # Concurrency tests that hang are the ones worth catching in CI rather than
  # letting a runner sit for its whole job timeout.
  set_tests_properties(${name} PROPERTIES TIMEOUT 300)
endforeach()
