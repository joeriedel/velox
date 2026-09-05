# End-to-end test for the `auto` exchange transport.
#
# Deliberately not a test target: no add_test(), so it never runs in ctest. It
# needs a UCX listener and a free port, which is not something a unit test
# suite should be arranging.
#
# Requires VELOX_BUILD_TESTING for PlanBuilder, and VELOX_BUILD_LAB to be
# switched on, which it is not by default.
if(VELOX_ENABLE_UCX_EXCHANGE)
  add_executable(velox_lab_transport_selection AutoTransportSelectionLab.cpp)

  target_link_libraries(
    velox_lab_transport_selection
    PRIVATE
      velox_ucx_exchange
      velox_exec
      velox_exec_test_lib
      velox_presto_serializer
      velox_functions_prestosql
      velox_vector_test_lib
      Folly::folly
      fmt::fmt
      ucxx::ucxx
  )

  # Scenario D exercises the cuDF producer writing device pages into the stock
  # buffer. Compiled in only when cuDF is enabled; skipped at runtime
  # otherwise, so the same program is the whole test either way.
  if(VELOX_ENABLE_CUDF)
    target_link_libraries(
      velox_lab_transport_selection
      PRIVATE velox_cudf_exec cudf::cudf CUDA::cudart
    )
  endif()
endif()
