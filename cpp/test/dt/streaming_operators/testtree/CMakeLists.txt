cmake_minimum_required(VERSION 3.16)

#######################################################################
# DT for OmniStream Streaming Operators
#######################################################################
set(BUSINESS_LIB "streaming_fuzz")
set(DTFRAME_DIR_ROOT $ENV{DT_FRAME}/dist)
set(TEST_CONFIG_FILE dtframe.cfg)
set(INSTALL_DIR ${CMAKE_CURRENT_SOURCE_DIR}/dist)
set(ENABLE_COVERAGE ON)
set(AUTO_GEN_DIR registration)

set(TARGET ${BUSINESS_LIB}_dt)

message(DT_FRAME is $ENV{DT_FRAME})
## include dtframe utils
link_directories(${DTFRAME_DIR_ROOT}/lib64)
include_directories(${DTFRAME_DIR_ROOT}/include ${PROJECT_SOURCE_DIR}/test/util)
message("cmd: ${DTFRAME_DIR_ROOT}/bin/dt_register register --config ${TEST_CONFIG_FILE}")
execute_process(
        COMMAND ${DTFRAME_DIR_ROOT}/bin/dt_register register --config ${TEST_CONFIG_FILE}
        WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
)

## build test target
file(GLOB_RECURSE SOURCE_FILES ${AUTO_GEN_DIR}/*.cpp)
include_directories(${CMAKE_CURRENT_SOURCE_DIR}/../sourcetree)

add_library(${TARGET} SHARED ${SOURCE_FILES})

target_link_libraries(${TARGET} dt_common ${BUSINESS_LIB})
target_link_libraries(${TARGET} rttr_core_d Secodefuzz)

install(TARGETS ${TARGET}
        LIBRARY DESTINATION ${INSTALL_DIR})

set(CMAKE_MODULE_PATH ${DTFRAME_DIR_ROOT}/cmake)
# for coverage
include(codecoverage)
setup_coverage_flags()
### lcov report
add_project_coverage_report_with_lcov(
        NAME "coverage_lcov_${BUSINESS_LIB}"
        RUN "${DTFRAME_DIR_ROOT}/bin/dt_engine --config ${TEST_CONFIG_FILE}"
        BASE_DIRECTORY ${CMAKE_SOURCE_DIR}
        EXCLUDE "${CMAKE_SOURCE_DIR}/testtree/*" "${CMAKE_SOURCE_DIR}/sourcetree/include/*.h" "${CMAKE_SOURCE_DIR}/../*"
        GENHTML_TITLE_WITH_GIT
        WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
)
