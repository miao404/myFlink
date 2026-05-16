## streaming_fuzz - streaming operator fuzz test library
set(TARGET_LIBRARY streaming_fuzz)
file(GLOB_RECURSE SRCS ${CMAKE_CURRENT_SOURCE_DIR}/*.cpp)
add_library(${TARGET_LIBRARY} SHARED ${SRCS})

add_compile_options(-fPIC)

target_include_directories(${TARGET_LIBRARY} PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}
    ${CMAKE_CURRENT_SOURCE_DIR}/../..
    ${SOURCE_ROOT}/test
    ${SOURCE_ROOT}/core/include
)

target_link_libraries(${TARGET_LIBRARY}
    dt_common
    core
    table
    runtime
    streaming
    basictypes
    basicfunctions
    thirdlibrary
    gtest
    ${ORT_VEC_SO}
    ${ORT_CODEGEN_SO}
    ${ORT_OP_SO}
)

install(TARGETS ${TARGET_LIBRARY}
    LIBRARY DESTINATION ${SOURCE_ROOT}/test/dt/installtree/product/lib/)
