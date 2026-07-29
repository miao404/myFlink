## modulefuzz - unified fuzz test library for table and streaming operators
set(TARGET_LIBRARY modulefuzz)
file(GLOB_RECURSE SRCS ${CMAKE_CURRENT_SOURCE_DIR}/*.cpp)
add_library(${TARGET_LIBRARY} SHARED ${SRCS})

add_compile_options(-fPIC)

target_include_directories(${TARGET_LIBRARY} PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}
    ${SOURCE_ROOT}/test
    ${SOURCE_ROOT}/core/include
)

target_link_libraries(${TARGET_LIBRARY}
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
