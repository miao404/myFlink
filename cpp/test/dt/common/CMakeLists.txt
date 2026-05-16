## dt_common - shared fuzz utilities
set(TARGET_LIBRARY dt_common)
file(GLOB_RECURSE SRCS ${CMAKE_CURRENT_SOURCE_DIR}/*.cpp)
add_library(${TARGET_LIBRARY} SHARED ${SRCS})

add_compile_options(-fPIC)

target_include_directories(${TARGET_LIBRARY} PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}/..
    ${CMAKE_CURRENT_SOURCE_DIR}/../..
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
    ${ORT_VEC_SO}
    ${ORT_CODEGEN_SO}
    ${ORT_OP_SO}
)

install(TARGETS ${TARGET_LIBRARY}
    LIBRARY DESTINATION ${CMAKE_CURRENT_SOURCE_DIR}/../installtree/product/lib/)
