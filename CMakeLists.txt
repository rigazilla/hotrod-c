cmake_minimum_required(VERSION 3.0.0)
project(hotrod-c VERSION 0.1.0 DESCRIPTION "A C library for Infinispan hotrod protocol")

include(CTest)
include(CPack)
enable_testing()

add_library(hotrod-c src/hotrod-c.cpp src/murmurHash3.cpp)
target_include_directories(hotrod-c PUBLIC include src)

set(HOTROD_SRC_INCLUDE_DIR ${PROJECT_SOURCE_DIR}/include)

add_executable(hotrod-example example/hotrodExample.cpp)
target_include_directories(hotrod-example PRIVATE include src)
target_link_libraries(hotrod-example hotrod-c)

add_subdirectory(test)

# check if Doxygen is installed
find_package(Doxygen)
if (DOXYGEN_FOUND)
    # set input and output files
    set(DOXYGEN_IN ${CMAKE_CURRENT_SOURCE_DIR}/docs/Doxyfile.in)
    set(DOXYGEN_OUT ${CMAKE_CURRENT_BINARY_DIR}/Doxyfile)

    # request to configure the file
    configure_file(${DOXYGEN_IN} ${DOXYGEN_OUT} @ONLY)
    message("Doxygen build started")

    # note the option ALL which allows to build the docs together with the application
    add_custom_target( doc_doxygen ALL
        COMMAND ${DOXYGEN_EXECUTABLE} ${DOXYGEN_OUT}
        WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}
        COMMENT "Generating API documentation with Doxygen"
        VERBATIM )
else (DOXYGEN_FOUND)
  message("Doxygen need to be installed to generate the doxygen documentation")
endif (DOXYGEN_FOUND)


set(CPACK_PROJECT_NAME ${PROJECT_NAME})
set(CPACK_PROJECT_VERSION ${PROJECT_VERSION})
