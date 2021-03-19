include(GoogleTest)


add_subdirectory(googletest)

add_library(main googletest/googletest/src/gtest_main.cc)
link_libraries(gtest main)

include_directories(aTest PRIVATE ${HOTROD_SRC_INCLUDE_DIR})
include_directories(aTest PRIVATE googletest/googletest/include)

set(aTestArgs --foo 1 --bar 2)
add_executable(aTest aTest.cpp)

gtest_discover_tests(aTest EXTRA_ARGS "${aTestArgs}")