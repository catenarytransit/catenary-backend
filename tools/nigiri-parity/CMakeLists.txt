cmake_minimum_required(VERSION 3.22)
project(catenary-nigiri-parity LANGUAGES C CXX ASM)

set(NIGIRI_SOURCE_DIR "" CACHE PATH "Checkout of Nigiri 82e55ced with pkg dependencies populated")
if (NOT EXISTS "${NIGIRI_SOURCE_DIR}/deps/CMakeLists.txt")
  message(FATAL_ERROR "Configure the pinned Nigiri checkout once to populate pkg dependencies, then set NIGIRI_SOURCE_DIR")
endif ()

add_subdirectory("${NIGIRI_SOURCE_DIR}/deps" deps)
add_subdirectory("${NIGIRI_SOURCE_DIR}" nigiri)
file(MAKE_DIRECTORY "${CMAKE_CURRENT_BINARY_DIR}/nigiri/generated/gtfsrt")
add_executable(catenary-pong-oracle pong_oracle.cc)
target_link_libraries(catenary-pong-oracle PRIVATE nigiri ianatzdb-res)
target_compile_features(catenary-pong-oracle PRIVATE cxx_std_23)
