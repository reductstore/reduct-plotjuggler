cmake_minimum_required(VERSION 3.20)
project(reduct_examples CXX)
set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

include(FetchContent)

# Fetch Reduct C++ SDK
set(REDUCT_CPP_USE_FETCHCONTENT ON CACHE BOOL "" FORCE)
FetchContent_Declare(
  reductcpp
  GIT_REPOSITORY https://github.com/reductstore/reduct-cpp
  GIT_TAG main
)
FetchContent_MakeAvailable(reductcpp)

# Fetch nlohmann/json
FetchContent_Declare(
  nlohmann_json
  GIT_REPOSITORY https://github.com/nlohmann/json
  GIT_TAG v3.11.3
)
FetchContent_MakeAvailable(nlohmann_json)

# Build each .cc file in src/ as an executable
file(GLOB SRC_FILES CONFIGURE_DEPENDS "${CMAKE_SOURCE_DIR}/src/*.cc")
foreach(src_file ${SRC_FILES})
  get_filename_component(exec_name ${src_file} NAME_WE)
  add_executable(${exec_name} ${src_file})
  target_link_libraries(${exec_name} PRIVATE
    reductcpp::reductcpp
    nlohmann_json::nlohmann_json
  )
endforeach()

# Put binaries in build/bin
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/bin)
