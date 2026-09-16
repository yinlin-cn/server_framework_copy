cmake_minimum_required(VERSION 3.20)

project(server_framework LANGUAGES CXX)

# 默认安装到项目根目录，安装结果落在 <project>/bin；
# 需要换位置时用 -DCMAKE_INSTALL_PREFIX=... 或 cmake --install build --prefix ...。
if(CMAKE_INSTALL_PREFIX_INITIALIZED_TO_DEFAULT)
    set(CMAKE_INSTALL_PREFIX "${CMAKE_SOURCE_DIR}"
        CACHE PATH "Install prefix" FORCE)
endif()

add_subdirectory(lib)
add_subdirectory(src)
