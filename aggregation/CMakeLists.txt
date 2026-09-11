cmake_minimum_required(VERSION 3.20)

project(server_framework LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

# 没有显式指定构建类型时，默认使用 Release。
if(NOT CMAKE_BUILD_TYPE AND NOT CMAKE_CONFIGURATION_TYPES)
    set(CMAKE_BUILD_TYPE Release CACHE STRING "Build type" FORCE)
    set_property(CACHE CMAKE_BUILD_TYPE PROPERTY STRINGS
        Debug Release RelWithDebInfo MinSizeRel)
endif()

option(ENABLE_TSAN "Enable ThreadSanitizer" OFF)
option(ENABLE_ASAN "Enable AddressSanitizer" OFF)

if(ENABLE_TSAN AND ENABLE_ASAN)
    message(FATAL_ERROR "ENABLE_TSAN and ENABLE_ASAN cannot be enabled together")
endif()

# 线程库：pthread。
find_package(Threads REQUIRED)

# MariaDB Connector/C 没有提供统一的 CMake package，这里用
# find_path/find_library 做一个 imported target，后续链接 MariaDB::MariaDB。
find_path(MARIADB_INCLUDE_DIR
    NAMES mysql.h
    PATH_SUFFIXES mariadb mariadb/mysql
)
find_library(MARIADB_LIBRARY NAMES mariadb)

if(NOT MARIADB_INCLUDE_DIR)
    message(FATAL_ERROR "mysql.h not found. Install libmariadb-dev.")
endif()
if(NOT MARIADB_LIBRARY)
    message(FATAL_ERROR "libmariadb not found. Install libmariadb-dev.")
endif()

add_library(MariaDB::MariaDB UNKNOWN IMPORTED)
set_target_properties(MariaDB::MariaDB PROPERTIES
    IMPORTED_LOCATION "${MARIADB_LIBRARY}"
    INTERFACE_INCLUDE_DIRECTORIES "${MARIADB_INCLUDE_DIR}"
)

set(FRAMEWORK_SOURCES
    main.cpp
    Server.cpp
    ReactorControl.cpp
    epoll.cpp
    divide_pool.cpp
    work_pool.cpp
    thread_pool.cpp
    blockingqueue.cpp
    EventAwaiter.cpp
    context.cpp
    Handler_epoll_make.cpp
    Handler_divide_make.cpp
    Handler_DB_make.cpp
    DB_pool.cpp
    connect_pool.cpp
    Logger.cpp
    Metrics.cpp
    BatchSender.cpp
    Handler_batch_make.cpp
    NetworkServer.cpp
    Reactor.cpp
    Acceptor.cpp
    connect_book.cpp
    FrameworkCall.cpp
    ConnectionSession.cpp
)

add_executable(server ${FRAMEWORK_SOURCES})

target_include_directories(server PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}
)

target_link_libraries(server PRIVATE
    Threads::Threads
    MariaDB::MariaDB
)

# GCC/Clang 下显式开启协程支持。
include(CheckCXXCompilerFlag)
check_cxx_compiler_flag("-fcoroutines" HAS_FCOROUTINES)
if(HAS_FCOROUTINES)
    target_compile_options(server PRIVATE -fcoroutines)
endif()

if(ENABLE_TSAN)
    target_compile_options(server PRIVATE -fsanitize=thread -g -O1)
    target_link_options(server PRIVATE -fsanitize=thread)
endif()

if(ENABLE_ASAN)
    target_compile_options(server PRIVATE -fsanitize=address -g -O1)
    target_link_options(server PRIVATE -fsanitize=address)
endif()
