cmake_minimum_required(VERSION 2.6)
project(nanodbc)

########################################
## Optional Settings
########################################
option(NANODBC_USE_UNICODE "build with unicode support enabled" OFF)
option(NANODBC_HANDLE_NODATA_BUG "enable special handling for SQL_NO_DATA (required for vertica)" OFF)
if(NANODBC_USE_UNICODE)
	message(STATUS "Unicode support: Enabled")
	add_definitions(-DNANODBC_USE_UNICODE)
else()
	message(STATUS "Unicode support: Disabled")
endif()

if(NANODBC_HANDLE_NODATA_BUG)
	message(STATUS "Handle SQL_NO_DATA bug: Enabled")
	add_definitions(-DNANODBC_HANDLE_NODATA_BUG)
else()
	message(STATUS "Handle SQL_NO_DATA bug: Enabled")
endif()

########################################
## Warnings as Errors
########################################
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wall -Werror")

########################################
## OS X Support
########################################
if(APPLE)
	set(CMAKE_MACOSX_RPATH ON)
endif()

########################################
## Mandatory C++11
########################################
add_definitions(--std=c++11)

########################################
## unixODBC or iODBC
########################################
find_program(ODBC_CONFIG odbc_config $ENV{ODBC_PATH}/bin /usr/bin /usr/local/bin PATHS)

if(NOT ODBC_CONFIG)
	find_program(ODBC_CONFIG iodbc-config $ENV{ODBC_PATH}/bin /usr/bin /usr/local/bin PATHS)
endif()

if(NOT ODBC_CONFIG)
	message(FATAL_ERROR "can not find odbc config program")
endif()

execute_process(COMMAND ${ODBC_CONFIG} --cflags OUTPUT_VARIABLE ODBC_COMPILE_FLAGS OUTPUT_STRIP_TRAILING_WHITESPACE)
execute_process(COMMAND ${ODBC_CONFIG} --libs OUTPUT_VARIABLE ODBC_LINK_FLAGS OUTPUT_STRIP_TRAILING_WHITESPACE)
set(CMAKE_SHARED_LINKER_FLAGS "${CMAKE_SHARED_LINKER_FLAGS} ${ODBC_LINK_FLAGS}")

########################################
## Shared Library
########################################
add_library(nanodbc SHARED src/nanodbc.cpp)
set_target_properties(nanodbc PROPERTIES
	COMPILE_FLAGS "${ODBC_COMPILE_FLAGS}"
	LIBRARY_OUTPUT_DIRECTORY "lib"
)

########################################
## Installation Settings
########################################
install(FILES src/nanodbc.h DESTINATION include)
install(TARGETS nanodbc LIBRARY DESTINATION lib)

########################################
## Unit Tests
########################################
enable_testing()
add_subdirectory(test)
add_test(NAME test COMMAND tests)
add_custom_target(check
	COMMAND ${CMAKE_CTEST_COMMAND} --force-new-ctest-process --output-on-failure
	DEPENDS tests)
