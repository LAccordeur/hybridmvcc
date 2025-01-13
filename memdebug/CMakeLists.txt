cmake_minimum_required(VERSION 3.16)

project(memdebug)

enable_language(C)

set(SRCLIST memdebug.c)

add_library(memdebug SHARED ${SRCLIST})
target_link_libraries(memdebug)
