include(${PROJECT_SOURCE_DIR}/cmake/plugin.cmake)

INCLUDE_DIRECTORIES(
  ${PROJECT_SOURCE_DIR}/bbinc
  ${PROJECT_SOURCE_DIR}/util
  ${PROJECT_SOURCE_DIR}/db
  ${CMAKE_CURRENT_BINARY_DIR}
  ${CMAKE_CURRENT_SOURCE_DIR}
)
add_plugin(kafkamsgqueue STATIC kafkamsgqueue.c)

