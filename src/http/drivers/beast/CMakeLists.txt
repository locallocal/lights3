# Async driver on Boost.Beast/Asio: one io_context per io thread, the project's own
# TlsStream over memory BIOs (docs/architecture/http-adapter.md §3.1)
if(LIGHTS3_DRIVER_BEAST)
  # Beast/Asio are both header-only; BOOST_ROOT or the system path both work.
  # TLS (docs/archive/gaps.md §7): asio::ssl needs libssl
  target_link_libraries(lights3_core PUBLIC OpenSSL::SSL)
  find_package(Boost 1.75 QUIET)
  if(NOT Boost_FOUND)
    find_path(LIGHTS3_BOOST_INCLUDE boost/beast.hpp
              HINTS "$ENV{BOOST_ROOT}" "$ENV{HOME}/.local/opt/boost-1.90")
    if(NOT LIGHTS3_BOOST_INCLUDE)
      message(FATAL_ERROR "LIGHTS3_DRIVER_BEAST=ON but Boost headers not found; "
                          "set BOOST_ROOT or install libboost-dev")
    endif()
  endif()
  target_sources(lights3_core PRIVATE beast_server.cc)
  target_compile_definitions(lights3_core PUBLIC LIGHTS3_DRIVER_BEAST)
  if(Boost_FOUND)
    target_link_libraries(lights3_core PRIVATE Boost::headers)
  else()
    target_include_directories(lights3_core SYSTEM PRIVATE ${LIGHTS3_BOOST_INCLUDE})
  endif()
endif()
