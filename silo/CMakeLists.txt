cmake_minimum_required(VERSION 3.10)

project(ccbench_silo
        VERSION 0.0.1
        DESCRIPTION "silo of ccbench"
        LANGUAGES CXX)

list(APPEND CMAKE_MODULE_PATH "${CMAKE_SOURCE_DIR}/../cmake")

option(ENABLE_SANITIZER "enable sanitizer on debug build" ON)
option(ENABLE_UB_SANITIZER "enable undefined behavior sanitizer on debug build" OFF)
option(ENABLE_COVERAGE "enable coverage on debug build" OFF)

find_package(Doxygen)
find_package(Threads REQUIRED)
find_package(gflags REQUIRED)
find_package(glog REQUIRED)
find_package(Boost
        COMPONENTS filesystem)

include(GNUInstallDirs)
include(CMakePackageConfigHelpers)
include(CompileOptions)

file(GLOB SILO_SOURCES
        "../common/result.cc"
        "../common/util.cc"
        "result.cc"
        "silo.cc"
        "transaction.cc"
        "util.cc"
        "log_buffer.cc"
        "logger.cc"
        "notifier.cc"
        )

file(GLOB REPLAY_SOURCES
        "replayTest.cc"
        )

add_executable(silo.exe ${SILO_SOURCES})
add_executable(replay_test.exe ${REPLAY_SOURCES})

target_link_libraries(silo.exe
        Boost::filesystem
        gflags::gflags
        ${PROJECT_SOURCE_DIR}/../third_party/mimalloc/out/release/libmimalloc.a
        ${PROJECT_SOURCE_DIR}/../third_party/masstree/libkohler_masstree_json.a
        Threads::Threads
        numa
        )

if (DEFINED ADD_ANALYSIS)
    add_definitions(-DADD_ANALYSIS=${ADD_ANALYSIS})
else ()
    add_definitions(-DADD_ANALYSIS=0)
endif ()

if (DEFINED BACK_OFF)
    add_definitions(-DBACK_OFF=${BACK_OFF})
else ()
    add_definitions(-DBACK_OFF=0)
endif ()

if (DEFINED KEY_SIZE)
    add_definitions(-DKEY_SIZE=${KEY_SIZE})
else ()
    add_definitions(-DKEY_SIZE=8)
endif ()

if (DEFINED MASSTREE_USE)
    add_definitions(-DMASSTREE_USE=${MASSTREE_USE})
else ()
    add_definitions(-DMASSTREE_USE=1)
endif ()

if (DEFINED NO_WAIT_LOCKING_IN_VALIDATION)
    add_definitions(-DNO_WAIT_LOCKING_IN_VALIDATION=${NO_WAIT_LOCKING_IN_VALIDATION})
else ()
    add_definitions(-DNO_WAIT_LOCKING_IN_VALIDATION=1)
endif ()

if (DEFINED NO_WAIT_OF_TICTOC)
    add_definitions(-DNO_WAIT_OF_TICTOC=${NO_WAIT_OF_TICTOC})
else ()
    add_definitions(-DNO_WAIT_OF_TICTOC=0)
endif ()

if (DEFINED PARTITION_TABLE)
    add_definitions(-DPARTITION_TABLE=${PARTITION_TABLE})
else ()
    add_definitions(-DPARTITION_TABLE=0)
endif ()

if (DEFINED PROCEDURE_SORT)
    add_definitions(-DPROCEDURE_SORT=${PROCEDURE_SORT})
else ()
    add_definitions(-DPROCEDURE_SORT=0)
endif ()

if (DEFINED SLEEP_READ_PHASE)
    add_definitions(-DSLEEP_READ_PHASE=${SLEEP_READ_PHASE})
else ()
    add_definitions(-DSLEEP_READ_PHASE=0)
endif ()

if (DEFINED VAL_SIZE)
    add_definitions(-DVAL_SIZE=${VAL_SIZE})
else ()
    add_definitions(-DVAL_SIZE=4)
endif ()

if (DEFINED WAL)
    add_definitions(-DWAL=${WAL})
else ()
    add_definitions(-DWAL=0)
endif ()

if (DEFINED WALPMEM)
    add_definitions(-DWALPMEM=${WALPMEM})
    target_link_libraries(silo.exe pmem)
    target_link_libraries(replay_test.exe pmem)
else ()
    add_definitions(-DWALMEM=0)
endif ()

if (DEFINED PMEMCPY)
    add_definitions(-DPMEMCPY=${PMEMCPY})
else ()
    add_definitions(-DPMEMCPY=0)
endif ()

if (DEFINED DURABLE_EPOCH)
    add_definitions(-DDURABLE_EPOCH=${DURABLE_EPOCH})
else ()
    add_definitions(-DDURABLE_EPOCH=0)
endif ()

if (DEFINED NOTIFIER_THREAD)
    add_definitions(-DNOTIFIER_THREAD=${NOTIFIER_THREAD})
else ()
    add_definitions(-DNOTIFIER_THREAD=0)
endif ()

if (DEFINED NOLOG)
    add_definitions(-DNOLOG=${NOLOG})
else ()
    add_definitions(-DNOLOG=0)
endif ()

if (DEFINED DEQ_MIN_EPOCH)
    add_definitions(-DDEQ_MIN_EPOCH=${DEQ_MIN_EPOCH})
else ()
    add_definitions(-DDEQ_MIN_EPOCH=1)
endif ()

if (DEFINED EPOCH_DIFF)
    add_definitions(-DEPOCH_DIFF=${EPOCH_DIFF})
else ()
    add_definitions(-DEPOCH_DIFF=1)
endif ()
