#include "catch.hpp"

#include "test/base_test_fixture.h"
#include <cstdio>

namespace
{
    // According to the sqliteodbc documentation,
    // driver name is different on Windows and Unix.
    #ifdef _WIN32
        const nanodbc::string_type driver_name(NANODBC_TEXT("SQLite3 ODBC Driver"));
    #else
        const nanodbc::string_type driver_name(NANODBC_TEXT("SQLite3"));
    #endif
    const nanodbc::string_type connection_string
        = NANODBC_TEXT("Driver=") + driver_name
        + NANODBC_TEXT(";Database=nanodbc.db;");

    struct sqlite_fixture : public base_test_fixture
    {
        sqlite_fixture()
        : base_test_fixture(connection_string)
        {
            sqlite_cleanup(); // in case prior test exited without proper cleanup
        }

        virtual ~sqlite_fixture() NANODBC_NOEXCEPT
        {
            sqlite_cleanup();
        }

        void sqlite_cleanup() NANODBC_NOEXCEPT
        {
            int success = std::remove("nanodbc.db");
            (void)success;

        }
    };
}

// NOTE: No catlog_* tests; not supported by SQLite.


// Unicode build on Ubuntu 12.04 with unixODBC 2.2.14p2 and libsqliteodbc 0.91-3 throws:
// test/sqlite_test.cpp:42: FAILED:
// due to a fatal error condition:
//   SIGSEGV - Segmentation violation signal
// See discussions at
// https://github.com/lexicalunit/nanodbc/pull/154
// https://groups.google.com/forum/#!msg/catch-forum/7tIpgm8SvDA/1QZZESIuCQAJ
// TODO: Uncomment as soon as the SIGSEGV issue has been fixed.
#ifndef NANODBC_USE_UNICODE
TEST_CASE_METHOD(sqlite_fixture, "affected_rows_test", "[sqlite][affected_rows]")
{
    nanodbc::connection conn = connect();

    // CREATE TABLE  (CREATE DATABASE not supported)
    {
        auto result = execute(conn, NANODBC_TEXT("CREATE TABLE nanodbc_test_temp_table (i int)"));
        REQUIRE(result.affected_rows() == 0);
    }
    // INSERT
    {
        nanodbc::result result;
        result = execute(conn, NANODBC_TEXT("INSERT INTO nanodbc_test_temp_table VALUES (1)"));
        REQUIRE(result.affected_rows() == 1);
        result = execute(conn, NANODBC_TEXT("INSERT INTO nanodbc_test_temp_table VALUES (2)"));
        REQUIRE(result.affected_rows() == 1);
    }
    // SELECT
    {
        //auto result = execute(conn, NANODBC_TEXT("SELECT i FROM nanodbc_test_temp_table"));
        //REQUIRE(result.affected_rows() == 0);
    }
    // DELETE
    {
        auto result = execute(conn, NANODBC_TEXT("DELETE FROM nanodbc_test_temp_table"));
        REQUIRE(result.affected_rows() == 2);
        // then DROP TABLE
        {
            auto result2 = execute(conn, NANODBC_TEXT("DROP TABLE nanodbc_test_temp_table"));
            REQUIRE(result2.affected_rows() == 2);
        }
    }

    // DROP TABLE, without prior DELETE
    {
        execute(conn, NANODBC_TEXT("CREATE TABLE nanodbc_test_temp_table (i int)"));
        execute(conn, NANODBC_TEXT("INSERT INTO nanodbc_test_temp_table VALUES (1)"));
        execute(conn, NANODBC_TEXT("INSERT INTO nanodbc_test_temp_table VALUES (2)"));

        auto result = execute(conn, NANODBC_TEXT("DROP TABLE nanodbc_test_temp_table"));
        REQUIRE(result.affected_rows() == 1);
    }
}
#endif

TEST_CASE_METHOD(sqlite_fixture, "blob_test", "[sqlite][blob]")
{
    blob_test();
}

TEST_CASE_METHOD(sqlite_fixture, "dbms_info_test", "[sqlite][dmbs][metadata][info]")
{
    dbms_info_test();

    // Additional SQLite-specific checks
    nanodbc::connection connection = connect();
    REQUIRE(connection.dbms_name() == NANODBC_TEXT("SQLite"));
}

TEST_CASE_METHOD(sqlite_fixture, "decimal_conversion_test", "[sqlite][decimal][conversion]")
{
    // SQLite ODBC driver requires dedicated test.
    // The driver converts SQL DECIMAL value to C float value
    // without preserving DECIMAL(N,M) dimensions
    // and strips any trailing zeros.
    nanodbc::connection connection = connect();
    nanodbc::result results;

    drop_table(connection, NANODBC_TEXT("decimal_conversion_test"));
    execute(connection, NANODBC_TEXT("create table decimal_conversion_test (d decimal(9, 3));"));
    execute(connection, NANODBC_TEXT("insert into decimal_conversion_test values (12345.987);"));
    execute(connection, NANODBC_TEXT("insert into decimal_conversion_test values (5.6);"));
    execute(connection, NANODBC_TEXT("insert into decimal_conversion_test values (1);"));
    execute(connection, NANODBC_TEXT("insert into decimal_conversion_test values (-1.333);"));
    results = execute(connection, NANODBC_TEXT("select * from decimal_conversion_test order by 1 desc;"));

    REQUIRE(results.next());
    REQUIRE(results.get<nanodbc::string_type>(0) == NANODBC_TEXT("12345.987"));

    REQUIRE(results.next());
    REQUIRE(results.get<nanodbc::string_type>(0) == NANODBC_TEXT("5.6"));

    REQUIRE(results.next());
    REQUIRE(results.get<nanodbc::string_type>(0) == NANODBC_TEXT("1"));

    REQUIRE(results.next());
    REQUIRE(results.get<nanodbc::string_type>(0) == NANODBC_TEXT("-1.333"));
}

TEST_CASE_METHOD(sqlite_fixture, "exception_test", "[sqlite][exception]")
{
    exception_test();
}

TEST_CASE_METHOD(sqlite_fixture, "execute_multiple_transaction_test", "[sqlite][execute][transaction]")
{
    execute_multiple_transaction_test();
}

TEST_CASE_METHOD(sqlite_fixture, "execute_multiple_test", "[sqlite][execute]")
{
    execute_multiple_test();
}

TEST_CASE_METHOD(sqlite_fixture, "integral_test", "[sqlite][integral]")
{
    integral_test<sqlite_fixture>();
}

TEST_CASE_METHOD(sqlite_fixture, "integral_boundary_test", "[sqlite][integral]")
{
    nanodbc::connection connection = connect();
    drop_table(connection, NANODBC_TEXT("integral_boundary_test"));

    // SQLite3 uses single storage class INTEGER for all integral SQL types 
    execute(connection, NANODBC_TEXT("create table integral_boundary_test(i1 integer,i2 integer,i4 integer,i8 integer);"));
    nanodbc::statement statement(connection);
    prepare(statement, NANODBC_TEXT("insert into integral_boundary_test(i1,i2,i4,i8) values (?,?,?,?);"));

    // min
    std::int16_t const i1min = std::numeric_limits<std::int8_t>::min();
    auto const i2min = std::numeric_limits<std::int16_t>::min();
    auto const i4min = std::numeric_limits<std::int32_t>::min();
    auto const i8min = std::numeric_limits<std::int64_t>::min();
    statement.bind(0, &i1min);
    statement.bind(1, &i2min);
    statement.bind(2, &i4min);
    statement.bind(3, &i8min);
    REQUIRE(statement.connected());
    execute(statement);

    // max insert
    std::int16_t const i1max = std::numeric_limits<std::int8_t>::max();
    auto const i2max = std::numeric_limits<std::int16_t>::max();
    auto const i4max = std::numeric_limits<std::int32_t>::max();
    auto const i8max = std::numeric_limits<std::int64_t>::max();
    statement.bind(0, &i1max);
    statement.bind(1, &i2max);
    statement.bind(2, &i4max);
    statement.bind(3, &i8max);
    REQUIRE(statement.connected());
    execute(statement);

    // query
    nanodbc::result result = execute(connection, NANODBC_TEXT("select i1,i2,i4,i8 from integral_boundary_test order by i1 asc;"));
    // min
    REQUIRE(result.next());
    // All of string converted values are incorrect
    //auto si1min = result.get<nanodbc::string_type>(0);
    //auto si2min = result.get<nanodbc::string_type>(1);
    //auto si4min = result.get<nanodbc::string_type>(2);
    //auto si8min = result.get<nanodbc::string_type>(3);
    REQUIRE(result.get<std::int16_t>(0) == static_cast<std::int16_t>(i1min));
    REQUIRE(result.get<std::int16_t>(1) == i2min);
    REQUIRE(result.get<std::int32_t>(2) == i4min);
    REQUIRE(result.get<std::int64_t>(3) == i8min);
    // max
    REQUIRE(result.next());
    REQUIRE(result.get<std::int16_t>(0) == static_cast<std::int16_t>(i1max));
    REQUIRE(result.get<std::int16_t>(1) == i2max);
    REQUIRE(result.get<std::int32_t>(2) == i4max);
    REQUIRE(result.get<std::int64_t>(3) == i8max);
    // query end
    REQUIRE(!result.next());
}

TEST_CASE_METHOD(sqlite_fixture, "move_test", "[sqlite][move]")
{
    move_test();
}

TEST_CASE_METHOD(sqlite_fixture, "null_test", "[sqlite][null]")
{
    null_test();
}

TEST_CASE_METHOD(sqlite_fixture, "nullptr_nulls_test", "[sqlite][null]")
{
    nullptr_nulls_test();
}

TEST_CASE_METHOD(sqlite_fixture, "result_iterator_test", "[sqlite][iterator]")
{
    result_iterator_test();
}

TEST_CASE_METHOD(sqlite_fixture, "simple_test", "[sqlite]")
{
    simple_test();
}

TEST_CASE_METHOD(sqlite_fixture, "string_test", "[sqlite][string]")
{
    string_test();
}

TEST_CASE_METHOD(sqlite_fixture, "transaction_test", "[sqlite][transaction]")
{
    transaction_test();
}

TEST_CASE_METHOD(sqlite_fixture, "while_not_end_iteration_test", "[sqlite][looping]")
{
    while_not_end_iteration_test();
}

TEST_CASE_METHOD(sqlite_fixture, "while_next_iteration_test", "[sqlite][looping]")
{
    while_next_iteration_test();
}
