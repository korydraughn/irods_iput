#include <irods/rodsClient.h>
#include <irods/filesystem.hpp>
#include <irods/dstream.hpp>
#include <irods/connection_pool.hpp>
#include <irods/thread_pool.hpp>
#include <irods/irods_client_api_table.hpp>
#include <irods/irods_pack_table.hpp>

#include <iostream>
#include <memory>
#include <array>
#include <vector>
#include <stdexcept>
#include <iterator>
#include <thread>
#include <mutex>
#include <atomic>
#include <algorithm>

#include <boost/program_options.hpp>
#include <boost/filesystem.hpp>
#include <boost/asio.hpp>

namespace po   = boost::program_options;
namespace fs   = boost::filesystem;
namespace asio = boost::asio;

namespace ifs  = irods::experimental::filesystem;

constexpr auto operator ""_MB(unsigned long long _x) noexcept -> int;

auto put_file_chunk(irods::connection_pool& _conn_pool,
                    const fs::path& _from,
                    const ifs::path& _to,
                    unsigned long _offset,
                    unsigned long _chunk_size) -> void;
auto put_file(const rodsEnv& _env, const fs::path& _from, const ifs::path& _to) -> void;
auto put_file(rcComm_t& _comm, const fs::path& _from, const ifs::path& _to) -> void;

auto put_directory(irods::connection_pool& _conn_pool,
                   irods::thread_pool& _pool,
                   const fs::path& _from,
                   const ifs::path& _to) -> void;

int main(int _argc, char* _argv[])
{
    rodsEnv env;

    if (getRodsEnv(&env) < 0)
    {
        std::cerr << "cannot get iRODS env\n";
        return 1;
    }

    po::options_description desc{"Allowed options"};
    desc.add_options()
        ("help,h", "produce help message")
        ("src,s", po::value<std::string>()->required(), "local file/directory")
        ("dst,d", po::value<std::string>()->default_value(env.rodsHome), "iRODS collection [defaults to home collection]")
        ("connection_pool_size,c", po::value<int>()->default_value(4), "connection pool size for directories");

    po::positional_options_description pod;
    pod.add("src", 1);
    pod.add("dst", 2);

    po::variables_map vm;
    po::store(po::command_line_parser{_argc, _argv}.options(desc).positional(pod).run(), vm);
    po::notify(vm);

    if (vm.count("help"))
    {
        std::cout << desc << '\n';
        return 0;
    }

    try
    {
        const auto from = fs::canonical(vm["src"].as<std::string>());
        const ifs::path to = vm["dst"].as<std::string>();

        auto api_table = irods::get_client_api_table();
        auto pck_table = irods::get_pack_table();
        init_api_table(api_table, pck_table);

        if (fs::is_regular_file(from))
        {
            put_file(env, from, to / from.filename().string());
        }
        else if (fs::is_directory(from))
        {
            const auto pool_size = vm["connection_pool_size"].as<int>();
            irods::connection_pool conn_pool{pool_size, env.rodsHost, env.rodsPort, env.rodsUserName, env.rodsZone};
            irods::thread_pool thread_pool{static_cast<int>(std::thread::hardware_concurrency())};
            put_directory(conn_pool, thread_pool, from, to / std::rbegin(from)->string());
            thread_pool.join();
        }
        else
        {
            std::cerr << "path must point to a file or directory\n";
            return 1;
        }
    }
    catch (const std::exception& e)
    {
        std::cerr << e.what() << '\n';
        return 1;
    }

    return 0;
}

constexpr auto operator ""_MB(unsigned long long _x) noexcept -> int
{
    return _x * 1024 * 1024;
}

auto put_file_chunk(irods::connection_pool& _cpool,
                    const fs::path& _from,
                    const ifs::path& _to,
                    unsigned long _offset,
                    unsigned long _chunk_size) -> void
{
    try
    {
        std::ifstream in{_from.c_str(), std::ios_base::binary};

        if (!in)
            throw std::runtime_error{"cannot open file for reading"};

        auto conn = _cpool.get_connection();
        irods::experimental::odstream out{conn, _to};

        if (!out)
            throw std::runtime_error{"cannot open data object for writing [path: " + _to.string() + ']'};

        in.seekg(_offset);
        out.seekp(_offset);

        std::array<char, 4_MB> buf{};
        unsigned long bytes_pushed = 0;

        while (in && bytes_pushed < _chunk_size)
        {
            in.read(buf.data(), std::min(buf.size(), _chunk_size));
            out.write(buf.data(), in.gcount());
            bytes_pushed += in.gcount();
        }
    }
    catch (const std::exception& e)
    {
        std::cerr << e.what() << '\n';
    }
}

auto put_file(const rodsEnv& _env, const fs::path& _from, const ifs::path& _to) -> void
{
    try
    {
        const auto file_size = fs::file_size(_from);

        // If the local file's size is less than 32MB, then stream the file
        // over a single connection.
        if (file_size < 32_MB)
        {
            irods::connection_pool cpool{1, _env.rodsHost, _env.rodsPort, _env.rodsUserName, _env.rodsZone};

            // If the local file is empty, just create an empty data object
            // on the iRODS server and return.
            if (file_size == 0)
            {
                irods::experimental::odstream out{cpool.get_connection(), _to};

                if (!out)
                    throw std::runtime_error{"cannot open data object for writing [path: " + _to.string() + ']'};

                return;
            }

            put_file(cpool.get_connection(), _from, _to);

            return;
        }

        using int_type = unsigned long;

        constexpr int_type thread_count = 3;
        irods::connection_pool cpool{thread_count, _env.rodsHost, _env.rodsPort, _env.rodsUserName, _env.rodsZone};
        irods::thread_pool tpool{static_cast<int>(thread_count)};

        const int_type chunk_size = file_size / thread_count;
        const int_type remainder = file_size % thread_count;

        { irods::experimental::odstream target{cpool.get_connection(), _to}; }

        for (int_type i = 0; i < thread_count; ++i)
        {
            irods::thread_pool::post(tpool, [&, offset = i * chunk_size] {
                put_file_chunk(cpool, _from, _to, offset, chunk_size);
            });
        }

        if (remainder > 0)
        {
            irods::thread_pool::post(tpool, [&, offset = thread_count * chunk_size] {
                put_file_chunk(cpool, _from, _to, offset, remainder);
            });
        }

        tpool.join();
    }
    catch (const std::exception& e)
    {
        std::cerr << e.what() << '\n';
    }
}

auto put_file(rcComm_t& _comm, const fs::path& _from, const ifs::path& _to) -> void
{
    try
    {
        const auto file_size = fs::file_size(_from);

        // If the local file is empty, just create an empty data object
        // on the iRODS server and return.
        if (file_size == 0)
        {
            irods::experimental::odstream out{_comm, _to};

            if (!out)
                throw std::runtime_error{"cannot open data object for writing [path: " + _to.string() + ']'};

            return;
        }

        std::ifstream in{_from.c_str(), std::ios_base::binary};

        if (!in)
            throw std::runtime_error{"cannot open file for reading"};

        irods::experimental::odstream out{_comm, _to};

        if (!out)
            throw std::runtime_error{"cannot open data object for writing [path: " + _to.string() + ']'};

        std::array<char, 4_MB> buf{};

        while (in)
        {
            in.read(buf.data(), buf.size());
            out.write(buf.data(), in.gcount());
        }
    }
    catch (const std::exception& e)
    {
        std::cerr << e.what() << '\n';
    }
}

auto put_directory(irods::connection_pool& _conn_pool,
                   irods::thread_pool& _thread_pool,
                   const fs::path& _from,
                   const ifs::path& _to) -> void
{
    ifs::create_collections(_conn_pool.get_connection(), _to);

    for (auto&& e : fs::directory_iterator{_from})
    {
        irods::thread_pool::post(_thread_pool, [&_conn_pool, &_thread_pool, e, _to]() {
            const auto& from = e.path();

            if (fs::is_regular_file(e.status()))
            {
                put_file(_conn_pool.get_connection(), from, _to / from.filename().string());
            }
            else if (fs::is_directory(e.status()))
            {
                put_directory(_conn_pool, _thread_pool, from, _to / std::rbegin(from)->string());
            }
        });
    }
}

