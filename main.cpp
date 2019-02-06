#include <irods/rodsClient.h>
#include <irods/filesystem.hpp>
#include <irods/dstream.hpp>
#include <irods/irods_client_api_table.hpp>
#include <irods/irods_pack_table.hpp>

#include <iostream>
#include <memory>
#include <fstream>
#include <array>
#include <stdexcept>
#include <iterator>
#include <thread>
#include <mutex>

#include <boost/filesystem.hpp>
#include <boost/asio.hpp>
#include <boost/asio/thread_pool.hpp>

namespace fs = boost::filesystem;
namespace ifs = irods::experimental::filesystem;

using comm_ptr = std::unique_ptr<rcComm_t, void(*)(rcComm_t*)>;

auto init_irods_environment() -> void;
auto connect_to_irods() -> comm_ptr;
auto put_file(const fs::path& _from, const ifs::path& _to) -> void;
auto put_file(rcComm_t& _comm, const fs::path& _from, const ifs::path& _to) -> void;
auto put_directory(boost::asio::thread_pool& _pool, const fs::path& _from, const ifs::path& _to) -> void;

rodsEnv env;

int main(int _argc, char* _argv[])
{
    if (_argc != 3)
    {
        std::cerr << "USAGE:\n"
                  << "\tmy_iput <file> <iRODS collection>\n"
                  << "\tmy_iput <directory> <iRODS collection>\n";
        return 0;
    }

    try
    {
        const auto p = fs::canonical(_argv[1]);
        const ifs::path home = _argv[2];

        auto api_table = irods::get_client_api_table();
        auto pck_table = irods::get_pack_table();
        init_api_table(api_table, pck_table);

        init_irods_environment();

        if (fs::is_regular_file(p))
        {
            put_file(p, home / p.filename().string());
        }
        else if (fs::is_directory(p))
        {
            boost::asio::thread_pool pool{std::thread::hardware_concurrency()};
            put_directory(pool, p, home / std::rbegin(p)->string());
            pool.join();
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

auto init_irods_environment() -> void
{
    if (getRodsEnv(&env) < 0)
        throw std::runtime_error{"cannot get iRODS env"};

}

auto connect_to_irods() -> std::unique_ptr<rcComm_t, void(*)(rcComm_t*)>
{
    static std::mutex m;

    comm_ptr comm{nullptr, [](auto* p) { if (p) rcDisconnect(p); }};
    rErrMsg_t errors;

    {
        std::lock_guard lock{m};
        comm.reset(rcConnect(env.rodsHost, env.rodsPort, env.rodsUserName, env.rodsZone, 0, &errors));
    }

    if (!comm)
        throw std::runtime_error{"connect error"};

    char password[] = "rods";
    if (clientLoginWithPassword(comm.get(), password) != 0)
        throw std::runtime_error{"client login error"};

    return comm;
}

auto put_file(const fs::path& _from, const ifs::path& _to) -> void
{
    try
    {
        std::ifstream in{_from.c_str(), std::ios_base::binary};

        if (!in)
            throw std::runtime_error{"cannot open file for reading"};

        auto comm = connect_to_irods();

        if (!comm)
            throw std::runtime_error{"cannot open connection to iRODS server"};

        irods::experimental::odstream out{*comm, _to};

        if (!out)
            throw std::runtime_error{"cannot open data object for writing [path: " + _to.string() + ']'};

        std::array<char, 4096> buf{};

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

auto put_file(rcComm_t& _comm, const fs::path& _from, const ifs::path& _to) -> void
{
    try
    {
        std::ifstream in{_from.c_str(), std::ios_base::binary};

        if (!in)
            throw std::runtime_error{"cannot open file for reading"};

        irods::experimental::odstream out{_comm, _to};

        if (!out)
            throw std::runtime_error{"cannot open data object for writing [path: " + _to.string() + ']'};

        std::array<char, 4096> buf{};

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

auto put_directory(boost::asio::thread_pool& _pool, const fs::path& _from, const ifs::path& _to) -> void
{
    /*
    for (auto&& e : fs::directory_iterator{_from})
    {
        if (fs::is_regular_file(e.status()))
        {
            boost::asio::post(_pool, [p = e.path(), _to]() {
                put_file(p, _to / p.filename().string());
            });
        }
        else if (fs::is_directory(e.status()))
        {
            const auto to = _to / std::rbegin(e.path())->string();
            ifs::create_collections(*connect_to_irods(), to);

            boost::asio::post(_pool, [&_pool, from = e.path(), to]() {
                put_directory(_pool, from, to);
            });
        }
    }
    */

    for (auto&& e : fs::directory_iterator{_from})
    {
        boost::asio::post(_pool, [&_pool, s = e.status(), from = e.path(), _to]() {
            if (fs::is_regular_file(s))
            {
                put_file(from, _to / from.filename().string());
            }
            else if (fs::is_directory(s))
            {
                const auto to = _to / std::rbegin(from)->string();
                ifs::create_collections(*connect_to_irods(), to);
                put_directory(_pool, from, to);
            }
        });
    }

    /*
    for (auto&& e : fs::directory_iterator{_from})
    {
        boost::asio::post(_pool, [&_pool, s = e.status(), from = e.path(), _to]() {
            /
            if (auto comm = connect_to_irods(); comm)
                ifs::exists(*comm, _to);

            const auto to = _to / std::rbegin(from)->string() / from.filename().string();
            if (auto comm = connect_to_irods(); comm)
                ifs::exists(*comm, to);
            /

            if (fs::is_regular_file(s))
            {
                auto comm = connect_to_irods();
                ifs::create_collections(*comm, _to);
                put_file(*comm, from, _to / from.filename().string());
            }
            else if (fs::is_directory(s))
            {
                const auto to = _to / std::rbegin(from)->string();
                ifs::create_collections(*connect_to_irods(), to);
                put_directory(_pool, from, to);
            }
        });
    }
    */
}

