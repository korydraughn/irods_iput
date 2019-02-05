#include <irods/rodsClient.h>
#include <irods/filesystem.hpp>
#include <irods/dstream.hpp>

#include <iostream>
#include <memory>
#include <fstream>
#include <array>
#include <stdexcept>
#include <iterator>
#include <thread>

#include <boost/filesystem.hpp>
#include <boost/asio.hpp>
#include <boost/asio/thread_pool.hpp>

namespace bfs = boost::filesystem;
namespace ifs = irods::experimental::filesystem;

using comm_ptr = std::unique_ptr<rcComm_t, void(*)(rcComm_t*)>;

auto connect_to_irods() -> comm_ptr;
auto put_file(const bfs::path& _from, const ifs::path& _to) -> void;
auto put_directory(boost::asio::thread_pool& _pool, const bfs::path& _from, const ifs::path& _to) -> void;

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
        const auto p = bfs::canonical(_argv[1]);
        const ifs::path home = _argv[2];

        if (bfs::is_regular_file(p))
        {
            put_file(p, home / p.filename().string());
        }
        else if (bfs::is_directory(p))
        {
            //std::cout << "hardware concurrency = " << std::thread::hardware_concurrency() << '\n';
            //boost::asio::thread_pool pool{std::thread::hardware_concurrency()};
            boost::asio::thread_pool pool;
            const auto collection_name = *std::prev(std::end(p));
            put_directory(pool, p, home / collection_name.string());
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

auto connect_to_irods() -> std::unique_ptr<rcComm_t, void(*)(rcComm_t*)>
{
    rodsEnv env;
    getRodsEnv(&env);

    comm_ptr comm{nullptr, [](auto* p) { if (p) rcDisconnect(p); }};

    rErrMsg_t errors;
    comm.reset(rcConnect(env.rodsHost, env.rodsPort, env.rodsUserName, env.rodsZone, 0, &errors));

    char password[] = "rods";
    clientLoginWithPassword(comm.get(), password);

    return comm;
}

auto put_file(const bfs::path& _from, const ifs::path& _to) -> void
{
    std::ifstream in{_from.c_str(), std::ios_base::binary};

    if (!in)
        throw std::runtime_error{"cannot open file for reading"};

    auto comm = connect_to_irods();

    if (!comm)
        throw std::runtime_error{"cannot open connection to iRODS server"};

    irods::experimental::odstream out{*comm, _to};

    if (!out)
        throw std::runtime_error{"cannot open data object for writing"};

    std::array<char, 4096> buf{};

    while (in)
    {
        in.read(buf.data(), buf.size());
        out.write(buf.data(), in.gcount());
    }
}

auto put_directory(boost::asio::thread_pool& _pool, const bfs::path& _from, const ifs::path& _to) -> void
{
    for (auto&& e : bfs::directory_iterator{_from})
    {
        if (bfs::is_regular_file(e.status()))
        {
            boost::asio::post(_pool, [p = e.path(), _to]() {
                put_file(p, _to / p.filename().string());
            });
        }
        else if (bfs::is_directory(e.status()))
        {
            boost::asio::post(_pool, [&_pool, p = e.path(), _to]() {
                const auto dir = std::prev(std::end(p))->string();
                const auto to = _to / dir;

                ifs::create_collections(*connect_to_irods(), to);
                put_directory(_pool, p, to);
            });
        }
    }
}

