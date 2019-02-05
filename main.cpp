#include <irods/rodsClient.h>
#include <irods/filesystem.hpp>
#include <irods/dstream.hpp>

#include <iostream>
#include <memory>
#include <fstream>
#include <array>
#include <stdexcept>
#include <iterator>

#include <boost/filesystem.hpp>

namespace bfs = boost::filesystem;
namespace ifs = irods::experimental::filesystem;

using comm_ptr = std::unique_ptr<rcComm_t, void(*)(rcComm_t*)>;

auto connect_to_irods() -> comm_ptr;
auto put_file(const bfs::path& _from, const ifs::path& _to) -> void;
auto put_directory(const bfs::path& _from, const ifs::path& _to) -> void;

int main(int _argc, char* _argv[])
{
    if (_argc != 2)
    {
        std::cerr << "did you forget to pass something?\n";
        return 0;
    }

    try
    {
        const auto p = bfs::canonical(_argv[1]);
        const ifs::path home = "/tempZone/home/rods";

        if (bfs::is_regular_file(p))
        {
            put_file(p, home / p.filename().string());
        }
        else if (bfs::is_directory(p))
        {
            const auto collection_name = *std::prev(std::end(p));
            put_directory(p, home / collection_name.string());
        }
        else
        {
            std::cerr << "path must point to a file or directory\n";
        }
    }
    catch (const std::exception& e)
    {
        std::cerr << e.what() << '\n';
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

auto put_directory(const bfs::path& _from, const ifs::path& _to) -> void
{
    for (auto&& e : bfs::directory_iterator{_from})
    {
        if (bfs::is_regular_file(e.status()))
        {
            //std::cout << e.path() << " -> " << (_to / e.path().filename().string()) << '\n';
            put_file(e.path(), _to / e.path().filename().string());
        }
        else if (bfs::is_directory(e.status()))
        {
            /*
            const auto dir = std::prev(std::end(e.path()))->string();
            const auto to = _to / dir;
            std::cout << "created " << to << '\n';
            put_directory(e.path(), to);
            */

            const auto dir = std::prev(std::end(e.path()))->string();
            const auto to = _to / dir;

            {
                auto comm = connect_to_irods();
                ifs::create_collections(*comm, to);
            }

            put_directory(e.path(), to);
        }
    }
}

