#include <irods/rodsClient.h>
#include <irods/filesystem.hpp>
#include <irods/dstream.hpp>
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

#include <boost/filesystem.hpp>
#include <boost/asio.hpp>
#include <boost/asio/thread_pool.hpp>

namespace fs  = boost::filesystem;
namespace ifs = irods::experimental::filesystem;

using comm_ptr = std::unique_ptr<rcComm_t, void(*)(rcComm_t*)>;

constexpr auto operator ""_MB(unsigned long long _x) noexcept -> int;

auto put_file(rcComm_t& _comm, const fs::path& _from, const ifs::path& _to) -> void;
auto put_directory(boost::asio::thread_pool& _pool, const fs::path& _from, const ifs::path& _to) -> void;

class connection_pool
{
private:
    struct conn_context;

    using conn_ptr = std::unique_ptr<rcComm_t, int(*)(rcComm_t*)>;
    using pool_type = std::array<conn_context, 4>;

public:
    class connection_proxy
    {
    public:
        connection_proxy(connection_pool& _pool, rcComm_t& _conn, int _index)
            : pool_{_pool}
            , conn_{_conn}
            , index_{_index}
        {
        }

        ~connection_proxy()
        {
            pool_.return_connection(index_);
        }

        operator rcComm_t&() const noexcept
        {
            return conn_;
        }

    private:
        connection_pool& pool_;
        rcComm_t& conn_;
        int index_;
    };

    connection_pool()
        : env_{}
        , pool_{}
    {
        if (getRodsEnv(&env_) < 0)
            throw std::runtime_error{"cannot get iRODS env"};

        for (auto&& ctx : pool_)
        {
            rErrMsg_t errors;
            ctx.conn.reset(rcConnect(env_.rodsHost, env_.rodsPort, env_.rodsUserName,
                                     env_.rodsZone, 0, &errors));
            if (!ctx.conn)
                throw std::runtime_error{"connect error"};

            char password[] = "rods";
            if (clientLoginWithPassword(ctx.conn.get(), password) != 0)
                throw std::runtime_error{"client login error"};
        }
    }

    connection_proxy get_connection()
    {
        for (int i = 0;; i = ++i % pool_.size())
        {
            std::unique_lock l{pool_[i].mutex, std::defer_lock};

            if (l.try_lock())
            {
                if (!pool_[i].in_use.load())
                {
                    pool_[i].in_use.store(true);
                    l.unlock();

                    return {*this, *pool_[i].conn, i};
                }

                l.unlock();
            }
        }
    }

private:
    struct conn_context
    {
        std::mutex mutex;
        std::atomic<bool> in_use{};
        conn_ptr conn{nullptr, rcDisconnect};
    };

    void return_connection(int _index)
    {
        pool_[_index].in_use.store(false);
    }

    rodsEnv env_;
    pool_type pool_;
};

auto conn_pool = std::make_unique<connection_pool>();

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
        const auto from = fs::canonical(_argv[1]);
        const ifs::path to = _argv[2];

        auto api_table = irods::get_client_api_table();
        auto pck_table = irods::get_pack_table();
        init_api_table(api_table, pck_table);

        if (fs::is_regular_file(from))
        {
            put_file(conn_pool->get_connection(), from, to / from.filename().string());
        }
        else if (fs::is_directory(from))
        {
            boost::asio::thread_pool pool{std::thread::hardware_concurrency()};
            put_directory(pool, from, to / std::rbegin(from)->string());
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

constexpr auto operator ""_MB(unsigned long long _x) noexcept -> int
{
    return _x * 1024 * 1024;
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

        std::array<char, 4_MB> buf{};
        //std::vector<char> buf(fs::file_size(_from));

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
    ifs::create_collections(conn_pool->get_connection(), _to);

    for (auto&& e : fs::directory_iterator{_from})
    {
        boost::asio::post(_pool, [&_pool, s = e.status(), from = e.path(), _to]() {
            if (fs::is_regular_file(s))
            {
                put_file(conn_pool->get_connection(), from, _to / from.filename().string());
            }
            else if (fs::is_directory(s))
            {
                put_directory(_pool, from, _to / std::rbegin(from)->string());
            }
        });
    }
}

