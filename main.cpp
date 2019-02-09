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

namespace fs   = boost::filesystem;
namespace ifs  = irods::experimental::filesystem;
namespace asio = boost::asio;

class connection_pool
{
private:
    struct connection_context;

    using connection_pointer      = std::unique_ptr<rcComm_t, int(*)(rcComm_t*)>;
    using connection_context_list = std::vector<connection_context>;

public:
    // A wrapper around a connection in the pool.
    // On destruction, the underlying connection is immediately returned
    // to the pool.
    class connection_proxy
    {
    public:
        friend class connection_pool;

        ~connection_proxy()
        {
            pool_.return_connection(index_);
        }

        operator rcComm_t&() const noexcept
        {
            return conn_;
        }

    private:
        connection_proxy(connection_pool& _pool, rcComm_t& _conn, int _index) noexcept
            : pool_{_pool}
            , conn_{_conn}
            , index_{_index}
        {
        }

        connection_pool& pool_;
        rcComm_t& conn_;
        int index_;
    };

    explicit connection_pool(int _size = 4)
        : env_{}
        , conn_ctxs_(_size)
    {
        if (getRodsEnv(&env_) < 0)
            throw std::runtime_error{"cannot get iRODS env"};

        for (auto&& ctx : conn_ctxs_)
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
        for (int i = 0;; i = ++i % conn_ctxs_.size())
        {
            std::unique_lock lock{conn_ctxs_[i].mutex, std::defer_lock};

            if (lock.try_lock())
            {
                if (!conn_ctxs_[i].in_use.load())
                {
                    conn_ctxs_[i].in_use.store(true);

                    return {*this, *conn_ctxs_[i].conn, i};
                }
            }
        }
    }

private:
    struct connection_context
    {
        std::mutex mutex;
        std::atomic<bool> in_use{};
        connection_pointer conn{nullptr, rcDisconnect};
    };

    void return_connection(int _index) noexcept
    {
        conn_ctxs_[_index].in_use.store(false);
    }

    rodsEnv env_;
    connection_context_list conn_ctxs_;
};

constexpr auto operator ""_MB(unsigned long long _x) noexcept -> int;

auto put_file(rcComm_t& _comm, const fs::path& _from, const ifs::path& _to) -> void;

auto put_directory(connection_pool& _conn_pool,
                   asio::thread_pool& _pool,
                   const fs::path& _from,
                   const ifs::path& _to) -> void;

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
            connection_pool conn_pool{1};
            put_file(conn_pool.get_connection(), from, to / from.filename().string());
        }
        else if (fs::is_directory(from))
        {
            connection_pool conn_pool{4};
            asio::thread_pool thread_pool{std::thread::hardware_concurrency()};
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

auto put_file(rcComm_t& _comm, const fs::path& _from, const ifs::path& _to) -> void
{
    try
    {
#if defined(SINGLE_READ_WRITE) || defined(RETURN_FAST_ON_EMPTY_FILES)
        const auto file_size = fs::file_size(_from);

        // If the local file is empty, just create an empty data object
        // on the iRODS server and return.
        if (file_size == 0)
        {
            irods::experimental::odstream{_comm, _to};

            if (!out)
                throw std::runtime_error{"cannot open data object for writing [path: " + _to.string() + ']'};

            return;
        }
#endif

        std::ifstream in{_from.c_str(), std::ios_base::binary};

        if (!in)
            throw std::runtime_error{"cannot open file for reading"};

        irods::experimental::odstream out{_comm, _to};

        if (!out)
            throw std::runtime_error{"cannot open data object for writing [path: " + _to.string() + ']'};

#ifdef SINGLE_READ_WRITE
        std::vector<char> buf(file_size);
#else
        std::array<char, 4_MB> buf{};
#endif

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

auto put_directory(connection_pool& _conn_pool,
                   asio::thread_pool& _thread_pool,
                   const fs::path& _from,
                   const ifs::path& _to) -> void
{
    ifs::create_collections(_conn_pool.get_connection(), _to);

    for (auto&& e : fs::directory_iterator{_from})
    {
        asio::post(_thread_pool, [&_conn_pool, &_thread_pool, e, _to]() {
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

