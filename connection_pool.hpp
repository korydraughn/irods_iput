#ifndef IRODS_CONNECTION_POOL_HPP
#define IRODS_CONNECTION_POOL_HPP

#include "rcConnect.h"

#include <memory>
#include <vector>
#include <mutex>
#include <atomic>

#if defined(RODS_SERVER) || defined(RODS_CLERVER)
    #define rxComm_t    rsComm_t
#else
    #define rxComm_t    rcComm_t
#endif // defined(RODS_SERVER) || defined(RODS_CLERVER)

namespace irods
{
    class connection_pool
    {
    public:
        // A wrapper around a connection in the pool.
        // On destruction, the underlying connection is immediately returned
        // to the pool.
        class connection_proxy
        {
        public:
            friend class connection_pool;

            ~connection_proxy();

            operator rxComm_t&() const noexcept;

        private:
            connection_proxy(connection_pool& _pool, rxComm_t& _conn, int _index) noexcept;

            connection_pool& pool_;
            rxComm_t& conn_;
            int index_;
        };

        connection_pool(int _size,
                        const std::string& _host,
                        const int _port,
                        const std::string& _username,
                        const std::string& _zone);

        connection_proxy get_connection();

    private:
        using connection_pointer = std::unique_ptr<rxComm_t, int(*)(rxComm_t*)>;

        struct connection_context
        {
            std::mutex mutex{};
            std::atomic<bool> in_use{};
            connection_pointer conn{nullptr, rcDisconnect};
        };

        void return_connection(int _index) noexcept;

        std::vector<connection_context> conn_ctxs_;
    };
} // namespace irods

#endif // IRODS_CONNECTION_POOL_HPP

