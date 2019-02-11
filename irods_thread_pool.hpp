#ifndef IRODS_THREAD_POOL_HPP
#define IRODS_THREAD_POOL_HPP

#include <boost/version.hpp>
#include <boost/asio.hpp>

#define SUPPORTS_BOOST_THREAD_POOL \
    (BOOST_VERSION / 100000) == 1 && ((BOOST_VERSION / 100) % 1000) >= 67

#if !SUPPORTS_BOOST_THREAD_POOL
    #include <boost/thread.hpp>
#endif

#include <memory>

namespace irods {

#if SUPPORTS_BOOST_THREAD_POOL
    class thread_pool {
        public:
            explicit thread_pool(int _size)
                : pool_{static_cast<std::size_t>(_size)}
            {
            }

            template <typename Function>
            void dispatch(Function&& _func) {
                boost::asio::dispatch(pool_, std::forward<Function>(_func));
            }

            template <typename Function>
            void post(Function&& _func) {
                boost::asio::post(pool_, std::forward<Function>(_func));
            }

            template <typename Function>
            void defer(Function&& _func) {
                boost::asio::defer(pool_, std::forward<Function>(_func));
            }

            void join() {
                pool_.join();
            }

            void stop() {
                pool_.stop();
            }

        private:
            boost::asio::thread_pool pool_;
    };

    template <typename Function>
    inline void dispatch(thread_pool& _pool, Function&& _func) {
        _pool.dispatch(std::forward<Function>(_func));
    }

    template <typename Function>
    inline void post(thread_pool& _pool, Function&& _func) {
        _pool.post(std::forward<Function>(_func));
    }

    template <typename Function>
    inline void defer(thread_pool& _pool, Function&& _func) {
        _pool.defer(std::forward<Function>(_func));
    }
#else
    class thread_pool {
        public:
            explicit thread_pool(int _size)
                : io_service_{std::make_shared<boost::asio::io_service>()}
                , work_{std::make_shared<boost::asio::io_service::work>(*io_service_)}
            {
                for (decltype(_size) i{}; i < _size; i++) {
                    thread_group_.create_thread([this] {
                        io_service_->run();
                    });
                }
            }

            ~thread_pool() {
                stop();
                join();
            }

            template <typename Function>
            void dispatch(Function&& _func) {
                io_service_->dispatch(std::forward<Function>(_func));
            }

            template <typename Function>
            void post(Function&& _func) {
                io_service_->post(std::forward<Function>(_func));
            }

            void join() {
                thread_group_.join_all();
            }

            void stop() {
                if (io_service_) {
                    io_service_->stop();
                }
            }

        private:
            boost::thread_group thread_group_;
            std::shared_ptr<boost::asio::io_service> io_service_;
            std::shared_ptr<boost::asio::io_service::work> work_;
    };

    template <typename Function>
    inline void dispatch(thread_pool& _pool, Function&& _func) {
        _pool.dispatch(std::forward<Function>(_func));
    }

    template <typename Function>
    inline void post(thread_pool& _pool, Function&& _func) {
        _pool.post(std::forward<Function>(_func));
    }
#endif
} // namespace irods

#endif // IRODS_THREAD_POOL_HPP

