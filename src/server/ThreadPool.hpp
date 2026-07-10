#pragma once

#include <condition_variable>
#include <functional>
#include <mutex>
#include <queue>
#include <thread>
#include <vector>

#include "shared/io/Pipeline.hpp"

namespace message_broker {

    class ThreadPool {
    private:
        std::vector<std::thread> _workers;
        std::queue<std::function<void(const Pipeline&)>> _tasks;

        std::mutex _mutex;
        std::condition_variable _condition;

        bool _stopping = false;
        
        void AddWorker();

        void WorkerLoop();

    public:
        explicit ThreadPool();

        ThreadPool(const ThreadPool&) = delete;
        ThreadPool& operator=(const ThreadPool&) = delete;

        void Enqueue(std::function<void(const Pipeline&)> task);
        void Stop() noexcept;

        ~ThreadPool() noexcept;
    };

}
