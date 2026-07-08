#pragma once

#include <condition_variable>
#include <functional>
#include <mutex>
#include <queue>
#include <thread>
#include <vector>

namespace message_broker {

    class ThreadPool {
    private:
        std::vector<std::thread> _workers;
        std::queue<std::function<void()>> _tasks;

        std::mutex _mutex;
        std::condition_variable _condition;

        bool _stopping = false;
        
        void WorkerLoop();

    public:
        explicit ThreadPool(size_t threadCount);

        ThreadPool(const ThreadPool&) = delete;
        ThreadPool& operator=(const ThreadPool&) = delete;

        void Enqueue(std::function<void()> task);
        void Stop() noexcept;

        ~ThreadPool() noexcept;
    };

}
