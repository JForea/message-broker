#include "ThreadPool.hpp"

#include <iostream>

namespace message_broker {

    ThreadPool::ThreadPool(size_t threadCount) {
        if (threadCount == 0)
            throw std::invalid_argument("Thread pool size must be greater than zero.");

        _workers.reserve(threadCount);

        for (size_t i = 0; i < threadCount; ++i) {
            _workers.emplace_back([this] {
                WorkerLoop();
            });
        }
    }

    void ThreadPool::WorkerLoop() {
        while (true) {
            std::function<void()> task;

            {
                std::unique_lock lock(_mutex);

                _condition.wait(lock, [this] {
                    return _stopping || !_tasks.empty();
                });

                if (_stopping && _tasks.empty())
                    return;

                task = std::move(_tasks.front());
                _tasks.pop();
            }

            // Prevent a task exception from terminating the worker thread.
            // TODO: Add normal logger.
            try {
                task();
            } catch (const std::exception& e) {
                std::cerr << "ThreadPool task failed: " << e.what() << '\n';
            } catch(...) {
                std::cerr << "ThreadPool task failed: unknown exception\n";
            }
        }
    }

    void ThreadPool::Enqueue(std::function<void()> task) {
        {
            std::lock_guard lock(_mutex);

            if (_stopping)
                throw std::runtime_error("Cannot enqueue task into stopped thread pool.");

            _tasks.push(std::move(task));
        }

        _condition.notify_one();
    }

    void ThreadPool::Stop() noexcept {
        {
            std::lock_guard lock(_mutex);

            if (_stopping)
                return;

            _stopping = true;
        }

        _condition.notify_all();

        for (auto& worker : _workers) {
            if (worker.joinable())
                worker.join();
        }
    }

    ThreadPool::~ThreadPool() noexcept {
        Stop();
    }
    
}
