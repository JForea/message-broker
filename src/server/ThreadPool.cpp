#include "ThreadPool.hpp"

#include <iostream>

namespace {

    constexpr uint16_t MaxTasksPerWorker = 10;

}

namespace message_broker {

    ThreadPool::ThreadPool() {
        AddWorker();
    }

    void ThreadPool::AddWorker() {
        _workers.emplace_back(std::thread([this] {
            WorkerLoop();
        }));
    }

    void ThreadPool::WorkerLoop() {
        Pipeline pipeline;

        while (true) {
            std::function<void(const Pipeline&)> task;

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
                task(pipeline);
            } catch (const std::exception& e) {
                std::cerr << "ThreadPool task failed: " << e.what() << '\n';
            } catch(...) {
                std::cerr << "ThreadPool task failed: unknown exception\n";
            }
        }
    }

    void ThreadPool::Enqueue(std::function<void(const Pipeline&)> task) {
        {
            std::lock_guard lock(_mutex);

            if (_stopping)
                throw std::runtime_error("Cannot enqueue task into stopped thread pool.");

            _tasks.push(std::move(task));

            if (_tasks.size() >= _workers.size() * MaxTasksPerWorker)
                AddWorker();
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
