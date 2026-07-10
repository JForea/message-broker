#pragma once

#include "shared/io/Pipeline.hpp"

#include <condition_variable>
#include <cstddef>
#include <memory>
#include <mutex>
#include <vector>

namespace message_broker {

    class PipePool {
    public:
        class PipeHandle {
        private:
            PipePool* _pool = nullptr;
            std::unique_ptr<Pipeline> _pipeline;

            void Reset() noexcept;

        public:
            PipeHandle(PipePool& pool, std::unique_ptr<Pipeline> pipeline) noexcept;

            PipeHandle(const PipeHandle&) = delete;
            PipeHandle& operator=(const PipeHandle&) = delete;

            PipeHandle(PipeHandle&& other) noexcept;
            PipeHandle& operator=(PipeHandle&& other) noexcept;

            Pipeline& Get() noexcept;

            ~PipeHandle() noexcept;
        };

    private:
        std::mutex _mutex;
        std::condition_variable _condition;
        size_t _capacity;

        std::vector<std::unique_ptr<Pipeline>> _availablePipelines;
        bool _stopping = false;

        void Release(std::unique_ptr<Pipeline> pipeline) noexcept;

    public:
        explicit PipePool(size_t size);

        PipePool(const PipePool&) = delete;
        PipePool& operator=(const PipePool&) = delete;

        PipeHandle Acquire();
        std::vector<PipeHandle> AcquireMany(size_t count);

        void Stop() noexcept;
    };

}

