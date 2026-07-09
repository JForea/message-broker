#include "PipePool.hpp"

namespace message_broker {

    PipePool::PipeHandle::PipeHandle(
        PipePool& pool,
        std::unique_ptr<Pipeline> pipeline
    ) noexcept
        : _pool(&pool), _pipeline(std::move(pipeline)) {}

    void PipePool::PipeHandle::Reset() noexcept {
        if (_pool && _pipeline)
            _pool->Release(std::move(_pipeline));

        _pool = nullptr;
    }

    PipePool::PipeHandle::PipeHandle(PipeHandle&& other) noexcept
        : _pool(other._pool), _pipeline(std::move(other._pipeline)) {
        other._pool = nullptr;
    }

    PipePool::PipeHandle& PipePool::PipeHandle::operator=(PipeHandle&& other) noexcept {
        if (this != &other) {
            Reset();

            _pool = other._pool;
            _pipeline = std::move(other._pipeline);

            other._pool = nullptr;
        }

        return *this;
    }

    Pipeline& PipePool::PipeHandle::Get() noexcept {
        return *_pipeline;
    }

    PipePool::PipeHandle::~PipeHandle() noexcept {
        Reset();
    }
    
    PipePool::PipePool(size_t size) {
        if (size == 0)
            throw std::invalid_argument("Pipe pool size must be greater than zero.");

        _availablePipelines.reserve(size);

        for (int i = 0; i < size; ++i)
            _availablePipelines.push_back(std::make_unique<Pipeline>());
    }

    PipePool::PipeHandle PipePool::Acquire() {
        std::unique_lock lock(_mutex);

        _condition.wait(lock, [this] {
            return _stopping || !_availablePipelines.empty();
        });

        if (_stopping)
            throw std::runtime_error("Pipe pool is stopped");

        auto pipeline = std::move(_availablePipelines.back());
        _availablePipelines.pop_back();

        return PipeHandle(*this, std::move(pipeline));
    }

    void PipePool::Release(std::unique_ptr<Pipeline> pipeline) noexcept {
        if (!pipeline)
            return;

        {
            std::lock_guard lock(_mutex);

            if (_stopping)
                return;

            _availablePipelines.push_back(std::move(pipeline));
        }

        _condition.notify_one();
    }

    void PipePool::Stop() noexcept {
        {
            std::lock_guard lock(_mutex);

            if (_stopping)
                return;

            _stopping = true;
        }

        _condition.notify_all();
    }

}
