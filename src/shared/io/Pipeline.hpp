#pragma once

#include <unistd.h>

#include <stdexcept>

namespace message_broker {
    
    class Pipeline {
    private:
        int _readEnd;
        int _writeEnd;
    
    public:
        Pipeline() {
            int pipeFds[2];
            if (pipe(pipeFds) == -1)
                throw std::runtime_error("Pipe creation failed.");

            _readEnd = pipeFds[0];
            _writeEnd = pipeFds[1];
        }

        Pipeline(const Pipeline&) = delete;
        Pipeline& operator=(const Pipeline&) = delete;

        int ReadEnd() const {
            return _readEnd;
        }

        int WriteEnd() const {
            return _writeEnd;
        }

        ~Pipeline() noexcept {
            close(_readEnd);
            close(_writeEnd);
        }
    };

}
