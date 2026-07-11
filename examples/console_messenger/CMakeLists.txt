add_executable(
    console-messenger
    main.cpp
    Helpers.cpp
)

target_link_libraries(
    console-messenger
    PRIVATE
        message-broker-client
)