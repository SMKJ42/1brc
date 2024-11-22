# About

To test out the project yourself, simply run `cargo run --release light` in your terminal.

To get a flamegraph, run `sudo cargo --profile release flamegraph -- full` in your terminal

If you want to check out how to produce the full 1 billion rows, follow this link

https://github.com/gunnarmorling/1brc

### WARNING: this will take a while and use ~10Gb storage

To save you some reading, just clone that repo, and run

1.  ```
    ./mvnw clean verify
    ```

2.  ```
    ./create_measurements.sh 1000000000
    ```
