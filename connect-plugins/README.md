# Kafka Connect plugins

The Compose file mounts this directory at `/etc/kafka-connect/jars`.

The base image does not bundle the Snowflake sink connector because it
requires a separately managed connector version and Snowflake credentials.
Install a compatible connector distribution here before creating a
Snowflake sink connector. Keep downloaded binaries out of source control.
