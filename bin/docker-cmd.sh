#!/bin/sh

LIB_PATH=/usr/local/lib/kafka-connect-tools

if [ -n "$JAVA_HOME" ]; then
    JAVA_BIN="$JAVA_HOME/bin/java"
else
    JAVA_BIN=$(command -v java)
fi

for jar in "$LIB_PATH"/kafka-connect-cli*.jar; do
    break
done

exec "$JAVA_BIN" -jar "$jar" "$@"
