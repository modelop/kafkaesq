#!/bin/sh

SELF=`which "$0" 2>/dev/null`
[ $? -gt 0 -a -f "$0" ] && SELF="./$0"

JAVA=java
if test -n "$JAVA_HOME"; then
  JAVA="$JAVA_HOME/bin/java"
fi
exec "$JAVA" $JAVA_ARGS -jar $SELF "$@"
exit 1

