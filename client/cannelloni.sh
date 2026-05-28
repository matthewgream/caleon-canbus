#!/bin/sh
set -eu
HOST="caleon-canbus.local"
IFACE="vcan0"
PORT=20000

# Resolve the mDNS name to an IPv4 address, retrying while
# the network and avahi settle after boot.
IP=""
for i in $(seq 1 30); do
    IP=$(getent ahostsv4 "$HOST" | awk '{print $1; exit}')
    [ -n "$IP" ] && break
    sleep 1
done

if [ -z "$IP" ]; then
    echo "cannelloni-client: could not resolve $HOST" >&2
    exit 1
fi

echo "cannelloni-client: $HOST -> $IP"
exec /usr/local/bin/cannelloni -I "$IFACE" -R "$IP" -r "$PORT" -l "$PORT" -p
