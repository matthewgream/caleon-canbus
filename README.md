# caleon-canbus

Reverse-engineered MQTT bridge and live TUI for a Sorel/Caleon hydronic heating
install (CALEONbox + CALEON S room panel + 1-wire sensors) reachable over CAN.

## Architecture

The CAN bus is bridged from the install site to a client over IP via 
[cannelloni](https://github.com/mguentner/cannelloni):

```
  [Heating bus]
       |
       | CAN @ 250 kbit/s
       v
  CANable2 USB adapter  (Bus 001 Device 004: 16d0:117e MCS CANable2,
       |                 firmware b158aa7 from github.com/normaldotcom/canable2.git)
       |
  ===== server (on-site box) =====
   slcand   /dev/ttyACM0  ->  can0
   cannelloni   can0  <->  UDP :20000
       |
       | UDP/IP over the LAN
       v
  ===== client =====
   vcan0 (virtual CAN)
   cannelloni   vcan0  <->  server:20000
       |
       v
   caleon2mqtt   --->   mosquitto   --->   caleon-tui / Home Assistant / etc.
```

Systemd units to run each side are in `server/` and `client/`:

- `server/slcand.service` brings up `can0` from the CANable2 at 250 kbit/s.
- `server/cannelloni.service` exposes `can0` as UDP/20000.
- `client/vcan0.service` creates the virtual `vcan0`.
- `client/cannelloni.service` (via `cannelloni.sh`) resolves the server over
  mDNS and tunnels `vcan0` to it.

## caleon2mqtt

`caleon2mqtt.c` is a single-file C decoder (libmosquitto + libcjson) that
listens on `vcan0`, parses the SOREL SCBI program/function/message layout,
applies per-function scaling (decicelsius, decipercent, mode enum, ASCII
relay/PWM labels, sensor-absent sentinels) and publishes structured JSON on
`caleon/*` topics. The raw decoder debug stream is still printed on stdout so
further protocol reverse-engineering can continue.

- Build: `make` (needs `libmosquitto-dev` and `libcjson-dev`).
- Run:   `./caleon2mqtt caleon2mqtt.cfg`.
- Config: `caleon2mqtt.cfg` (JSON) holds the broker settings plus the
  room/subscriber/relay/aux-function name tables — all installation-specific
  mapping lives there, not in code.

## caleon-tui

`caleon-tui.js` is a Node.js [blessed](https://github.com/chjj/blessed) console
TUI that subscribes to `caleon/#` and shows live rooms, central plant,
relays, HC sensors and an event log. It reads the same `caleon2mqtt.cfg` for
human-readable names.

![caleon-tui screenshot](assets/caleon-tui.jpg)

- Install: `npm install`.
- Run:     `node caleon-tui.js` (q or Ctrl-C to quit).

## docs

Vendor documentation kept under `docs/` for reference:

- `SOREL_CAN_bus_interface_rev11_COSTUMERS.pdf` — **primary protocol reference**
  (program/function/message layout, payload formats, scaling).
- `CBox_Documentation.pdf` — CALEONbox Anybus Modbus-TCP gateway manual; useful
  for the `0x95 ROOMDATA` definition.
- `Datenpunkte_Modbus_CBox_Clima.pdf` — Modbus register map / data-point list
  for the gateway (confirms decicelsius and decipercent scaling).
- `Cbox_Systemanleitung_en.pdf` — CALEONbox system / installer manual.
- `CALEONbox_en.pdf` — CALEONbox product datasheet.
- `70003d_CALEON_Smart_en.pdf` — CALEON Smart room-panel manual.
