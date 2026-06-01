# CALEON Android app — extracted protocol notes

Reverse-engineered from `docs/°CALEON_7.0.36.xapk` (app `com.sorel.caleon`,
version 7.0.36, by SOREL). Intended to supplement the manufacturer's partial
protocol docs for accessing the heat controller.

> Scope: this documents how the **official app** talks to the controller. It does
> not cover any CAN-bus interface — the app reaches the device over the internet
> via Nabto, not CAN.

## 1. App architecture

- The app is a **Capacitor (Ionic/Angular) web app**. All application logic is
  plain (minified) JavaScript under `assets/public/` inside the base APK — no
  decompilation required. Key bundle: `assets/public/main.*.js`.
- Device connectivity uses **classic uNabto / AppMyProduct (AMP)** through:
  - JS shim: `cordova-plugin-nabto` (`assets/public/plugins/cordova-plugin-nabto/`)
  - native lib: `config.arm64_v8a.apk → lib/arm64-v8a/libnabto_client_api_jni.so`
- So the controller is a **uNabto micro-server**: it exposes a fixed set of RPC
  "queries", each with a numeric id and a binary request/response layout defined
  by an interface XML the client injects at startup.

## 2. Transport & addressing

- Device host: `host(serial) = "<serial>.sorel-connect.net"`
  (`sorel-connect.net` is SOREL's Nabto basestation / portal).
- Every operation is an RPC call of the form:

  ```
  nabto://<serial>.sorel-connect.net/<query_name>?<urlencoded-params>
  ```

  Built in `invokeRpc(host, query, params)`:
  - params: `encodeURI(key) + "=" + encodeURIComponent(value)`, joined with `&`.
  - **raw string params are null-terminated** — the app appends `"\0"` to
    `id`, `action`, `season`, `vacation`, `mode`, `name`, etc. before sending.

- Session bring-up sequence (in `NabtoService`):
  1. `nabto.startup()` / `startupAndOpenProfile(user, pass)` — opens a profile.
     Plugin defaults are user `guest`, pass `123456`; real users authenticate
     against `sorel-connect.net` (signup / login / `createKeyPair` /
     `createSignedKeyPair` / `setBasestationAuthJson`).
  2. `nabto.rpcSetDefaultInterface(<xml>)` — injects the interface definition,
     loaded from `./assets/nabto/unabto_queries.xml` (copied here as
     `unabto_queries.xml`).
  3. `nabto.prepareInvoke([host])` — prepares the connection to the device.
  4. `nabto.rpcInvoke("nabto://…")` — per-call RPC.

- **Local/LAN access is possible**: the plugin exposes
  `setLocalConnectionPsk(host, pskId, psk)` (16-byte hex id + 16-byte hex key),
  i.e. direct encrypted local connections without the basestation. The app also
  references offline hosts. Pairing/PSK provisioning is account-bound.

## 3. The query interface

The full interface is in `unabto_queries.xml` (this directory). Two families:

### 3a. Room / state queries (ids 256–262)

| id  | query              | purpose |
|-----|--------------------|---------|
| 256 | `room_list.json`   | list rooms (name, type, temperature, humidity) |
| 257 | `room_state.json`  | read/update a room: `tset`, `vacation`, `season`, `mode`, `name` (empty req values are ignored = read-only) |
| 258 | `room_timer.json`  | read/update schedule timers (bitfields `bf0..bf2`) |
| 259 | `timer_copy.json`  | copy timers across days (`mask`) |
| 260 | `synch_flags.json` | feature/capability flags bitfield |
| 261 | `versions.json`    | firmware + nabto version strings |
| 262 | `message_log.json` | device message log (datetime + msg) |

App call signatures (from `main.*.js`):
- `ListRooms()` → `room_list.json`
- `RoomState(id, tdelta, season, vacation, mode, name)` → `room_state.json`
  (string args sent null-terminated; pass empty to read without writing)
- `TimerState(id, type, day, [bf0,bf1,bf2])` → `room_timer.json`
- `CopyTimer(id, type, day, mask)` → `timer_copy.json`
- `GetVersions()` → `versions.json`
- `GetMessageLog()` → `message_log.json`
- `synch_flags.json` is gated/used for capability detection.

### 3b. Generic on-device-menu protocol (ids 512–531)

This mirrors the controller's physical menu over RPC. You start at the root and
walk down by following `action` tokens. `action.json` (or `action_2.json`)
returns a node with a `type`; the app then calls the matching detail query.

Walk:
- `GetMenuAction(id, action)` → `action.json` / `action_2.json`
  (returns `id`, `title`, `back`, `forward`, `type`, plus hold/extended fields in v2)

`type` → follow-up query (dispatch from `GetMenuDetails`):

| node `type`                      | follow-up query        | id  |
|----------------------------------|------------------------|-----|
| `string list`                    | `string_list.json`     | 513 |
| `dynamic list` / `dynamic list tiled` | `dynamic_list.json` | 514 |
| `options`                        | `options.json`         | 515 |
| `timer`                          | `timer.json`           | 516 |
| `text`                           | `text.json`            | 517 |
| `information`                    | `information.json`     | 518 |
| `binary query`                   | `binary_query.json`    | 519 |
| `system date` / `month day`      | `system_date(_2).json` | 521/531 |
| (system time)                    | `system_time.json`     | 522 |
| `offset`                         | `offset.json`          | 523 |
| (system datetime)                | `system_datetime.json` | 524 |
| (time ranges)                    | `time_ranges(_2).json` | 525/530 |

Editing happens via the same nodes:
- `options`: re-invoke with `increase` / `decrease` action tokens.
- `offset`: `increase` / `decrease` per list entry.
- `text`: send `value`.
- `binary query`: send `yes` / `no` token.
- timers / time ranges / dates: send the new values; copy variants spread to
  other days via a `mask`.

### `extended_api` feature flag

`RoomService.HasFeature("extended_api")` selects the `_2` query variants
(`action_2`, `string_list_2`, `dynamic_list_2`, `time_ranges_2`,
`system_date_2`) which add hold-actions and extra fields. Capability is derived
from `synch_flags.json`.

## 4. Practical implications for accessing the controller

- The **entire control surface** is reachable as JSON-ish RPC over Nabto — not
  just the documented registers. Walking the menu tree (`action.json` from root)
  enumerates every setting the physical UI exposes.
- To talk to the device directly you need: a Nabto client (libnabto / the AMP
  client), the device serial (`<serial>.sorel-connect.net`), the
  `unabto_queries.xml` interface (provided here), and valid credentials — either
  a `sorel-connect.net` account or a local-connection PSK provisioned to the
  device.
- Field types in `unabto_queries.xml` (`uint8/16/32`, `int8/16`, `raw`) give the
  exact request/response wire layout for each query.

## 5. Source artifacts in the XAPK

- `assets/public/assets/nabto/unabto_queries.xml` — interface definition (the spec).
- `assets/public/main.*.js` — `NabtoService` (RPC), `RoomService`, `DeviceService`,
  menu rendering. Search for `invokeRpc`, `InvokeRPC`, `GetMenu*`, `room_*`.
- `assets/public/plugins/cordova-plugin-nabto/www/` — JS API surface + constants
  (`nabto-constants.js` has error/exception codes).
- `assets/capacitor.config.json` — server host `sorel-connect.net`.
- `config.arm64_v8a.apk → libnabto_client_api_jni.so` — native Nabto client.

## 6. Nabto Client SDK Resources

The Nabto SDK Resources are available at [developer.nabto.com](https://developer.nabto.com/).
