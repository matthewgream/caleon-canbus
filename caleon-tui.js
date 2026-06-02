#!/usr/bin/env node
/*
 * caleon-tui -- blessed TUI that subscribes to caleon/# and shows
 *               live room / relay / HC-sensor / device state.
 *
 * Run: node caleon-tui.js [--config caleon2mqtt.cfg] [--host localhost]
 */

const blessed = require('blessed');
const mqtt = require('mqtt');
const fs = require('fs');
const path = require('path');

/* ---------------------------------------------------------------- args */

const argv = process.argv.slice(2);
const argFor = (k, d) => {
    const i = argv.indexOf(k);
    return i >= 0 && argv[i + 1] ? argv[i + 1] : d;
};
const CFG_PATH = argFor('--config', path.join(__dirname, 'caleon2mqtt.cfg'));
const HOST = argFor('--host', 'localhost');
const PORT = parseInt(argFor('--port', '1883'), 10);
const PREFIX = argFor('--prefix', 'caleon');

/* ---------------------------------------------------------------- config */

let cfg = { rooms: {}, subscribers: {}, hc_sensors: {}, hc_relays: {} };
try {
    cfg = { ...cfg, ...JSON.parse(fs.readFileSync(CFG_PATH, 'utf8')) };
} catch (e) {
    console.error(`config: cannot load ${CFG_PATH}: ${e.message} -- using empty defaults`);
}

/* Build an ordered room list from config. Key is "<idx>:<type>". */
const roomEntries = Object.entries(cfg.rooms || {})
    .map(([key, value]) => {
        const [ridx, rtyp] = key.split(':').map((n) => parseInt(n, 10));
        return { key, ridx, rtyp, name: typeof value === 'string' ? value : value?.name || key };
    })
    .sort((a, b) => a.ridx - b.ridx);

const roomByKey = new Map(roomEntries.map((r) => [r.key, r]));

/* ---------------------------------------------------------------- state */

const now = () => Date.now() / 1000;
const ageStr = (t) => {
    if (!t) return '   -';
    const dt = Math.max(0, Math.floor(now() - t));
    if (dt < 60) return `${dt}s`.padStart(4);
    if (dt < 3600) return `${Math.floor(dt / 60)}m`.padStart(4);
    return `${Math.floor(dt / 3600)}h`.padStart(4);
};

const state = {
    rooms: new Map() /* key "ri:rt" -> {temp, tset, humidity, swval, t_*} */,
    relays: new Map() /* no -> {mode, value_raw, ts} */,
    sensors: new Map() /* no -> {value, type, ts} */,
    devices: new Map() /* subscriber_id -> {name, oem, variant, ts} */,
    central: new Map() /* function name -> {value, unit, present, ts} (RI=0,RT=0) */,
    datetimes: [] /* recent system_datetime broadcasts, newest first */,
    /* Per-zone parameter writes from CALEON_S to CALEONbox (0x8C, ROOMSYNC
     * shape). Keyed by `${instance}` -> { relayName, zone, params: Map<pid, {paramName, value, value_raw, unit, ts}> }. */
    zones: new Map(),
    /* All local addressable temp/humidity probes -- 0x8B fn=0x00 readings
     * and fn=0x05 offsets unified. Keyed by "<subscriber_id>.<slot>"
     * (e.g. "151.0" = CALEON S panel sensor, "181.0".."181.7" = CALEONbox
     * 1-wire bus). Value: { host, slot, temp: {value, offset, ts},
     * humidity: {value, offset, ts} }. */
    probes: new Map(),
    unknown: 0,
    msgs: 0,
    raws: [] /* tail of {topic, type, line} */,
};

roomEntries.forEach((r) => state.rooms.set(r.key, {}));

/* ---------------------------------------------------------------- ui */

const screen = blessed.screen({ smartCSR: true, title: 'caleon-tui', fullUnicode: true });

const mkBox = (opts) =>
    blessed.box(
        Object.assign(
            {
                border: 'line',
                tags: true,
                style: { border: { fg: 'cyan' }, label: { fg: 'white', bold: true } },
            },
            opts
        )
    );

/* ---- layout: tweak these constants to retune the screen ---- */

const N_ROOMS = roomEntries.length;
const N_RELAYS = cfg.hc_relays ? Object.keys(cfg.hc_relays).length : 14;
const N_SENSORS = cfg.hc_sensors ? Object.keys(cfg.hc_sensors).length : 14;

const LAYOUT = {
    /* Three equal-ish columns across the top section. */
    COL1_LEFT: 0,
    COL2_LEFT: '34%',
    COL3_LEFT: '67%',
    COL1_WIDTH: '34%',
    COL2_WIDTH: '33%',
    COL3_WIDTH: '33%',

    /* Bottom row: status takes a half column on the left, log fills the rest. */
    STATUS_WIDTH: '17%',
    LOG_LEFT: '17%',
    LOG_WIDTH: '83%',
    BOTTOM_HEIGHT: 10 /* rows (status + log + borders) */,

    /* Minimum box heights in rows. rooms/relays/hc_sensors are sized to fit
     * config counts + a spare line + 2 borders, so they never overflow.
     * zones/devices/onewire don't have a config-derived size and use a
     * sensible fixed baseline. Any leftover vertical space in a column is
     * split equally between its rows (see distributeColumn() below). */
    ROOMS_H: N_ROOMS + 4 /* header + spare + 2 borders         */,
    RELAYS_H: N_RELAYS + 3,
    SENSORS_H: N_SENSORS + 7,
    ZONES_H: 12 /* up to ~8 zones + header/spare      */,
    DEVICES_H: 8 /* 2 devices + panel + datetime sub   */,
    ONEWIRE_H: 8 /* up to ~8 slots, temp-only typical  */,
};
LAYOUT.TOP_HEIGHT = `100%-${LAYOUT.BOTTOM_HEIGHT}`; /* blessed offset expr */

/* Compute final box heights for one column. Each column has N rows stacked;
 * each row has a minimum height. Any leftover vertical space is divided
 * equally between rows (the last row absorbs the remainder). Computed once
 * at startup from the terminal size -- relaunch the TUI after resizing. */
const SCREEN_H = process.stdout.rows && process.stdout.rows > 10 ? process.stdout.rows : 40;
const TOP_H = Math.max(20, SCREEN_H - LAYOUT.BOTTOM_HEIGHT);
function distributeColumn(...rowMins) {
    const baseSum = rowMins.reduce((a, b) => a + b, 0);
    const leftover = Math.max(0, TOP_H - baseSum);
    const perRow = Math.floor(leftover / rowMins.length);
    const rem = leftover - perRow * rowMins.length;
    return rowMins.map((m, i) => m + perRow + (i === rowMins.length - 1 ? rem : 0));
}
const [HR_ROOMS, HR_ZONES] = distributeColumn(LAYOUT.ROOMS_H, LAYOUT.ZONES_H);
const [HR_HC, HR_RELAYS] = distributeColumn(LAYOUT.SENSORS_H, LAYOUT.RELAYS_H);
const [HR_DEVICES, HR_ONEWIRE] = distributeColumn(LAYOUT.DEVICES_H, LAYOUT.ONEWIRE_H);

/* Shared formatting helper used by several renders. */
const fmtVal = (v, suffix = '', width = 7) => {
    if (v === null || v === undefined || Number.isNaN(v)) return '   -- '.padStart(width);
    return (typeof v === 'number' ? v.toFixed(1) : String(v)).padStart(width) + suffix;
};

/* ============================================================== rooms == */

const roomsBox = mkBox({
    parent: screen,
    label: ' Rooms (ecsRcTroom / Tset / Switch / Humidity) ',
    top: 0,
    left: LAYOUT.COL1_LEFT,
    width: LAYOUT.COL1_WIDTH,
    height: HR_ROOMS,
});

function renderRooms() {
    const lines = ['{bold}{white-fg} idx rt   name              temp     set   mode      hum   age{/}{/bold}'];
    for (const r of roomEntries) {
        const s = state.rooms.get(r.key) || {};
        lines.push(
            ` ${String(r.ridx).padStart(2)}  ${String(r.rtyp).padStart(2)} ` +
                ` {yellow-fg}${(r.name || '').padEnd(14)}{/yellow-fg}` +
                ` ${fmtVal(s.temp, '°C')}` +
                ` ${fmtVal(s.tset, '°C')}` +
                ` ${s.swval == null ? '   --   ' : (s.swmode || '?' + s.swval).padEnd(8)}` +
                ` ${fmtVal(s.humidity, '%', 5)}` +
                `  ${ageStr(s.t_temp || s.t_tset || s.t_sw || s.t_hum)}`
        );
    }
    /* unmapped rooms that appeared on the bus */
    for (const [k, s] of state.rooms.entries())
        if (!roomByKey.has(k)) {
            const [ri, rt] = k.split(':');
            lines.push(
                ` ${ri.padStart(2)}  ${rt.padStart(2)}  {red-fg}(unmapped)    {/red-fg}` +
                    ` ${fmtVal(s.temp, '°C')} ${fmtVal(s.tset, '°C')}` +
                    ` ${(s.swval ?? '--').toString().padStart(4)}` +
                    ` ${fmtVal(s.humidity, '%', 5)}  ${ageStr(s.t_temp || s.t_tset)}`
            );
        }
    roomsBox.setContent(lines.join('\n'));
}

function processSensor(m, tstamp) {
    const present = m.present !== false && m.value !== null;
    if (m.room_index === 0 && m.room_type === 0) state.central.set(m.function, { value: present ? m.value : null, unit: m.unit || '', present, ts: tstamp });
    else {
        const key = `${m.room_index}:${m.room_type}`;
        const s = state.rooms.get(key) || {};
        if (m.function === 'ecsRcTroom') {
            s.temp = present ? m.value : null;
            s.t_temp = tstamp;
        } else if (m.function === 'ecsRcTset') {
            s.tset = present ? m.value : null;
            s.t_tset = tstamp;
        } else if (m.function === 'ecsRcHumidity') {
            s.humidity = present ? m.value : null;
            s.t_hum = tstamp;
        } else if (m.function === 'ecsRcSwitch') {
            s.swval = present ? m.value_raw : null;
            s.swmode = present && m.mode_name ? m.mode_name : null;
            s.t_sw = tstamp;
        }
        state.rooms.set(key, s);
    }
}

/* ============================================================== zones == */

const zonesBox = mkBox({
    parent: screen,
    label: ' Zones (0x8C param writes from CALEON_S panel) ',
    top: HR_ROOMS,
    left: LAYOUT.COL1_LEFT,
    width: LAYOUT.COL1_WIDTH,
    height: HR_ZONES,
});

const ZONE_KNOWN_PIDS = [0x01, 0x08, 0x11, 0x12, 0x1c];

function renderZones() {
    /* per-relay parameter writes captured from CALEON_S → CALEONbox. One
     * row per zone with the known paramIDs as columns; extra/unknown
     * params surface as a "+N" suffix on the age. */
    const fmtParam = (p, width) => {
        if (!p) return '--'.padStart(width);
        const v = p.value;
        if (v == null) return '--'.padStart(width);
        if (typeof v === 'number') return (v.toFixed(1) + (p.unit || '')).padStart(width);
        return String(v).padStart(width);
    };
    const lines = ['{bold}{white-fg} zone  name           pol     seize    W/S      eco     floor       age{/}{/bold}'];
    for (const [, z] of [...state.zones.entries()].sort((a, b) => Number(a[0]) - Number(b[0]))) {
        const ts = [...z.params.values()].map((p) => p.ts).filter(Boolean);
        const latest = ts.length ? Math.max(...ts) : 0;
        const extras = [...z.params.keys()].filter((pid) => !ZONE_KNOWN_PIDS.includes(pid));
        const ageCell = ageStr(latest) + (extras.length ? ` {yellow-fg}+${extras.length}{/}` : '    ');
        lines.push(
            `  ${z.zone.padEnd(2)}  ` +
                `{yellow-fg}${(z.relayName || '').padEnd(12)}{/yellow-fg}  ` +
                `${fmtParam(z.params.get(0x01), 6)} ` +
                `${fmtParam(z.params.get(0x08), 7)} ` +
                `${fmtParam(z.params.get(0x11), 7)} ` +
                `${fmtParam(z.params.get(0x12), 7)} ` +
                `${fmtParam(z.params.get(0x1c), 8)}  ` +
                `${ageCell}`
        );
    }
    zonesBox.setContent(lines.join('\n'));
}

function processParam(m, tstamp) {
    const inst = m.instance;
    const z = state.zones.get(inst) || { zone: m.zone || '?', relayName: m.relay_name || '', params: new Map() };
    if (m.relay_name) z.relayName = m.relay_name;
    if (m.zone) z.zone = m.zone;
    z.params.set(m.param_id, {
        paramName: m.param_name || `pid${m.param_id.toString(16)}`,
        value: m.value !== undefined ? m.value : m.value_raw,
        value_raw: m.value_raw,
        unit: m.unit || '',
        ts: tstamp,
    });
    state.zones.set(inst, z);
}

/* ============================================================ sensors == */

const sensorsBox = mkBox({
    parent: screen,
    label: ' Sensors (0x8B DLF_SENSOR) ',
    top: 0,
    left: LAYOUT.COL2_LEFT,
    width: LAYOUT.COL2_WIDTH,
    height: HR_HC,
});

function renderSensors() {
    const lines = ['{bold}{white-fg}  no  name              kind         raw      value     age{/}{/bold}'];
    for (const [no, s] of [...state.sensors.entries()].sort((a, b) => a[0] - b[0])) {
        const nm =
            (cfg.hc_sensors &&
                (cfg.hc_sensors[`0x${no.toString(16).padStart(2, '0').toUpperCase()}`] ||
                    cfg.hc_sensors[`0x${no.toString(16).padStart(2, '0')}`] ||
                    cfg.hc_sensors[String(no)])) ||
            '';
        const absent = s.present === false || s.value === null || s.value === undefined;
        const kind = absent ? '{#888-fg}(absent)    {/}' : (s.type_name || '?').padEnd(12);
        const valCell = absent ? '{#888-fg}    --   {/}' : `${String(s.value).padStart(6)} ${(s.unit || '').padEnd(4)}`;
        lines.push(`  ${String(no).padStart(2)}  ${nm.padEnd(16)}  ${kind}` + `  ${String(s.value_raw ?? '--').padStart(6)}` + `  ${valCell}  ${ageStr(s.ts)}`);
    }
    sensorsBox.setContent(lines.join('\n'));
}

function processHcSensor(m, tstamp) {
    const present = m.present !== false && m.value !== null;
    state.sensors.set(m.sensor_no, {
        type: m.sensor_type,
        type_name: m.sensor_type_name || '',
        unit: m.unit || '',
        value: present ? m.value : null,
        value_raw: m.value_raw,
        present,
        ts: tstamp,
    });
}

/* ============================================================= relays == */

const relaysBox = mkBox({
    parent: screen,
    label: ' Relays (0x8B DLF_RELAY) ',
    top: HR_HC,
    left: LAYOUT.COL2_LEFT,
    width: LAYOUT.COL2_WIDTH,
    height: HR_RELAYS,
});

function renderRelays() {
    /* bytes 4-7 are state-related but we don't yet know which is on/off, so
     * display all four so the user can spot which one moves first. */
    const lines = ['{bold}{white-fg}  no  name         label flag  extra(4..7)  age{/}{/bold}'];
    for (const [no, r] of [...state.relays.entries()].sort((a, b) => a[0] - b[0])) {
        const nm =
            (cfg.hc_relays &&
                (cfg.hc_relays[`0x${no.toString(16).padStart(2, '0').toUpperCase()}`] ||
                    cfg.hc_relays[`0x${no.toString(16).padStart(2, '0')}`] ||
                    cfg.hc_relays[String(no)])) ||
            '';
        const extra = (r.extra || [])
            .map((b) => b.toString(16).padStart(2, '0').toUpperCase())
            .join(' ')
            .padEnd(11);
        const extraStyled = (r.extra || []).every((b) => b === 0) ? `{#888-fg}${extra}{/}` : `{yellow-fg}${extra}{/}`;
        lines.push(
            `  ${String(no).padStart(2)}  ${nm.padEnd(11)}  ` +
                `"${(r.label || '').padEnd(2)}"  ` +
                `0x${(r.flag ?? 0).toString(16).padStart(2, '0').toUpperCase()}  ` +
                `${extraStyled}  ${ageStr(r.ts)}`
        );
    }
    relaysBox.setContent(lines.join('\n'));
}

function processHcRelay(m, tstamp) {
    state.relays.set(m.relay_no, { flag: m.flag, label: m.label || '', extra: m.extra || [], ts: tstamp });
}

/* ============================================================ devices == */

const devicesBox = mkBox({
    parent: screen,
    label: ' Devices ',
    top: 0,
    left: LAYOUT.COL3_LEFT,
    width: LAYOUT.COL3_WIDTH,
    height: HR_DEVICES,
    scrollable: true,
    alwaysScroll: true,
    mouse: true,
});

function renderDevices() {
    const lines = ['{bold}{white-fg} addr  name              oem  var    fw   age{/}{/bold}'];
    for (const [id, d] of [...state.devices.entries()].sort())
        lines.push(
            ` 0x${id.toString(16).padStart(2, '0').toUpperCase()}  ` +
                `{yellow-fg}${(d.name || '?').padEnd(14)}{/yellow-fg}` +
                `  0x${(d.oem ?? 0).toString(16).padStart(2, '0').toUpperCase()}` +
                `  0x${(d.variant ?? 0).toString(16).padStart(2, '0').toUpperCase()}` +
                `  ${d.firmware != null ? String(d.firmware).padStart(5) : '   --'}` +
                `  ${ageStr(d.ts)}`
        );
    lines.push('', '{bold}{white-fg} System date/time (program 0x87){/}{/bold}');
    for (const d of state?.datetimes) {
        const skStr = d.skew_secs == null ? '    --' : (d.skew_secs > 0 ? '+' : '') + d.skew_secs + 's';
        lines.push(`  ${d.iso8601}  dow=${d.dow ?? '?'}  skew=${skStr.padStart(6)}  age ${ageStr(d.ts)}`);
    }
    devicesBox.setContent(lines.join('\n'));
}

function deviceNameFor(id) {
    return cfg?.subscribers?.[`0x${id.toString(16).padStart(2, '0').toUpperCase()}`] || cfg?.subscribers?.[`0x${id.toString(16).padStart(2, '0')}`] || '';
}

function processHeartbeat(m, tstamp) {
    const prev = state.devices.get(m.device_subscriber_id) || {};
    state.devices.set(m.device_subscriber_id, {
        ...prev,
        name: prev.name || deviceNameFor(m.device_subscriber_id),
        oem: m.oem_id,
        variant: m.device_variant,
        ts: tstamp,
    });
}

function processDeviceInfo(m, tstamp) {
    if (!m.firmware) return; /* request frames carry firmware=0, ignore */
    const prev = state.devices.get(m.subscriber_id) || {};
    state.devices.set(m.subscriber_id, {
        ...prev,
        name: m.subscriber_name || prev.name || deviceNameFor(m.subscriber_id),
        oem: m.oem_id ?? prev.oem,
        variant: m.device_variant ?? prev.variant,
        firmware: m.firmware,
        ts: tstamp,
    });
}

function processDatetime(m, tstamp) {
    if (!m.iso8601) return;
    if (state.datetimes.length && state.datetimes[0].iso8601 === m.iso8601) return; /* dup */
    const reported = Math.floor(Date.parse(m.iso8601) / 1000);
    state.datetimes.unshift({
        iso8601: m.iso8601,
        dow: m.dow,
        skew_secs: Number.isFinite(reported) ? reported - Math.floor(Date.now() / 1000) : null,
        ts: tstamp,
    });
    if (state.datetimes.length > 4) state.datetimes.length = 4;
}

/* ============================================================= probes == */

const onewiresBox = mkBox({
    parent: screen,
    label: ' Probes (panel + 1-wire, <host>.<slot>) ',
    top: HR_DEVICES,
    left: LAYOUT.COL3_LEFT,
    width: LAYOUT.COL3_WIDTH,
    height: HR_ONEWIRE,
});

function renderProbes() {
    const lines = ['{bold}{white-fg}  addr     field     value       offset    age{/}{/bold}'];
    const sorted = [...state.probes.values()].sort((a, b) => a.host - b.host || a.slot - b.slot);
    for (const probe of sorted) {
        for (const fn of ['temp', 'humidity']) {
            const p = probe[fn];
            if (!p || p.value == null) continue;
            const unit = fn === 'temp' ? '°C' : '%';
            const off = p.offset != null ? `${p.offset >= 0 ? '+' : ''}${p.offset.toFixed(1)}${unit}` : '--';
            const addr = `${probe.host}.${probe.slot + 1}`;
            lines.push(`  ${addr.padEnd(7)}  ${fn.padEnd(8)}  ${p.value.toFixed(2).padStart(6)}${unit}  {#888-fg}${off.padStart(6)}{/}  ${ageStr(p.ts)}`);
        }
    }
    onewiresBox.setContent(lines.join('\n'));
}

function processProbe(m, tstamp, isOffset) {
    const key = `${m.subscriber_id}.${m.slot}`;
    const probe = state.probes.get(key) || { host: m.subscriber_id, slot: m.slot, temp: {}, humidity: {} };
    const p = probe[m.field] || {};
    if (isOffset) {
        p.offset = m.offset;
        p.offset_unit = m.unit;
        p.offset_ts = tstamp;
        if (p.ts == null) p.ts = tstamp;
    } else {
        p.value = m.value;
        p.unit = m.unit;
        p.ts = tstamp;
    }
    probe[m.field] = p;
    state.probes.set(key, probe);
}

/* ============================================================= status == */

const status = blessed.text({
    parent: screen,
    top: LAYOUT.TOP_HEIGHT,
    left: 0,
    width: LAYOUT.STATUS_WIDTH,
    height: LAYOUT.BOTTOM_HEIGHT,
    border: 'line',
    tags: true,
    style: { border: { fg: 'cyan' } },
    label: ' Status ',
});

function renderStatus() {
    status.setContent(
        ` ${mqttClient && mqttClient.connected ? '{green-fg}●{/}' : '{red-fg}○{/}'} ${HOST}:${PORT}\n` +
            ` {cyan-fg}sub:{/} ${PREFIX}/#\n` +
            ` {cyan-fg}msgs:{/} ${state.msgs}\n` +
            ` {cyan-fg}unkn:{/} ${state.unknown}\n` +
            ` {cyan-fg}rooms:{/} ${roomEntries.length}/${state.rooms.size - roomEntries.length}\n` +
            ` {#888-fg}q quit{/}`
    );
}

/* ============================================================= events == */

const events = mkBox({
    parent: screen,
    label: ' Events ',
    top: LAYOUT.TOP_HEIGHT,
    left: LAYOUT.LOG_LEFT,
    width: LAYOUT.LOG_WIDTH,
    height: LAYOUT.BOTTOM_HEIGHT,
});

function previewFor(m) {
    const t = m.type;
    if (t === 'sensor') return `${m.function} val=${m.value}${m.unit || ''}` + (m.room_name ? ` room=${m.room_name}` : ` ri=${m.room_index} rt=${m.room_type}`);
    if (t === 'hc_sensor')
        return `sno=${m.sensor_no}${m.name ? '(' + m.name + ')' : ''} type=0x${(m.sensor_type || 0).toString(16).toUpperCase()} raw=${m.value_raw}`;
    if (t === 'hc_relay') return `rno=${m.relay_no}${m.name ? '(' + m.name + ')' : ''} mode=0x${(m.mode || 0).toString(16).toUpperCase()} raw=${m.value_raw}`;
    if (t === 'heartbeat')
        return `dev=0x${(m.device_subscriber_id || 0).toString(16).toUpperCase()} oem=0x${(m.oem_id || 0).toString(16).toUpperCase()} var=0x${(m.device_variant || 0).toString(16).toUpperCase()}`;
    if (t === 'device_info')
        return `dev=0x${(m.subscriber_id || 0).toString(16).toUpperCase()} oem=0x${(m.oem_id || 0).toString(16).toUpperCase()} var=0x${(m.device_variant || 0).toString(16).toUpperCase()} fw=${m.firmware}`;
    if (t === 'system_datetime') return `${m.iso8601} dow=${m.dow}`;
    if (t === 'param')
        return `zone=${m.zone || '?'} ${m.relay_name ? '(' + m.relay_name + ') ' : ''}${m.param_name || 'pid' + m.param_id} = ${m.value !== undefined ? m.value : m.value_raw}${m.unit || ''}`;
    if (t === 'local_sensor') return `${m.subscriber_id}.${(m.slot ?? 0) + 1} ${m.field}=${m.value != null ? m.value.toFixed(2) : '--'}${m.unit || ''}`;
    if (t === 'local_offset')
        return `${m.subscriber_id}.${(m.slot ?? 0) + 1} ${m.field} offset=${m.offset >= 0 ? '+' : ''}${m.offset != null ? m.offset.toFixed(1) : '--'}${m.unit || ''}`;
    return JSON.stringify(m).slice(0, 80);
}

function logEvent(topic, m) {
    const d = new Date();
    state.raws.push(
        ` ${String(d.getHours()).padStart(2, '0')}:${String(d.getMinutes()).padStart(2, '0')}:${String(d.getSeconds()).padStart(2, '0')} {cyan-fg}${topic.replace(PREFIX + '/', '').padEnd(11)}{/} ${previewFor(m)}`
    );
    if (state.raws.length > 200) state.raws.shift();
}

function renderEvents() {
    events.setContent(state.raws.slice(-Math.max(1, events.height - 2)).join('\n'));
}

/* =========================================================== dispatch == */

function doRender() {
    renderRooms();
    renderZones();
    renderSensors();
    renderRelays();
    renderDevices();
    renderProbes();
    renderStatus();
    renderEvents();
    screen.render();
}

function doMessage(topic, m) {
    const tstamp = m.ts || Date.now() / 1000;
    logEvent(topic, m);
    switch (m.type) {
        case 'sensor':
            return processSensor(m, tstamp);
        case 'hc_sensor':
            return processHcSensor(m, tstamp);
        case 'hc_relay':
            return processHcRelay(m, tstamp);
        case 'heartbeat':
            return processHeartbeat(m, tstamp);
        case 'device_info':
            return processDeviceInfo(m, tstamp);
        case 'system_datetime':
            return processDatetime(m, tstamp);
        case 'param':
            return processParam(m, tstamp);
        case 'local_sensor':
            return processProbe(m, tstamp, false);
        case 'local_offset':
            return processProbe(m, tstamp, true);
        default:
            state.unknown++;
    }
}

/* ================================================================ mqtt == */

const mqttClient = mqtt.connect(`mqtt://${HOST}:${PORT}`, { reconnectPeriod: 2000 });
mqttClient.on('connect', () => {
    mqttClient.subscribe(`${PREFIX}/#`, { qos: 0 });
});
mqttClient.on('message', (topic, buf) => {
    state.msgs++;
    let m;
    try {
        m = JSON.parse(buf.toString());
    } catch {
        state.unknown++;
        return;
    }
    doMessage(topic, m);
});

screen.key(['q', 'C-c'], () => process.exit(0));

setInterval(doRender, 500);
doRender();

/* END */
