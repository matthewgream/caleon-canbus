#!/usr/bin/env node
/*
 * caleon-tui -- blessed TUI that subscribes to caleon/# and shows
 *               live room / relay / HC-sensor / device state.
 *
 * Run: node caleon-tui.js [--config caleon2mqtt.cfg] [--host localhost]
 */

const blessed = require('blessed');
const mqtt    = require('mqtt');
const fs      = require('fs');
const path    = require('path');

/* ---------------------------------------------------------------- args */
const argv = process.argv.slice(2);
const argFor = (k, d) => {
  const i = argv.indexOf(k);
  return i >= 0 && argv[i + 1] ? argv[i + 1] : d;
};
const CFG_PATH = argFor('--config', path.join(__dirname, 'caleon2mqtt.cfg'));
const HOST     = argFor('--host', 'localhost');
const PORT     = parseInt(argFor('--port', '1883'), 10);
const PREFIX   = argFor('--prefix', 'caleon');

/* ---------------------------------------------------------------- config */
let cfg = { rooms: {}, subscribers: {}, hc_sensors: {}, hc_relays: {} };
try {
  const raw = fs.readFileSync(CFG_PATH, 'utf8');
  const parsed = JSON.parse(raw);
  cfg = { ...cfg, ...parsed };
} catch (e) {
  console.error(`config: cannot load ${CFG_PATH}: ${e.message} -- using empty defaults`);
}

/* Build an ordered room list from config. Key is "<idx>:<type>". */
const roomEntries = Object.entries(cfg.rooms || {})
  .map(([k, v]) => {
    const [ridx, rtyp] = k.split(':').map(n => parseInt(n, 10));
    const name = typeof v === 'string' ? v : (v && v.name) || k;
    return { key: k, ridx, rtyp, name };
  })
  .sort((a, b) => a.ridx - b.ridx);

const roomByKey = new Map(roomEntries.map(r => [r.key, r]));

/* ---------------------------------------------------------------- state */
const now = () => Date.now() / 1000;
const ageStr = (t) => {
  if (!t) return '   -';
  const dt = Math.max(0, Math.floor(now() - t));
  if (dt < 60)    return `${dt}s`.padStart(4);
  if (dt < 3600)  return `${Math.floor(dt/60)}m`.padStart(4);
  return `${Math.floor(dt/3600)}h`.padStart(4);
};

const state = {
  rooms:   new Map(),  /* key "ri:rt" -> {temp, tset, humidity, swval, t_*} */
  relays:  new Map(),  /* no -> {mode, value_raw, ts} */
  sensors: new Map(),  /* no -> {value, type, ts} */
  devices: new Map(),  /* subscriber_id -> {name, oem, variant, ts} */
  central: new Map(),  /* function name -> {value, unit, present, ts} (RI=0,RT=0) */
  unknown: 0,
  msgs: 0,
  raws: [],            /* tail of {topic, type, line} */
};

roomEntries.forEach(r => state.rooms.set(r.key, {}));

/* ---------------------------------------------------------------- ui */
const screen = blessed.screen({ smartCSR: true, title: 'caleon-tui',
                                fullUnicode: true });

const mkBox = (opts) => blessed.box(Object.assign({
  border: 'line',
  tags: true,
  style: { border: { fg: 'cyan' }, label: { fg: 'white', bold: true } },
}, opts));

const roomsBox = mkBox({
  parent: screen, label: ' Rooms (ecsRcTroom / Tset / Switch / Humidity) ',
  top: 0, left: 0, width: '60%', height: '45%',
});
const devBox = mkBox({
  parent: screen, label: ' Devices & Flow ',
  top: 0, left: '60%', width: '40%', height: '25%',
});
const relayBox = mkBox({
  parent: screen, label: ' Relays (0x8B DLF_RELAY) ',
  top: '25%', left: '60%', width: '40%', height: '40%',
});
const sensorBox = mkBox({
  parent: screen, label: ' HC Sensors (0x8B DLF_SENSOR) ',
  top: '45%', left: 0, width: '60%', height: '40%',
});
const logBox = mkBox({
  parent: screen, label: ' Event log ',
  top: '85%', left: 0, width: '100%', height: '15%',
  style: { border: { fg: 'gray' } },
});

const status = blessed.text({
  parent: screen, top: '65%', left: '60%', width: '40%', height: '20%',
  border: 'line', tags: true,
  style: { border: { fg: 'cyan' } },
  label: ' Status ',
});

screen.key(['q', 'C-c'], () => process.exit(0));

/* ---------------------------------------------------------------- render */
const fmtVal = (v, suffix = '', width = 7) => {
  if (v === null || v === undefined || Number.isNaN(v))
    return '   -- '.padStart(width);
  return (typeof v === 'number' ? v.toFixed(1) : String(v)).padStart(width) + suffix;
};

function render() {
  /* rooms */
  let lines = [];
  lines.push('{bold}{white-fg} idx rt   name              temp     set   mode      hum   age{/}{/bold}');
  for (const r of roomEntries) {
    const s = state.rooms.get(r.key) || {};
    const ageT = ageStr(s.t_temp || s.t_tset || s.t_sw || s.t_hum);
    const modeCell = s.swval == null ? '   --   '
                   : (s.swmode || `?(${s.swval})`).padEnd(8);
    lines.push(
      ` ${String(r.ridx).padStart(2)}  ${String(r.rtyp).padStart(2)} ` +
      ` {yellow-fg}${(r.name || '').padEnd(14)}{/yellow-fg}` +
      ` ${fmtVal(s.temp,    '°C')}` +
      ` ${fmtVal(s.tset,    '°C')}` +
      ` ${modeCell}` +
      ` ${fmtVal(s.humidity, '%', 5)}` +
      `  ${ageT}`
    );
  }
  /* unmapped rooms that appeared on the bus */
  for (const [k, s] of state.rooms.entries()) {
    if (roomByKey.has(k)) continue;
    const [ri, rt] = k.split(':');
    lines.push(
      ` ${ri.padStart(2)}  ${rt.padStart(2)}  {red-fg}(unmapped)    {/red-fg}` +
      ` ${fmtVal(s.temp,'°C')} ${fmtVal(s.tset,'°C')}` +
      ` ${(s.swval ?? '--').toString().padStart(4)}` +
      ` ${fmtVal(s.humidity,'%',5)}  ${ageStr(s.t_temp || s.t_tset)}`
    );
  }
  roomsBox.setContent(lines.join('\n'));

  /* devices + flow */
  lines = [];
  lines.push('{bold}{white-fg} addr  name              oem  var  age{/}{/bold}');
  for (const [id, d] of [...state.devices.entries()].sort()) {
    lines.push(
      ` 0x${id.toString(16).padStart(2,'0').toUpperCase()}  ` +
      `{yellow-fg}${(d.name || '?').padEnd(14)}{/yellow-fg}` +
      `  0x${(d.oem ?? 0).toString(16).padStart(2,'0').toUpperCase()}` +
      `  0x${(d.variant ?? 0).toString(16).padStart(2,'0').toUpperCase()}` +
      `  ${ageStr(d.ts)}`
    );
  }
  lines.push('');
  lines.push('{bold}{white-fg} Central plant (RI=0,RT=0){/}{/bold}');
  if (state.central.size === 0) {
    lines.push('  (waiting...)');
  } else {
    for (const [fn, c] of [...state.central.entries()].sort()) {
      lines.push(
        `  ${fn.padEnd(14)} ${fmtVal(c.value, c.unit ? '°C' : '   ')}` +
        `${c.present ? '' : ' {gray-fg}(absent){/}'}  age ${ageStr(c.ts)}`
      );
    }
  }
  devBox.setContent(lines.join('\n'));

  /* relays -- bytes 4-7 are state-related but we don't yet know which is on/off,
   * so display all four so the user can spot which one moves first. */
  lines = [];
  lines.push('{bold}{white-fg}  no  name         label flag  extra(4..7)  age{/}{/bold}');
  for (const [no, r] of [...state.relays.entries()].sort((a,b) => a[0]-b[0])) {
    const nm = (cfg.hc_relays && (cfg.hc_relays[`0x${no.toString(16).padStart(2,'0').toUpperCase()}`] || cfg.hc_relays[`0x${no.toString(16).padStart(2,'0')}`] || cfg.hc_relays[String(no)])) || '';
    const extra = (r.extra || []).map(b => b.toString(16).padStart(2,'0').toUpperCase()).join(' ').padEnd(11);
    const allZero = (r.extra || []).every(b => b === 0);
    const extraStyled = allZero ? `{gray-fg}${extra}{/}` : `{yellow-fg}${extra}{/}`;
    lines.push(
      `  ${String(no).padStart(2)}  ${nm.padEnd(11)}  ` +
      `"${(r.label||'').padEnd(2)}"  ` +
      `0x${(r.flag ?? 0).toString(16).padStart(2,'0').toUpperCase()}  ` +
      `${extraStyled}  ${ageStr(r.ts)}`
    );
  }
  relayBox.setContent(lines.join('\n'));

  /* sensors */
  lines = [];
  lines.push('{bold}{white-fg}  no  name              kind         raw      value     age{/}{/bold}');
  for (const [no, s] of [...state.sensors.entries()].sort((a,b) => a[0]-b[0])) {
    const nm = (cfg.hc_sensors && (cfg.hc_sensors[`0x${no.toString(16).padStart(2,'0').toUpperCase()}`] || cfg.hc_sensors[`0x${no.toString(16).padStart(2,'0')}`] || cfg.hc_sensors[String(no)])) || '';
    const absent = (s.present === false || s.value === null || s.value === undefined);
    const kind = absent ? '{gray-fg}(absent)    {/}' : (s.type_name || '?').padEnd(12);
    const valCell = absent ? '{gray-fg}    --   {/}'
                           : `${String(s.value).padStart(6)} ${(s.unit||'').padEnd(4)}`;
    lines.push(
      `  ${String(no).padStart(2)}  ${nm.padEnd(16)}  ${kind}` +
      `  ${String(s.value_raw ?? '--').padStart(6)}` +
      `  ${valCell}  ${ageStr(s.ts)}`
    );
  }
  sensorBox.setContent(lines.join('\n'));

  /* status */
  status.setContent(
    `{cyan-fg}MQTT:{/}     ${HOST}:${PORT} ${mqttClient && mqttClient.connected ? '{green-fg}connected{/}' : '{red-fg}offline{/}'}\n` +
    `{cyan-fg}Topic:{/}    ${PREFIX}/#\n` +
    `{cyan-fg}Messages:{/} ${state.msgs}   {cyan-fg}Unknown types:{/} ${state.unknown}\n` +
    `{cyan-fg}Rooms:{/}    ${roomEntries.length} mapped, ${state.rooms.size - roomEntries.length} unmapped\n` +
    `q to quit`
  );

  /* log tail */
  logBox.setContent(state.raws.slice(-Math.max(1, logBox.height - 2)).join('\n'));

  screen.render();
}

/* ---------------------------------------------------------------- mqtt */
const mqttClient = mqtt.connect(`mqtt://${HOST}:${PORT}`, {
  reconnectPeriod: 2000,
});

mqttClient.on('connect', () => {
  mqttClient.subscribe(`${PREFIX}/#`, { qos: 0 });
});

mqttClient.on('message', (topic, buf) => {
  state.msgs++;
  let m;
  try { m = JSON.parse(buf.toString()); }
  catch { state.unknown++; return; }

  const t = m.type;
  const tstamp = m.ts || (Date.now() / 1000);

  /* event-log tail (short form) */
  const short = topic.replace(`${PREFIX}/`, '');
  let preview = '';
  if (t === 'sensor') {
    preview = `${m.function} val=${m.value}${m.unit||''}` +
              (m.room_name ? ` room=${m.room_name}` : ` ri=${m.room_index} rt=${m.room_type}`);
  } else if (t === 'hc_sensor') {
    preview = `sno=${m.sensor_no}${m.name?'('+m.name+')':''} type=0x${(m.sensor_type||0).toString(16).toUpperCase()} raw=${m.value_raw}`;
  } else if (t === 'hc_relay') {
    preview = `rno=${m.relay_no}${m.name?'('+m.name+')':''} mode=0x${(m.mode||0).toString(16).toUpperCase()} raw=${m.value_raw}`;
  } else if (t === 'heartbeat') {
    preview = `dev=0x${(m.device_subscriber_id||0).toString(16).toUpperCase()} oem=0x${(m.oem_id||0).toString(16).toUpperCase()} var=0x${(m.device_variant||0).toString(16).toUpperCase()}`;
  } else {
    preview = JSON.stringify(m).slice(0, 80);
  }
  state.raws.push(`{gray-fg}${new Date().toLocaleTimeString()}{/} {cyan-fg}${short.padEnd(11)}{/} ${preview}`);
  if (state.raws.length > 200) state.raws.shift();

  /* dispatch into state */
  if (t === 'sensor') {
    const present = m.present !== false && m.value !== null;
    /* (RI=0, RT=0) is the "central plant" -- not a room. CALEONbox sends
     * its system-wide sensors (ecsRcTflow, ecsFlow, ecsOutdor, ...) with
     * those zero room fields. Route them to the central panel instead. */
    if (m.room_index === 0 && m.room_type === 0) {
      state.central.set(m.function, {
        value: present ? m.value : null, unit: m.unit || '', present, ts: tstamp,
      });
    } else {
      const key = `${m.room_index}:${m.room_type}`;
      const s = state.rooms.get(key) || {};
      if (m.function === 'ecsRcTroom')    { s.temp     = present ? m.value : null;     s.t_temp = tstamp; }
      if (m.function === 'ecsRcTset')     { s.tset     = present ? m.value : null;     s.t_tset = tstamp; }
      if (m.function === 'ecsRcHumidity') { s.humidity = present ? m.value : null;     s.t_hum  = tstamp; }
      if (m.function === 'ecsRcSwitch')   {
        s.swval = present ? m.value_raw : null;
        s.swmode = (present && m.mode_name) ? m.mode_name : null;
        s.t_sw = tstamp;
      }
      state.rooms.set(key, s);
    }
  } else if (t === 'hc_relay') {
    state.relays.set(m.relay_no, { flag: m.flag, label: m.label || '',
                                    extra: m.extra || [], ts: tstamp });
  } else if (t === 'hc_sensor') {
    const present = m.present !== false && m.value !== null;
    state.sensors.set(m.sensor_no, { type: m.sensor_type,
                                     type_name: m.sensor_type_name || '',
                                     unit: m.unit || '',
                                     value: present ? m.value : null,
                                     value_raw: m.value_raw,
                                     present, ts: tstamp });
  } else if (t === 'heartbeat') {
    const id = m.device_subscriber_id;
    state.devices.set(id, {
      name: (cfg.subscribers && (cfg.subscribers[`0x${id.toString(16).padStart(2,'0').toUpperCase()}`] || cfg.subscribers[`0x${id.toString(16).padStart(2,'0')}`])) || '',
      oem: m.oem_id, variant: m.device_variant, ts: tstamp,
    });
  } else {
    state.unknown++;
  }
});

/* ---------------------------------------------------------------- loop */
setInterval(render, 500);
render();
