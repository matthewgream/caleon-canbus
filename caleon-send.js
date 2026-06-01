#!/usr/bin/env node
/*
 * caleon-send -- send Sorel SCBI CONTROLLER (0x0B) requests over CAN.
 *
 * Builds the 29-bit address ID per SCBI rev11:
 *
 *   bits  0.. 7  Program Type     (0x0B CONTROLLER)
 *   bits  8..15  Subscriber ID    sender id (us); default 0xA5, currently
 *                                 unused on this bus -- pass --sender to override
 *   bits 16..23  Function Type    see CMDS below
 *   bits 24..26  Protocol Type    0 (CAN_FORMAT_0)
 *   bits 27..28  CAN Message Type 0 (MSG_REQUEST)
 *
 * Shells out to cansend(1) from can-utils, so any socketcan iface works
 * (vcan0 / can0). Body convention: data[0] = target subscriber id, mirroring
 * the observed DEVICE_INFO request shape. 0x00 means "unspecified".
 *
 * Watch for responses in one of:
 *   - tail -F /var/log/caleon2mqtt-all.log         (if capture_all is enabled)
 *   - ./caleon2mqtt -c ... --debug                  (live stdout)
 *   - mosquitto_sub -h localhost -t 'caleon/#' -v   (decoded JSON)
 */

const { spawnSync } = require('node:child_process');

const PROGRAM_CONTROLLER = 0x0b;
const PROTO_FORMAT_0 = 0;
const MSG_REQUEST = 0;

/* CONTROLLER (0x0B) function types from SCBI rev11 table 3. The doc only
 * fully describes (0), (1), (8); the GET_* / SET_* entries are listed but
 * the body format is left to discover -- that's why this tool exists. */
const CMDS = {
    has_anybody_here: { fn: 0x00, note: 'discover -- expect each controller to reply with I_AM_HERE' },
    i_am_here: { fn: 0x01, note: 'normally sent every 60s by each controller' },
    get_controller_id: { fn: 0x02, note: 'query controller for its currently-selected control program number' },
    get_active_programs_list: { fn: 0x03, note: 'query the list of available control programs' },
    add_program: { fn: 0x04, note: 'destructive -- avoid unless you know what you are doing' },
    remove_program: { fn: 0x05, note: 'destructive -- avoid unless you know what you are doing' },
    get_system_date_time: { fn: 0x06, note: 'query the controller clock' },
    set_system_date_time: { fn: 0x07, note: 'destructive -- writes the clock' },
    i_am_reseted: { fn: 0x08, note: 'normally sent by a controller after reset' },
    datalogger_test: { fn: 0x09, note: 'datalogger hardware test' },
    device_info: { fn: 0x14, note: 'OEM/variant/firmware (already used in caleon2mqtt)' },
};

function parseId(s) {
    if (typeof s === 'number') return s;
    const v = /^0x/i.test(s) ? parseInt(s, 16) : parseInt(s, 10);
    if (!Number.isFinite(v) || v < 0 || v > 0xff) throw new Error(`bad id: ${s}`);
    return v;
}

function buildId({ msgType = MSG_REQUEST, fnType, subId, progType = PROGRAM_CONTROLLER, protoType = PROTO_FORMAT_0 }) {
    return (((msgType & 3) << 27) | ((protoType & 7) << 24) | ((fnType & 0xff) << 16) | ((subId & 0xff) << 8) | (progType & 0xff)) >>> 0;
}

const hex2 = (n) => n.toString(16).toUpperCase().padStart(2, '0');
const hex8 = (n) => n.toString(16).toUpperCase().padStart(8, '0');

function send(iface, id, data, dryRun) {
    const frame = `${hex8(id)}#${data.map(hex2).join('')}`;
    const cmd = `cansend ${iface} ${frame}`;
    console.log(`> ${cmd}`);
    if (dryRun) return;
    const r = spawnSync('cansend', [iface, frame], { stdio: 'inherit' });
    if (r.status !== 0) {
        console.error(`cansend exited ${r.status}`);
        process.exitCode = r.status ?? 1;
    }
}

function usage() {
    const cmds = Object.entries(CMDS)
        .map(([k, v]) => `  ${k.padEnd(26)} fn=0x${hex2(v.fn)}  ${v.note}`)
        .join('\n');
    console.error(`Usage: caleon-send.js [opts] <cmd> [<cmd> ...]

Options:
  -i, --iface IFACE     CAN interface (default vcan0)
  -s, --sender ID       our subscriber id (default 0xA5, currently unused on this bus)
  -t, --target ID       target controller id in data[0] (default 0x00 = unspecified)
                        try 0xB5 (caleon_box) or 0x97 (caleon_s) if no reply
  -n, --dry-run         print the cansend command but don't execute it

Commands (CONTROLLER / 0x0B):
${cmds}

Watch for responses with one of:
  tail -F /var/log/caleon2mqtt-all.log          # if debug.capture_all is enabled
  ./caleon2mqtt -c ... --debug                  # live stdout
  mosquitto_sub -h localhost -t 'caleon/#' -v   # decoded JSON (subset)`);
}

function main() {
    const argv = process.argv.slice(2);
    const opts = { iface: 'vcan0', sender: 0xa5, target: 0x00, dryRun: false, cmds: [] };
    for (let i = 0; i < argv.length; i++) {
        const a = argv[i];
        if (a === '-i' || a === '--iface') opts.iface = argv[++i];
        else if (a === '-s' || a === '--sender') opts.sender = parseId(argv[++i]);
        else if (a === '-t' || a === '--target') opts.target = parseId(argv[++i]);
        else if (a === '-n' || a === '--dry-run') opts.dryRun = true;
        else if (a === '-h' || a === '--help') {
            usage();
            return;
        } else if (a.startsWith('-')) {
            console.error(`unknown option: ${a}`);
            usage();
            process.exit(2);
        } else {
            opts.cmds.push(a);
        }
    }
    if (opts.cmds.length === 0) {
        usage();
        process.exit(2);
    }
    for (const name of opts.cmds) {
        const c = CMDS[name];
        if (!c) {
            console.error(`unknown command: ${name}`);
            process.exit(2);
        }
        const id = buildId({ fnType: c.fn, subId: opts.sender });
        const data = [opts.target & 0xff];
        send(opts.iface, id, data, opts.dryRun);
    }
}

main();
