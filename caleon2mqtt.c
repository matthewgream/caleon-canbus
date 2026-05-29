/*
 * caleon2mqtt -- decode Caleon/Sorel CAN traffic and republish it as JSON
 *                over MQTT, while preserving the same human-readable debug
 *                output as test.c so we can keep reverse-engineering frames
 *                we don't yet understand.
 *
 * Build:  make
 * Run:    ./caleon2mqtt [-c caleon2mqtt.cfg] [-i vcan0]
 */

#include <errno.h>
#include <fcntl.h>
#include <getopt.h>
#include <signal.h>
#include <stdarg.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>

#include <linux/can.h>
#include <linux/can/raw.h>
#include <net/if.h>
#include <sys/ioctl.h>
#include <sys/socket.h>

#include <cjson/cJSON.h>
#include <mosquitto.h>

/* ------------------------------------------------------------------ config */

typedef struct {
    char can_iface[IFNAMSIZ];

    char mqtt_host[128];
    int mqtt_port;
    int mqtt_qos;
    bool mqtt_retain_sensors;
    char mqtt_topic[128]; /* prefix; per-event subtopic appended  */
    char mqtt_client_id[128];
    char mqtt_username[64];
    char mqtt_password[64];
    int mqtt_keepalive;

    cJSON *root;        /* parsed config tree                    */
    cJSON *rooms;       /* "rooms"      : { "<idx>:<type>": ... } */
    cJSON *subscribers; /* "subscribers": { "0xNN": "name" }     */
    cJSON *hc_sensors;  /* "hc_sensors" : { "<no>": "name" }     */
    cJSON *hc_relays;   /* "hc_relays"  : { "<no>": "name" }     */
} config_t;

static config_t g_cfg;
static volatile sig_atomic_t g_run = 1;
static bool g_debug                = false;

/* Verbose per-frame decoder trace -- gated by --debug. When off the
 * program runs silent on stdout (still emits errors and lifecycle
 * lines on stderr) so it is safe to run as a systemd service. */
static inline void dbg_out(const char *fmt, ...) {
    if (!g_debug)
        return;
    va_list ap;
    va_start(ap, fmt);
    vfprintf(stdout, fmt, ap);
    va_end(ap);
}
#define DBGP(...) dbg_out(__VA_ARGS__)

static void on_signal(int sig) {
    (void)sig;
    g_run = 0;
}

static char *slurp(const char *path) {
    FILE *f = fopen(path, "rb");
    if (!f)
        return NULL;
    if (fseek(f, 0, SEEK_END) != 0) {
        fclose(f);
        return NULL;
    }
    long n = ftell(f);
    if (n < 0) {
        fclose(f);
        return NULL;
    }
    rewind(f);
    char *buf = malloc((size_t)n + 1);
    if (!buf) {
        fclose(f);
        return NULL;
    }
    size_t got = fread(buf, 1, (size_t)n, f);
    fclose(f);
    buf[got] = '\0';
    return buf;
}

static const char *json_str(const cJSON *o, const char *k, const char *dflt) {
    if (!o)
        return dflt;
    cJSON *v = cJSON_GetObjectItemCaseSensitive(o, k);
    return (v && cJSON_IsString(v)) ? v->valuestring : dflt;
}
static int json_int(const cJSON *o, const char *k, int dflt) {
    if (!o)
        return dflt;
    cJSON *v = cJSON_GetObjectItemCaseSensitive(o, k);
    return (v && cJSON_IsNumber(v)) ? v->valueint : dflt;
}
static bool json_bool(const cJSON *o, const char *k, bool dflt) {
    if (!o)
        return dflt;
    cJSON *v = cJSON_GetObjectItemCaseSensitive(o, k);
    if (!v)
        return dflt;
    if (cJSON_IsBool(v))
        return cJSON_IsTrue(v);
    if (cJSON_IsNumber(v))
        return v->valueint != 0;
    return dflt;
}

static void cfg_defaults(void) {
    memset(&g_cfg, 0, sizeof(g_cfg));
    strncpy(g_cfg.can_iface, "vcan0", IFNAMSIZ - 1);
    strncpy(g_cfg.mqtt_host, "localhost", sizeof(g_cfg.mqtt_host) - 1);
    g_cfg.mqtt_port           = 1883;
    g_cfg.mqtt_qos            = 0;
    g_cfg.mqtt_keepalive      = 60;
    g_cfg.mqtt_retain_sensors = false;
    strncpy(g_cfg.mqtt_topic, "caleon", sizeof(g_cfg.mqtt_topic) - 1);
    snprintf(g_cfg.mqtt_client_id, sizeof(g_cfg.mqtt_client_id), "caleon2mqtt-%d", (int)getpid());
}

static int cfg_load(const char *path) {
    cfg_defaults();
    char *buf = slurp(path);
    if (!buf) {
        fprintf(stderr, "config: cannot read '%s' (%s) -- using defaults\n", path, strerror(errno));
        return 0;
    }
    g_cfg.root = cJSON_Parse(buf);
    free(buf);
    if (!g_cfg.root) {
        fprintf(stderr, "config: parse error near '%s'\n", cJSON_GetErrorPtr() ? cJSON_GetErrorPtr() : "?");
        return -1;
    }

    cJSON *can = cJSON_GetObjectItemCaseSensitive(g_cfg.root, "can");
    if (can)
        strncpy(g_cfg.can_iface, json_str(can, "interface", g_cfg.can_iface), IFNAMSIZ - 1);

    cJSON *m = cJSON_GetObjectItemCaseSensitive(g_cfg.root, "mqtt");
    if (m) {
        strncpy(g_cfg.mqtt_host, json_str(m, "host", g_cfg.mqtt_host), sizeof(g_cfg.mqtt_host) - 1);
        g_cfg.mqtt_port           = json_int(m, "port", g_cfg.mqtt_port);
        g_cfg.mqtt_qos            = json_int(m, "qos", g_cfg.mqtt_qos);
        g_cfg.mqtt_keepalive      = json_int(m, "keepalive", g_cfg.mqtt_keepalive);
        g_cfg.mqtt_retain_sensors = json_bool(m, "retain_sensors", g_cfg.mqtt_retain_sensors);
        strncpy(g_cfg.mqtt_topic, json_str(m, "topic", g_cfg.mqtt_topic), sizeof(g_cfg.mqtt_topic) - 1);
        const char *cid = json_str(m, "client_id", NULL);
        if (cid)
            strncpy(g_cfg.mqtt_client_id, cid, sizeof(g_cfg.mqtt_client_id) - 1);
        const char *u = json_str(m, "username", NULL);
        if (u)
            strncpy(g_cfg.mqtt_username, u, sizeof(g_cfg.mqtt_username) - 1);
        const char *p = json_str(m, "password", NULL);
        if (p)
            strncpy(g_cfg.mqtt_password, p, sizeof(g_cfg.mqtt_password) - 1);
    }

    g_cfg.rooms       = cJSON_GetObjectItemCaseSensitive(g_cfg.root, "rooms");
    g_cfg.subscribers = cJSON_GetObjectItemCaseSensitive(g_cfg.root, "subscribers");
    g_cfg.hc_sensors  = cJSON_GetObjectItemCaseSensitive(g_cfg.root, "hc_sensors");
    g_cfg.hc_relays   = cJSON_GetObjectItemCaseSensitive(g_cfg.root, "hc_relays");
    return 0;
}

/* "rooms" lookup: key is "<room_index>:<room_type>" (decimal),
 * value can be a string (the name) or an object with a "name" field. */
static const char *cfg_room_name(uint8_t idx, uint8_t type) {
    if (!g_cfg.rooms)
        return NULL;
    char key[16];
    snprintf(key, sizeof(key), "%u:%u", idx, type);
    cJSON *r = cJSON_GetObjectItemCaseSensitive(g_cfg.rooms, key);
    if (!r)
        return NULL;
    if (cJSON_IsString(r))
        return r->valuestring;
    cJSON *n = cJSON_GetObjectItemCaseSensitive(r, "name");
    return (n && cJSON_IsString(n)) ? n->valuestring : NULL;
}

/* Look up a name in a sub-object by an 8-bit id, tolerating
 * "0xNN" (upper), "0xnn" (lower), and decimal keys. */
static const char *cfg_lookup_u8(cJSON *obj, uint8_t id) {
    if (!obj)
        return NULL;
    char key[8];
    snprintf(key, sizeof(key), "0x%02X", id);
    cJSON *v = cJSON_GetObjectItemCaseSensitive(obj, key);
    if (!v) {
        snprintf(key, sizeof(key), "0x%02x", id);
        v = cJSON_GetObjectItemCaseSensitive(obj, key);
    }
    if (!v) {
        snprintf(key, sizeof(key), "%u", id);
        v = cJSON_GetObjectItemCaseSensitive(obj, key);
    }
    return (v && cJSON_IsString(v)) ? v->valuestring : NULL;
}

/* -------------------------------------------------------------------- MQTT */

static struct mosquitto *g_mosq = NULL;
static bool g_mqtt_ok           = false;

static void mqtt_on_connect(struct mosquitto *m, void *ud, int rc) {
    (void)m;
    (void)ud;
    g_mqtt_ok = (rc == 0);
    fprintf(stderr, "mqtt: %s (%s)\n", g_mqtt_ok ? "connected" : "connect failed", mosquitto_connack_string(rc));
}
static void mqtt_on_disconnect(struct mosquitto *m, void *ud, int rc) {
    (void)m;
    (void)ud;
    g_mqtt_ok = false;
    fprintf(stderr, "mqtt: disconnected (%s) -- will retry\n", mosquitto_strerror(rc));
}

static int mqtt_init(void) {
    mosquitto_lib_init();
    g_mosq = mosquitto_new(g_cfg.mqtt_client_id, true, NULL);
    if (!g_mosq) {
        fprintf(stderr, "mqtt: mosquitto_new failed\n");
        return -1;
    }
    mosquitto_connect_callback_set(g_mosq, mqtt_on_connect);
    mosquitto_disconnect_callback_set(g_mosq, mqtt_on_disconnect);
    if (g_cfg.mqtt_username[0])
        mosquitto_username_pw_set(g_mosq, g_cfg.mqtt_username, g_cfg.mqtt_password[0] ? g_cfg.mqtt_password : NULL);
    int rc = mosquitto_connect_async(g_mosq, g_cfg.mqtt_host, g_cfg.mqtt_port, g_cfg.mqtt_keepalive);
    if (rc != MOSQ_ERR_SUCCESS) {
        fprintf(stderr, "mqtt: connect_async %s:%d failed: %s\n", g_cfg.mqtt_host, g_cfg.mqtt_port, mosquitto_strerror(rc));
        return -1;
    }
    rc = mosquitto_loop_start(g_mosq);
    if (rc != MOSQ_ERR_SUCCESS) {
        fprintf(stderr, "mqtt: loop_start failed: %s\n", mosquitto_strerror(rc));
        return -1;
    }
    fprintf(stderr, "mqtt: target %s:%d, prefix '%s'\n", g_cfg.mqtt_host, g_cfg.mqtt_port, g_cfg.mqtt_topic);
    return 0;
}

static void mqtt_shutdown(void) {
    if (g_mosq) {
        mosquitto_disconnect(g_mosq);
        mosquitto_loop_stop(g_mosq, false);
        mosquitto_destroy(g_mosq);
        g_mosq = NULL;
    }
    mosquitto_lib_cleanup();
}

static void mqtt_publish(const char *subtopic, cJSON *payload, bool retain) {
    if (!g_mosq)
        return;
    char topic[256];
    if (subtopic && subtopic[0])
        snprintf(topic, sizeof(topic), "%s/%s", g_cfg.mqtt_topic, subtopic);
    else
        snprintf(topic, sizeof(topic), "%s", g_cfg.mqtt_topic);
    char *s = cJSON_PrintUnformatted(payload);
    if (!s)
        return;
    int rc = mosquitto_publish(g_mosq, NULL, topic, (int)strlen(s), s, g_cfg.mqtt_qos, retain);
    if (rc != MOSQ_ERR_SUCCESS && rc != MOSQ_ERR_NO_CONN)
        fprintf(stderr, "mqtt: publish %s failed: %s\n", topic, mosquitto_strerror(rc));
    free(s);
}

/* ---------------------------------------------------------------- CAN open */

static int can_open(const char *iface) {
    int s = socket(PF_CAN, SOCK_RAW, CAN_RAW);
    if (s < 0) {
        perror("socket(PF_CAN)");
        return -1;
    }
    struct ifreq ifr;
    memset(&ifr, 0, sizeof(ifr));
    /* snprintf truncates safely and always NUL-terminates -- avoids the
     * gcc -Wstringop-truncation noise that strncpy(..., IFNAMSIZ-1) trips. */
    snprintf(ifr.ifr_name, IFNAMSIZ, "%s", iface);
    if (ioctl(s, SIOCGIFINDEX, &ifr) < 0) {
        fprintf(stderr, "can: SIOCGIFINDEX(%s): %s\n", iface, strerror(errno));
        close(s);
        return -1;
    }
    struct sockaddr_can addr;
    memset(&addr, 0, sizeof(addr));
    addr.can_family  = AF_CAN;
    addr.can_ifindex = ifr.ifr_ifindex;
    if (bind(s, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
        fprintf(stderr, "can: bind(%s): %s\n", iface, strerror(errno));
        close(s);
        return -1;
    }
    return s;
}

/* ----------------------------------------------------- protocol dictionary */

typedef struct {
    uint32_t can_id;     /* raw 32-bit id (may carry CAN_EFF_FLAG bits)  */
    uint32_t can_eff_id; /* 29-bit extended id with flag bits stripped   */
    uint8_t dlc;
    uint8_t data[8];
    time_t ts;

    uint8_t program_type;
    uint8_t subscriber_id;
    uint8_t function_type;
    uint8_t protocol_type;
    uint8_t message_type;
} frame_t;

static const char *msg_type_str(uint8_t t) {
    switch (t) {
    case 0:
        return "MSG_REQUEST";
    case 1:
        return "MSG_RESERVE";
    case 2:
        return "MSG_RESPONSE";
    case 3:
        return "MSG_ERROR";
    default:
        return "UNKNOWN";
    }
}
/* Program Type table per docs/SOREL_CAN_bus_interface_rev11_COSTUMERS.pdf.
 * 0x8B is NOT in the rev-11 doc but the function-number layout (DLF_SENSOR,
 * DLF_RELAY, DLG_HYDRAULIK_CONFIG) is identical to 0x80 DATALOGGER_MONITOR --
 * almost certainly a CALEONbox-specific channel of the same datalogger. */
static const char *prog_name(uint8_t p) {
    switch (p) {
    case 0x0B:
        return "CONTROLLER";
    case 0x80:
        return "DATALOGGER_MONITOR";
    case 0x83:
        return "REMOTESENSOR";
    case 0x84:
        return "DATALOGGER_NAMEDSENSORS"; /* aka REMOTESENSOR (compat) */
    case 0x85:
        return "HCC"; /* heating circuit control */
    case 0x8B:
        return "HEATINGCONTROL"; /* see comment above */
    case 0x8C:
        return "AVAILABLERESOURCES";
    case 0x90:
        return "PARAMETERSYNCCONFIG";
    case 0x91:
        return "ROOMSYNC";
    case 0x94:
        return "MSGLOG";
    case 0x95:
        return "ROOMDATA"; /* Anybus gateway: room  */
                           /* data req/resp; see    */
                           /* docs/CBox_Documentation*/
    case 0x97:
        return "CALEON_S_PEER"; /* observed: CALEON S    */
                                /* program byte == its   */
                                /* own subscriber id;    */
                                /* peer channel to the   */
                                /* CALEONbox (B5).       */
    default:
        return "UNKNOWN";
    }
}

/* DLF_SENSOR sensor type enum (SCBI rev 11 page 5). Drives both the
 * unit string and the value scaling in decode_heatingcontrol(). */
static const char *hc_sensor_type_name(uint8_t t) {
    switch (t) {
    case 0:
        return "unknown";
    case 1:
        return "flow"; /* VFS sensor (l/min)         */
    case 2:
        return "relPressure"; /* RPS sensor (bar)           */
    case 3:
        return "diffPressure"; /* DPS sensor (bar)           */
    case 4:
        return "temperature"; /* decicelsius                */
    case 5:
        return "humidity"; /* decipercent                */
    case 6:
        return "rcWheel"; /* room controller wheel      */
    case 7:
        return "rcSwitch"; /* room controller switch     */
    default:
        return "?";
    }
}

/* DLF_RELAY mode enum from SCBI rev 11 (0=SWITCHED, 1=PHASE, 2=PWM,
 * 3=VOLTAGE) is the 0x80 layout. Our 0x8B channel uses a different
 * payload entirely (ASCII labels in bytes 2-3) so the mode enum is
 * not currently applied; left documented here for when we add 0x80. */
static const char *controller_fn(uint8_t f) {
    switch (f) {
    case 0x00:
        return "HAS_ANYBODY_HERE";
    case 0x01:
        return "I_AM_HERE";
    case 0x02:
        return "GET_CONTROLLER_ID";
    case 0x03:
        return "GET_ACTIVE_PROGRAMS_LIST";
    case 0x04:
        return "ADD_PROGRAM";
    case 0x05:
        return "REMOVE_PROGRAM";
    case 0x06:
        return "GET_SYSTEM_DATE_TIME";
    case 0x07:
        return "SET_SYSTEM_DATE_TIME";
    case 0x08:
        return "I_AM_RESETED";
    case 0x09:
        return "DATALOGGER_TEST";
    case 0x14:
        return "DEVICE_INFO"; /* observed: OEM, variant?, fw_ver_LE */
    default:
        return "UNKNOWN";
    }
}
static const char *remotesensor_fn(uint8_t f) {
    switch (f) {
    case 0x00:
        return "ecsOutdor";
    case 0x01:
        return "ecsFlow";
    case 0x02:
        return "ecsCold";
    case 0x03:
        return "ecsSolar";
    case 0x04:
        return "ecsReserved1";
    case 0x05:
        return "ecsReserved2";
    case 0x06:
        return "ecsReserved3";
    case 0x07:
        return "ecsStorage";
    case 0x08:
        return "ecsStorageTop";
    case 0x09:
        return "ecsStorageCenter";
    case 0x0A:
        return "ecsStorageBottom";
    case 0x0B:
        return "ecsReserved4";
    case 0x0C:
        return "ecsRcTset";
    case 0x0D:
        return "ecsRcHumidity";
    case 0x0E:
        return "ecsRcTroom";
    case 0x0F:
        return "ecsRcWheel";
    case 0x10:
        return "ecsRcSwitch";
    case 0x16:
        return "ecsRcTflow";
    default:
        return "UNKNOWN";
    }
}
static const char *heatingcontrol_fn(uint8_t f) {
    switch (f) {
    case 0x00:
        return "General_Status";
    case 0x01:
        return "DLF_SENSOR";
    case 0x02:
        return "DLF_RELAY";
    case 0x08:
        return "DLG_HYDRAULIK_CONFIG";
    default:
        return "UNKNOWN";
    }
}
static const char *paramsync_fn(uint8_t f) {
    switch (f) {
    case 0x00:
        return "epsc_connect";
    case 0x01:
        return "epsc_factoryReset";
    default:
        return "UNKNOWN";
    }
}

/* Sentinel "sensor not present" value used by the bus when a slot is
 * configured but no probe is wired. Confirmed on ecsRcHumidity for every
 * room except the kitchen, which has a real RH sensor. We treat anything
 * deeply negative as "absent" to also catch 0x8000 if it appears. */
static bool ecs_is_absent(int16_t v) { return v <= -30000; }

/* ecsRcSwitch (0x83 fn=0x10) mode codes -- observed from app interaction,
 * NOT from the SCBI doc (which only references roomController.h).
 *
 * raw  binary  app mode
 *   2  0010    regular
 *   8  1000    comfort
 *  10  1010    eco
 *  15  1111    off       (frost protection)
 *
 * All four user-facing app modes captured. Encoding doesn't reduce to a
 * clean bitmask so we keep it as an opaque enum. */
static const char *rc_switch_mode_name(int v) {
    switch (v) {
    case 2:
        return "regular";
    case 8:
        return "comfort";
    case 10:
        return "eco";
    case 15:
        return "off";
    default:
        return NULL;
    }
}

/* Unit conversion for ecs* remote-sensor functions. The raw on-wire
 * value is little-endian int16; scaling depends on function:
 *
 *   degC  -- decicelsius: raw / 10.0  (confirmed for ecsRcTflow=300=30.0
 *            and ecsRcTset matched against app: 100=10.0, 210=21.0)
 *   %rh   -- decipercent: raw / 10.0  (confirmed against kitchen RH)
 *   code  -- opaque enum / bitmask, no scaling (ecsRcSwitch raw=0x0F
 *            looks like a 4-bit mask of available modes)
 *   raw   -- unknown scaling, passed through unchanged
 */
static void ecs_unit(uint8_t fn, double raw, double *out_value, const char **out_unit) {
    switch (fn) {
    case 0x00:
    case 0x01:
    case 0x02:
    case 0x03: /* Outdoor/Flow/Cold/Solar  */
    case 0x07:
    case 0x08:
    case 0x09:
    case 0x0A: /* Storage / Top/Center/Bot */
    case 0x0C:
    case 0x0E:
    case 0x16: /* Tset / Troom / Tflow     */
        *out_value = raw / 10.0;
        *out_unit  = "degC";
        return;
    case 0x0D: /* Humidity                 */
        *out_value = raw / 10.0;
        *out_unit  = "%rh";
        return;
    case 0x0F: /* Wheel: signed int8 -5..+5 */
        *out_value = raw;
        *out_unit  = "step";
        return;
    case 0x10: /* Switch (mode enum, see   */
               /*  rc_switch_mode_name)    */
        *out_value = raw;
        *out_unit  = "mode";
        return;
    default:
        *out_value = raw;
        *out_unit  = "raw";
        return;
    }
}

/* ------------------------------------------------------ stdout pretty-print */

static void print_prefix(const frame_t *f) {
    char hex[24];
    hex[0]     = '\0';
    size_t pos = 0;
    for (int i = 0; i < f->dlc && pos + 3 < sizeof(hex); i++)
        pos += snprintf(hex + pos, sizeof(hex) - pos, "%02X", f->data[i]);
    for (int i = 0; i < 8 - f->dlc && pos + 3 < sizeof(hex); i++)
        pos += snprintf(hex + pos, sizeof(hex) - pos, "  ");
    DBGP("%ld,%x,%d,%s", (long)f->ts, f->can_id, f->dlc, hex);
    DBGP(" ... ProgramType=0x%02X SubscriberID=0x%02X FunctionType=0x%02X"
         " ProtocolType=%d MessageType=%d",
         f->program_type, f->subscriber_id, f->function_type, f->protocol_type, f->message_type);
}

static void print_payload_raw(const frame_t *f, const char *label) {
    DBGP(" ... %s: ", label);
    for (int i = 0; i < f->dlc; i++)
        DBGP("0x%02X ", f->data[i]);
    DBGP("\n");
}

/* --------------------------------------------------------- JSON assemblers */

static void json_common(cJSON *j, const frame_t *f) {
    cJSON_AddNumberToObject(j, "ts", (double)f->ts);
    char idbuf[16];
    snprintf(idbuf, sizeof(idbuf), "0x%08X", f->can_eff_id);
    cJSON_AddStringToObject(j, "can_id", idbuf);
    cJSON_AddNumberToObject(j, "program_type", f->program_type);
    cJSON_AddStringToObject(j, "program", prog_name(f->program_type));
    cJSON_AddNumberToObject(j, "subscriber_id", f->subscriber_id);
    const char *sn = cfg_lookup_u8(g_cfg.subscribers, f->subscriber_id);
    if (sn)
        cJSON_AddStringToObject(j, "subscriber_name", sn);
    cJSON_AddNumberToObject(j, "function_type", f->function_type);
    cJSON_AddNumberToObject(j, "message_type", f->message_type);
    cJSON_AddStringToObject(j, "msg_type", msg_type_str(f->message_type));
}

/* ------------------------------------------------- per-program decoders */

static cJSON *decode_controller(const frame_t *f, const char **subtopic, bool *retain) {
    const char *fn = controller_fn(f->function_type);
    DBGP(" ... CONTROLLER (0x0B): Function=0x%02X (%s),"
         " MessageType=0x%X (%s), SubscriberID=0x%02X",
         f->function_type, fn, f->message_type, msg_type_str(f->message_type), f->subscriber_id);

    cJSON *e = NULL;
    if (f->function_type == 0x14 && f->dlc >= 4) {
        /* DEVICE_INFO: OEM_ID, variant?, firmware version (LE uint16),
         * then padding. Observed in the wild for the CALEONbox at
         *   payload = C1 00 88 81 00 00 00 00
         * which decodes as OEM=0xC1 (CALEONbox), variant=0x00,
         * firmware = 0x8188 = 33160. */
        uint8_t oem = f->data[0];
        uint8_t var = f->data[1];
        uint16_t fw = (uint16_t)(f->data[2] | (f->data[3] << 8));
        DBGP(" ... DEVICE_INFO: OEM=0x%02X, Variant=0x%02X,"
             " Firmware=%u (0x%04X)\n",
             oem, var, fw, fw);
        e = cJSON_CreateObject();
        cJSON_AddStringToObject(e, "type", "device_info");
        cJSON_AddStringToObject(e, "function", fn);
        json_common(e, f);
        cJSON_AddNumberToObject(e, "oem_id", oem);
        cJSON_AddNumberToObject(e, "device_variant", var);
        cJSON_AddNumberToObject(e, "firmware", fw);
        *subtopic = "device";
        *retain   = true;
    } else if ((f->function_type == 0x01 || f->function_type == 0x08) && f->dlc >= 4) {
        uint8_t dev_sub = f->data[0], dev_id = f->data[1];
        uint8_t oem = f->data[2], var = f->data[3];
        DBGP(" ... Payload: SubscriberID=0x%02X, DeviceID=0x%02X,"
             " OEM_ID=0x%02X, DeviceVariant=0x%02X\n",
             dev_sub, dev_id, oem, var);

        e = cJSON_CreateObject();
        cJSON_AddStringToObject(e, "type", f->function_type == 0x01 ? "heartbeat" : "reset");
        cJSON_AddStringToObject(e, "function", fn);
        json_common(e, f);
        cJSON_AddNumberToObject(e, "device_subscriber_id", dev_sub);
        cJSON_AddNumberToObject(e, "device_id", dev_id);
        cJSON_AddNumberToObject(e, "oem_id", oem);
        cJSON_AddNumberToObject(e, "device_variant", var);
        *subtopic = (f->function_type == 0x01) ? "heartbeat" : "reset";
        *retain   = false;
    } else if (f->dlc > 0) {
        print_payload_raw(f, "Payload");
    } else {
        DBGP("\n");
    }
    return e;
}

static cJSON *decode_paramsync(const frame_t *f, const char **subtopic, bool *retain) {
    (void)subtopic;
    (void)retain;
    const char *fn = paramsync_fn(f->function_type);
    DBGP(" ... PARAMETERSYNCCONFIG (0x90): Function=0x%02X (%s),"
         " MessageType=0x%X (%s), SubscriberID=0x%02X",
         f->function_type, fn, f->message_type, msg_type_str(f->message_type), f->subscriber_id);
    if (f->function_type == 0x00 && f->dlc >= 2) {
        DBGP(" ... Payload: Source=0x%02X, Destination=0x%02X\n", f->data[0], f->data[1]);
    } else if (f->function_type == 0x01 && f->dlc >= 6) {
        DBGP(" ... Payload: Source=0x%02X, Destination=0x%02X,"
             " Family=0x%02X, Synchronize=0x%02X, Disconnect=0x%02X,"
             " SelfTestDone=0x%02X\n",
             f->data[0], f->data[1], f->data[2], f->data[3], f->data[4], f->data[5]);
    } else if (f->dlc > 0) {
        print_payload_raw(f, "Payload");
    } else {
        DBGP("\n");
    }
    return NULL;
}

static cJSON *decode_remotesensor(const frame_t *f, const char **subtopic, bool *retain) {
    const char *fn = remotesensor_fn(f->function_type);
    DBGP(" ... REMOTESENSOR (0x%02X): Function=0x%02X (%s),"
         " MessageType=0x%X (%s), SubscriberID=0x%02X",
         f->program_type, f->function_type, fn, f->message_type, msg_type_str(f->message_type), f->subscriber_id);

    if (f->message_type == 0) {
        DBGP(" ... Request: No payload expected\n");
        return NULL;
    }
    if (f->message_type != 2 || (f->dlc != 4 && f->dlc != 6)) {
        if (f->dlc > 0)
            print_payload_raw(f, "Payload (unknown format)");
        else
            DBGP("\n");
        return NULL;
    }

    int16_t value = (int16_t)((f->data[1] << 8) | f->data[0]);
    uint8_t vc    = f->data[2];
    uint8_t ct    = f->data[3];
    uint8_t ridx  = (f->dlc == 6) ? f->data[4] : 0;
    uint8_t rtyp  = (f->dlc == 6) ? f->data[5] : 0;

    if (f->dlc == 6)
        DBGP(" ... CALEON Response: Value=0x%04X (%d),"
             " VirtualCircuit=0x%02X, ControllerType=0x%02X,"
             " RoomIndex=0x%02X, RoomType=0x%02X\n",
             (uint16_t)value, value, vc, ct, ridx, rtyp);
    else
        DBGP(" ... Response: Value=0x%04X (%d),"
             " VirtualCircuit=0x%02X, ControllerType=0x%02X\n",
             (uint16_t)value, value, vc, ct);

    cJSON *e = cJSON_CreateObject();
    cJSON_AddStringToObject(e, "type", "sensor");
    cJSON_AddStringToObject(e, "function", fn);
    json_common(e, f);
    cJSON_AddNumberToObject(e, "virtual_circuit", vc);
    cJSON_AddNumberToObject(e, "controller_type", ct);
    cJSON_AddNumberToObject(e, "value_raw", value);
    if (ecs_is_absent(value)) {
        cJSON_AddNullToObject(e, "value");
        cJSON_AddBoolToObject(e, "present", false);
    } else {
        double v;
        const char *unit;
        ecs_unit(f->function_type, (double)value, &v, &unit);
        cJSON_AddNumberToObject(e, "value", v);
        cJSON_AddStringToObject(e, "unit", unit);
        cJSON_AddBoolToObject(e, "present", true);
        /* For ecsRcSwitch (0x10) attach a friendly mode name when known */
        if (f->function_type == 0x10) {
            const char *mn = rc_switch_mode_name(value);
            if (mn)
                cJSON_AddStringToObject(e, "mode_name", mn);
        }
    }
    if (f->dlc == 6) {
        cJSON_AddNumberToObject(e, "room_index", ridx);
        cJSON_AddNumberToObject(e, "room_type", rtyp);
        const char *rn = cfg_room_name(ridx, rtyp);
        if (rn)
            cJSON_AddStringToObject(e, "room_name", rn);
    }
    *subtopic = "sensor";
    *retain   = g_cfg.mqtt_retain_sensors;
    return e;
}

static cJSON *decode_namedsensor(const frame_t *f, const char **subtopic, bool *retain) {
    (void)retain;
    DBGP(" ... NAMEDSENSOR (0x%02X): Function=0x%02X,"
         " MessageType=0x%X (%s), SubscriberID=0x%02X",
         f->program_type, f->function_type, f->message_type, msg_type_str(f->message_type), f->subscriber_id);
    if (f->message_type == 0) {
        DBGP(" ... Request: No payload expected\n");
        return NULL;
    }
    if (f->message_type != 2 || f->dlc < 2) {
        if (f->dlc > 0)
            print_payload_raw(f, "Payload");
        else
            DBGP("\n");
        return NULL;
    }
    int16_t value = (int16_t)((f->data[1] << 8) | f->data[0]);
    DBGP(" ... Response: Value=0x%04X (%d)\n", (uint16_t)value, value);
    cJSON *e = cJSON_CreateObject();
    cJSON_AddStringToObject(e, "type", "named_sensor");
    json_common(e, f);
    cJSON_AddNumberToObject(e, "value_raw", value);
    if (ecs_is_absent(value)) {
        cJSON_AddNullToObject(e, "value");
        cJSON_AddBoolToObject(e, "present", false);
    } else {
        /* NAMEDSENSOR scaling not yet mapped per-function; emit raw and
         * leave it to consumers / a future patch once we know what these
         * sensors actually are. */
        cJSON_AddNumberToObject(e, "value", value);
        cJSON_AddStringToObject(e, "unit", "raw");
        cJSON_AddBoolToObject(e, "present", true);
    }
    *subtopic = "sensor";
    return e;
}

/*
 * HEATINGCONTROL (0x8B) -- still being reverse-engineered.
 *
 * Function 0x00 General_Status: 8 bytes of status flags / mode bytes from
 *   the central controller. Bytes seen in our capture e.g.
 *     "000419500180FFFF"  "0002020A0E010000"  "0105020A1B010000"
 *   The trailing 0xFFFF and the leading sequence byte suggest a packed
 *   status block; we publish it as opaque hex for now so downstream tools
 *   can hash on it and we can refine the bit layout later.
 *
 * Function 0x01 DLF_SENSOR: per-sensor sample.
 *     data[0]   = sensor index
 *     data[1-2] = little-endian int16 value (decicelsius / ohms / raw)
 *     data[3]   = sensor_type (PT1000, KTY, NTC, "absent" = 0xFF, ...)
 *     data[7]   = flag byte (only present on dlc==8)
 *
 * Function 0x02 DLF_RELAY: per-relay state.
 *     data[0]   = relay index
 *     data[1]   = mode (00=off, 01=on, 02=auto, ...; exact mapping TBC)
 *     data[2-3] = little-endian int16 value (PWM 0..100, or runtime,
 *                 depending on relay type)
 *     data[4..] = extra (timer / cumulative runtime), still unknown.
 *
 * Function 0x08 DLG_HYDRAULIK_CONFIG: hydraulic-scheme descriptor; opaque.
 */
static cJSON *decode_heatingcontrol(const frame_t *f, const char **subtopic, bool *retain) {
    (void)retain;
    const char *fn = heatingcontrol_fn(f->function_type);
    DBGP(" ... HEATINGCONTROL (0x8B): Function=0x%02X (%s),"
         " MessageType=0x%X (%s), SubscriberID=0x%02X",
         f->function_type, fn, f->message_type, msg_type_str(f->message_type), f->subscriber_id);

    cJSON *e = NULL;
    if (f->function_type == 0x01 && f->dlc >= 3) {
        uint8_t sno   = f->data[0];
        int16_t value = (int16_t)((f->data[2] << 8) | f->data[1]);
        uint8_t styp  = (f->dlc >= 4) ? f->data[3] : 0;
        DBGP(" ... Sensor: Number=0x%02X, Value=0x%04X (%d),"
             " Type=0x%02X",
             sno, (uint16_t)value, value, styp);
        if (f->dlc >= 8)
            DBGP(", Flags=0x%02X", f->data[7]);
        DBGP("\n");

        e = cJSON_CreateObject();
        cJSON_AddStringToObject(e, "type", "hc_sensor");
        cJSON_AddStringToObject(e, "function", fn);
        json_common(e, f);
        cJSON_AddNumberToObject(e, "sensor_no", sno);
        cJSON_AddNumberToObject(e, "sensor_type", styp);
        cJSON_AddStringToObject(e, "sensor_type_name", hc_sensor_type_name(styp));
        cJSON_AddNumberToObject(e, "value_raw", value);
        /* 0x8B DLF_SENSOR appears to carry SLOT METADATA, not live
         * readings: every configured slot reports value_raw=0x0100=256
         * regardless of sensor_type, so 25.6degC/25.6% would be fake.
         * Live readings arrive on the 0x83 REMOTESENSOR channel. Mark
         * the well-known sentinels as not-present; only emit a scaled
         * value if we ever see something other than 256/6400/2304. */
        bool slot_sentinel = (value == 256 || value == 6400 || value == 2304);
        if (styp == 0 || slot_sentinel) {
            cJSON_AddNullToObject(e, "value");
            cJSON_AddBoolToObject(e, "present", false);
        } else {
            double v         = value;
            const char *unit = "raw";
            switch (styp) {
            case 4:
                v    = value / 10.0;
                unit = "degC";
                break;
            case 5:
                v    = value / 10.0;
                unit = "%rh";
                break;
            case 1:
                unit = "flow_raw";
                break; /* VFS, sub-type gives l/min range */
            case 2:
                unit = "pressure_raw";
                break; /* RPS, sub-type gives bar range  */
            case 3:
                unit = "pressure_raw";
                break; /* DPS                            */
            case 6:
            case 7:
                unit = "code";
                break; /* RC wheel / switch              */
            }
            cJSON_AddNumberToObject(e, "value", v);
            cJSON_AddStringToObject(e, "unit", unit);
            cJSON_AddBoolToObject(e, "present", true);
        }
        if (f->dlc >= 8)
            cJSON_AddNumberToObject(e, "flags", f->data[7]);
        const char *n = cfg_lookup_u8(g_cfg.hc_sensors, sno);
        if (n)
            cJSON_AddStringToObject(e, "name", n);
        *subtopic = "hc/sensor";
    } else if (f->function_type == 0x02 && f->dlc >= 4) {
        /* On the 0x8B channel the DLF_RELAY payload does NOT match the
         * SCBI rev-11 layout (which says byte1=mode, byte2=value%, ...).
         * Captured idle bytes for our box are:
         *   relay 0..10 : NN 08 <L> 00 00 00 00 00   <L>='A'..'K'
         *   relay 11..13: NN 08 4E <D> 00 00 00 00   "N1"/"N4"/"N8"
         * so bytes 2-3 are the ASCII terminal label printed on the box,
         * not a PWM duty cycle. byte 1 is always 0x08 (a status/format
         * flag, exact meaning unknown). The actual on/off state must
         * live in one of bytes 4-7 which are all zero while relays are
         * off -- we'll see which when a relay fires. */
        uint8_t rno   = f->data[0];
        uint8_t flag  = f->data[1];
        char label[3] = { (char)f->data[2], (f->dlc >= 4 && f->data[3]) ? (char)f->data[3] : '\0', '\0' };
        DBGP(" ... Relay: Number=0x%02X, Flag=0x%02X,"
             " Label=\"%s\"",
             rno, flag, label);
        if (f->dlc > 4) {
            DBGP(", Extra=");
            for (int i = 4; i < f->dlc; i++)
                DBGP("%02X", f->data[i]);
        }
        DBGP("\n");

        e = cJSON_CreateObject();
        cJSON_AddStringToObject(e, "type", "hc_relay");
        cJSON_AddStringToObject(e, "function", fn);
        json_common(e, f);
        cJSON_AddNumberToObject(e, "relay_no", rno);
        cJSON_AddNumberToObject(e, "flag", flag);
        cJSON_AddStringToObject(e, "label", label);
        /* Preserve all "extra" bytes so we can spot the on/off byte
         * the first time a relay actually switches under load. */
        cJSON *extra = cJSON_CreateArray();
        for (int i = 4; i < f->dlc; i++)
            cJSON_AddItemToArray(extra, cJSON_CreateNumber(f->data[i]));
        cJSON_AddItemToObject(e, "extra", extra);
        const char *n = cfg_lookup_u8(g_cfg.hc_relays, rno);
        if (n)
            cJSON_AddStringToObject(e, "name", n);
        *subtopic = "hc/relay";
    } else if (f->function_type == 0x00) {
        print_payload_raw(f, "Status");
    } else if (f->function_type == 0x08) {
        print_payload_raw(f, "Config");
    } else if (f->dlc > 0) {
        print_payload_raw(f, "Payload");
    } else {
        DBGP("\n");
    }
    return e;
}

/* ----------------------------------------------------- top-level dispatch */

static cJSON *decode(const frame_t *f, const char **subtopic, bool *retain) {
    *subtopic = NULL;
    *retain   = false;
    print_prefix(f);
    switch (f->program_type) {
    case 0x0B:
        return decode_controller(f, subtopic, retain);
    case 0x83:
        return decode_remotesensor(f, subtopic, retain);
    case 0x84:
        return decode_namedsensor(f, subtopic, retain);
    case 0x8B:
        return decode_heatingcontrol(f, subtopic, retain);
    case 0x90:
        return decode_paramsync(f, subtopic, retain);
    default:
        DBGP("\n");
        return NULL;
    }
}

/* ---------------------------------------------------------------- runtime */

static void usage(const char *p) {
    fprintf(stderr,
            "Usage: %s [-c config.cfg] [-i iface] [-d|--debug]\n"
            "  -c FILE        config file (default: caleon2mqtt.cfg)\n"
            "  -i IFACE       CAN interface override (default from config: %s)\n"
            "  -d, --debug    enable verbose per-frame decoder trace on stdout\n",
            p, g_cfg.can_iface[0] ? g_cfg.can_iface : "vcan0");
}

int main(int argc, char **argv) {
    const char *cfg_path             = "caleon2mqtt.cfg";
    const char *iface_arg            = NULL;
    static struct option long_opts[] = {
        { "config", required_argument, 0, 'c' }, { "iface", required_argument, 0, 'i' }, { "debug", no_argument, 0, 'd' }, { "help", no_argument, 0, 'h' }, { 0, 0, 0, 0 },
    };
    int opt;
    while ((opt = getopt_long(argc, argv, "c:i:dh", long_opts, NULL)) != -1) {
        switch (opt) {
        case 'c':
            cfg_path = optarg;
            break;
        case 'i':
            iface_arg = optarg;
            break;
        case 'd':
            g_debug = true;
            break;
        case 'h':
        default:
            usage(argv[0]);
            return opt == 'h' ? 0 : 1;
        }
    }
    if (cfg_load(cfg_path) < 0)
        return 1;
    if (iface_arg)
        strncpy(g_cfg.can_iface, iface_arg, IFNAMSIZ - 1);

    signal(SIGINT, on_signal);
    signal(SIGTERM, on_signal);
    signal(SIGPIPE, SIG_IGN);

    int s = can_open(g_cfg.can_iface);
    if (s < 0)
        return 1;
    fprintf(stderr, "can: listening on %s\n", g_cfg.can_iface);

    /* MQTT is best-effort: if the broker is down we still want the debug
     * stdout log so reverse-engineering can continue. */
    if (mqtt_init() < 0)
        fprintf(stderr, "mqtt: continuing without publish\n");

    while (g_run) {
        fd_set rfds;
        FD_ZERO(&rfds);
        FD_SET(s, &rfds);
        struct timeval tv = { 1, 0 };
        int r             = select(s + 1, &rfds, NULL, NULL, &tv);
        if (r < 0) {
            if (errno == EINTR)
                continue;
            perror("select");
            break;
        }
        if (r == 0 || !FD_ISSET(s, &rfds))
            continue;

        struct can_frame raw;
        ssize_t n = read(s, &raw, sizeof(raw));
        if (n != (ssize_t)sizeof(raw))
            continue;

        frame_t f;
        memset(&f, 0, sizeof(f));
        f.can_id     = raw.can_id;
        f.can_eff_id = raw.can_id & CAN_EFF_MASK;
        f.dlc        = raw.can_dlc;
        memcpy(f.data, raw.data, 8);
        time(&f.ts);
        f.program_type  = f.can_eff_id & 0xFF;
        f.subscriber_id = (f.can_eff_id >> 8) & 0xFF;
        f.function_type = (f.can_eff_id >> 16) & 0xFF;
        f.protocol_type = (f.can_eff_id >> 24) & 0x07;
        f.message_type  = (f.can_eff_id >> 27) & 0x03;

        const char *subtopic = NULL;
        bool retain          = false;
        cJSON *evt           = decode(&f, &subtopic, &retain);
        fflush(stdout);
        if (evt) {
            mqtt_publish(subtopic, evt, retain);
            cJSON_Delete(evt);
        }
    }

    fprintf(stderr, "shutting down\n");
    mqtt_shutdown();
    close(s);
    if (g_cfg.root)
        cJSON_Delete(g_cfg.root);
    return 0;
}
