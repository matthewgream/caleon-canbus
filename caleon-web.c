/*
 * caleon-web -- minimal HTTP UI in front of the caleon2mqtt MQTT stream.
 */
#include <arpa/inet.h>
#include <cjson/cJSON.h>
#include <errno.h>
#include <fcntl.h>
#include <getopt.h>
#include <mosquitto.h>
#include <netinet/in.h>
#include <poll.h>
#include <pthread.h>
#include <signal.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <time.h>
#include <unistd.h>

#define DEFAULT_HTTP_PORT 3002
#define MAX_ROOMS 32
#define MAX_DEVICES 16
#define MAX_CLIENTS 32
#define LISTEN_BACKLOG 16
#define HEARTBEAT_SECS 15

/* ------------------------------------------------------------------- state */

typedef struct {
    int idx, type;
    char name[64]; /* room_name from MQTT, or "" if unmapped     */
    double temp, tset, humidity;
    int swval;
    char mode[16];
    time_t t_temp, t_tset, t_hum, t_sw;
    bool seen_temp, seen_tset, seen_hum, seen_sw;
    bool present_temp, present_tset, present_hum, present_sw;
} room_t;

static room_t g_rooms[MAX_ROOMS];
static int g_nrooms = 0;

typedef struct {
    int subscriber_id;
    char subscriber_name[64]; /* "" if unmapped */
    int oem_id;
    int device_variant;
    int firmware;
    time_t ts;
} device_t;

static device_t g_devices[MAX_DEVICES];
static int g_ndevices = 0;

/* Generic events feed. Up to MAX_EVENTS newest-first, cached server-side so
 * the SSE snapshot can populate the UI on every reconnect/refresh. Today
 * the only producer is the SYSTEM_DATETIME decoder; more kinds slot in. */
#define MAX_EVENTS 5
typedef struct {
    char kind[16];    /* e.g. "datetime" */
    char summary[96]; /* human-readable one-liner */
    time_t local_ts;  /* server clock when received */
    bool used;
} event_t;
static event_t g_events[MAX_EVENTS];

static pthread_mutex_t g_state_mu = PTHREAD_MUTEX_INITIALIZER;

static int g_clients[MAX_CLIENTS];
static int g_nclients               = 0;
static pthread_mutex_t g_clients_mu = PTHREAD_MUTEX_INITIALIZER;

static volatile sig_atomic_t g_stop = 0;
static void on_sig(__attribute__((unused)) int s) { g_stop = 1; }

static bool g_debug = false;
#define DBG(...)                                                                                                                                                                   \
    do {                                                                                                                                                                           \
        if (g_debug) {                                                                                                                                                             \
            fprintf(stderr, "[debug] ");                                                                                                                                           \
            fprintf(stderr, __VA_ARGS__);                                                                                                                                          \
            fputc('\n', stderr);                                                                                                                                                   \
        }                                                                                                                                                                          \
    } while (0)

/* ---------------------------------------------------------------- helpers */

static room_t *find_or_add_room(int idx, int type) {
    for (int i = 0; i < g_nrooms; i++)
        if (g_rooms[i].idx == idx && g_rooms[i].type == type)
            return &g_rooms[i];
    if (g_nrooms >= MAX_ROOMS)
        return NULL;
    room_t *r = &g_rooms[g_nrooms++];
    memset(r, 0, sizeof(*r));
    r->idx  = idx;
    r->type = type;
    return r;
}

static device_t *find_or_add_device(int subscriber_id) {
    for (int i = 0; i < g_ndevices; i++)
        if (g_devices[i].subscriber_id == subscriber_id)
            return &g_devices[i];
    if (g_ndevices >= MAX_DEVICES)
        return NULL;
    device_t *d = &g_devices[g_ndevices++];
    memset(d, 0, sizeof(*d));
    d->subscriber_id = subscriber_id;
    return d;
}

static cJSON *event_to_json(const event_t *e, time_t now) {
    cJSON *o = cJSON_CreateObject();
    cJSON_AddStringToObject(o, "kind", e->kind);
    cJSON_AddStringToObject(o, "summary", e->summary);
    cJSON_AddNumberToObject(o, "age", (double)(now - e->local_ts));
    return o;
}

/* Push to the events ring (newest at index 0). Caller holds g_state_mu.
 * Drops a duplicate-of-last to avoid filling the ring on retained-msg
 * replays. Returns true if accepted, false if it was a dup. */
static bool events_push_locked(const char *kind, const char *summary) {
    if (g_events[0].used && strcmp(g_events[0].kind, kind) == 0 && strcmp(g_events[0].summary, summary) == 0)
        return false;
    for (int i = MAX_EVENTS - 1; i > 0; i--)
        g_events[i] = g_events[i - 1];
    snprintf(g_events[0].kind, sizeof(g_events[0].kind), "%s", kind);
    snprintf(g_events[0].summary, sizeof(g_events[0].summary), "%s", summary);
    g_events[0].local_ts = time(NULL);
    g_events[0].used     = true;
    return true;
}

static cJSON *device_to_json(const device_t *d, time_t now) {
    cJSON *o = cJSON_CreateObject();
    cJSON_AddNumberToObject(o, "subscriber_id", d->subscriber_id);
    if (d->subscriber_name[0])
        cJSON_AddStringToObject(o, "subscriber_name", d->subscriber_name);
    cJSON_AddNumberToObject(o, "oem_id", d->oem_id);
    cJSON_AddNumberToObject(o, "device_variant", d->device_variant);
    cJSON_AddNumberToObject(o, "firmware", d->firmware);
    cJSON_AddNumberToObject(o, "age", (double)(now - d->ts));
    return o;
}

/* Build a JSON object for one room. Fields that have never been observed
 * are omitted; fields seen and present carry the value; fields seen but
 * currently absent send JSON null. ages are seconds since last update. */
static cJSON *room_to_json(const room_t *r, time_t now) {
    cJSON *o = cJSON_CreateObject();
    cJSON_AddNumberToObject(o, "idx", r->idx);
    cJSON_AddNumberToObject(o, "type", r->type);
    if (r->name[0])
        cJSON_AddStringToObject(o, "name", r->name);
    if (r->seen_temp) {
        if (r->present_temp)
            cJSON_AddNumberToObject(o, "temp", r->temp);
        else
            cJSON_AddNullToObject(o, "temp");
        cJSON_AddNumberToObject(o, "age_temp", (double)(now - r->t_temp));
    }
    if (r->seen_tset) {
        if (r->present_tset)
            cJSON_AddNumberToObject(o, "tset", r->tset);
        else
            cJSON_AddNullToObject(o, "tset");
        cJSON_AddNumberToObject(o, "age_tset", (double)(now - r->t_tset));
    }
    if (r->seen_hum) {
        if (r->present_hum)
            cJSON_AddNumberToObject(o, "humidity", r->humidity);
        else
            cJSON_AddNullToObject(o, "humidity");
        cJSON_AddNumberToObject(o, "age_hum", (double)(now - r->t_hum));
    }
    if (r->seen_sw) {
        if (r->present_sw) {
            cJSON_AddStringToObject(o, "mode", r->mode);
            cJSON_AddNumberToObject(o, "swval", r->swval);
        } else
            cJSON_AddNullToObject(o, "mode");
        cJSON_AddNumberToObject(o, "age_sw", (double)(now - r->t_sw));
    }
    return o;
}

/* Write the full buffer; -1 means caller should drop the client. */
static int sse_write(int fd, const char *buf, size_t len) {
    while (len) {
        const ssize_t n = send(fd, buf, len, MSG_NOSIGNAL);
        if (n < 0) {
            if (errno != EINTR)
                return -1; /* EAGAIN on a slow client also drops -- intentional */
        } else {
            buf += n;
            len -= n;
        }
    }
    return 0;
}
static void broadcast_event(const char *event, cJSON *json) {
    char *jstr = cJSON_PrintUnformatted(json);
    char head[64];
    const int hlen    = snprintf(head, sizeof(head), "event: %s\ndata: ", event);
    const size_t jlen = strlen(jstr);
    pthread_mutex_lock(&g_clients_mu);
    int drop = 0, sent = 0;
    int i = 0;
    while (i < g_nclients) {
        if (sse_write(g_clients[i], head, (size_t)hlen) < 0 || sse_write(g_clients[i], jstr, jlen) < 0 || sse_write(g_clients[i], "\n\n", 2) < 0) {
            close(g_clients[i]);
            g_clients[i] = g_clients[--g_nclients];
            drop++;
        } else {
            i++;
            sent++;
        }
    }
    const int total = g_nclients;
    pthread_mutex_unlock(&g_clients_mu);
    DBG("sse broadcast event=%s bytes=%zu sent=%d drop=%d clients=%d", event, jlen, sent, drop, total);
    free(jstr);
    cJSON_Delete(json);
}

/* -------------------------------------------------------------- mqtt side */

static void on_mqtt_connect(struct mosquitto *m, __attribute__((unused)) void *u, int rc) {
    if (rc != 0) {
        fprintf(stderr, "mqtt: connect failed: %s\n", mosquitto_connack_string(rc));
        return;
    }
    fprintf(stderr, "mqtt: connected, subscribing caleon/sensor, caleon/device and caleon/datetime\n");
    int mid = 0;
    int sub = mosquitto_subscribe(m, &mid, "caleon/sensor", 0);
    DBG("mqtt subscribe rc=%d mid=%d topic=caleon/sensor", sub, mid);
    sub = mosquitto_subscribe(m, &mid, "caleon/device", 0);
    DBG("mqtt subscribe rc=%d mid=%d topic=caleon/device", sub, mid);
    sub = mosquitto_subscribe(m, &mid, "caleon/datetime", 0);
    DBG("mqtt subscribe rc=%d mid=%d topic=caleon/datetime", sub, mid);
}

static void on_mqtt_disconnect(__attribute__((unused)) struct mosquitto *m, __attribute__((unused)) void *u, int rc) {
    fprintf(stderr, "mqtt: disconnected rc=%d (%s)\n", rc, rc ? "unexpected -- async reconnect will retry" : "clean");
}

static void on_mqtt_subscribe(__attribute__((unused)) struct mosquitto *m, __attribute__((unused)) void *u, int mid, int qos_count, const int *granted_qos) {
    DBG("mqtt SUBACK mid=%d qos_count=%d granted_qos[0]=%d", mid, qos_count, qos_count > 0 ? granted_qos[0] : -1);
}

static void handle_device_msg(cJSON *p) {
    cJSON *jsub  = cJSON_GetObjectItem(p, "subscriber_id");
    cJSON *jname = cJSON_GetObjectItem(p, "subscriber_name");
    cJSON *joem  = cJSON_GetObjectItem(p, "oem_id");
    cJSON *jvar  = cJSON_GetObjectItem(p, "device_variant");
    cJSON *jfw   = cJSON_GetObjectItem(p, "firmware");
    if (!cJSON_IsNumber(jsub) || !cJSON_IsNumber(jfw)) {
        DBG("  -> skip device: missing subscriber_id or firmware");
        return;
    }
    const int sub = (int)jsub->valuedouble;
    const int fw  = (int)jfw->valuedouble;
    /* DEVICE_INFO request packets carry firmware=0 -- ignore those,
     * we only want the actual response with the real version. */
    if (fw == 0) {
        DBG("  -> skip device: firmware=0 (request, not response)");
        return;
    }
    device_t snap;
    pthread_mutex_lock(&g_state_mu);
    device_t *d = find_or_add_device(sub);
    if (!d) {
        pthread_mutex_unlock(&g_state_mu);
        return;
    }
    d->oem_id         = cJSON_IsNumber(joem) ? (int)joem->valuedouble : 0;
    d->device_variant = cJSON_IsNumber(jvar) ? (int)jvar->valuedouble : 0;
    d->firmware       = fw;
    if (cJSON_IsString(jname))
        snprintf(d->subscriber_name, sizeof(d->subscriber_name), "%s", jname->valuestring);
    d->ts = time(NULL);
    snap  = *d;
    pthread_mutex_unlock(&g_state_mu);
    DBG("  -> device sub=0x%02X name=%s oem=0x%02X fw=%u", sub, snap.subscriber_name[0] ? snap.subscriber_name : "?", snap.oem_id, snap.firmware);
    broadcast_event("device", device_to_json(&snap, time(NULL)));
}

static void handle_datetime_msg(cJSON *p) {
    cJSON *jiso = cJSON_GetObjectItem(p, "iso8601");
    cJSON *jy = cJSON_GetObjectItem(p, "year"), *jm = cJSON_GetObjectItem(p, "month"), *jd = cJSON_GetObjectItem(p, "day");
    cJSON *jh = cJSON_GetObjectItem(p, "hour"), *jmi = cJSON_GetObjectItem(p, "minute"), *js = cJSON_GetObjectItem(p, "second");
    cJSON *jdow = cJSON_GetObjectItem(p, "dow");
    if (!cJSON_IsString(jiso) || !cJSON_IsNumber(jy) || !cJSON_IsNumber(jm) || !cJSON_IsNumber(jd) || !cJSON_IsNumber(jh) || !cJSON_IsNumber(jmi) || !cJSON_IsNumber(js)) {
        DBG("  -> skip datetime: missing iso8601/y/mo/d/h/mi/s");
        return;
    }
    /* mktime() interprets struct tm as local time; the controller
     * also broadcasts local time, so this skew is reasonably exact
     * as long as both clocks are in the same zone. */
    struct tm tmv         = { .tm_year  = (int)jy->valuedouble - 1900,
                              .tm_mon   = (int)jm->valuedouble - 1,
                              .tm_mday  = (int)jd->valuedouble,
                              .tm_hour  = (int)jh->valuedouble,
                              .tm_min   = (int)jmi->valuedouble,
                              .tm_sec   = (int)js->valuedouble,
                              .tm_isdst = -1 };
    const time_t reported = mktime(&tmv);
    const long skew       = (reported != (time_t)-1) ? (long)(reported - time(NULL)) : 0;
    char summary[96];
    snprintf(summary, sizeof(summary), "%s  dow=%d  skew=%s%lds", jiso->valuestring, cJSON_IsNumber(jdow) ? (int)jdow->valuedouble : 0, skew > 0 ? "+" : "", skew);
    pthread_mutex_lock(&g_state_mu);
    const bool accepted = events_push_locked("datetime", summary);
    const event_t snap  = g_events[0];
    pthread_mutex_unlock(&g_state_mu);
    if (accepted) {
        DBG("  -> event datetime: %s", summary);
        broadcast_event("event", event_to_json(&snap, time(NULL)));
    } else
        DBG("  -> dup datetime, skipped: %s", summary);
}

static void handle_room_msg(cJSON *p) {
    cJSON *jfn = cJSON_GetObjectItem(p, "function");
    cJSON *jri = cJSON_GetObjectItem(p, "room_index");
    cJSON *jrt = cJSON_GetObjectItem(p, "room_type");
    if (!cJSON_IsString(jfn) || !cJSON_IsNumber(jri) || !cJSON_IsNumber(jrt)) {
        DBG("  -> skip: missing function/room_index/room_type (fn=%s ri=%s rt=%s)", cJSON_IsString(jfn) ? "ok" : "no", cJSON_IsNumber(jri) ? "ok" : "no",
            cJSON_IsNumber(jrt) ? "ok" : "no");
        return;
    }

    const int idx = (int)jri->valuedouble, type = (int)jrt->valuedouble;
    /* (RI=0, RT=0) is the central plant pseudo-room, not a real room. */
    if (idx == 0 && type == 0) {
        DBG("  -> skip: central plant (RI=0, RT=0) fn=%s", jfn->valuestring);
        return;
    }

    cJSON *jv    = cJSON_GetObjectItem(p, "value");
    cJSON *jvr   = cJSON_GetObjectItem(p, "value_raw");
    cJSON *jpres = cJSON_GetObjectItem(p, "present");
    cJSON *jname = cJSON_GetObjectItem(p, "room_name");
    cJSON *jmode = cJSON_GetObjectItem(p, "mode_name");

    const bool present = cJSON_IsNull(jv) ? false : !(cJSON_IsBool(jpres) && !jpres->valueint);
    bool change        = false;
    room_t snap;

    pthread_mutex_lock(&g_state_mu);
    room_t *r = find_or_add_room(idx, type);
    if (!r) {
        pthread_mutex_unlock(&g_state_mu);
        return;
    }
    if (cJSON_IsString(jname))
        snprintf(r->name, sizeof(r->name), "%s", jname->valuestring);
    const char *fn = jfn->valuestring;
    if (strcmp(fn, "ecsRcTroom") == 0) {
        r->seen_temp    = true;
        r->present_temp = present;
        if (present && cJSON_IsNumber(jv))
            r->temp = jv->valuedouble;
        r->t_temp = time(NULL);
        change    = true;
    } else if (strcmp(fn, "ecsRcTset") == 0) {
        r->seen_tset    = true;
        r->present_tset = present;
        if (present && cJSON_IsNumber(jv))
            r->tset = jv->valuedouble;
        r->t_tset = time(NULL);
        change    = true;
    } else if (strcmp(fn, "ecsRcHumidity") == 0) {
        r->seen_hum    = true;
        r->present_hum = present;
        if (present && cJSON_IsNumber(jv))
            r->humidity = jv->valuedouble;
        r->t_hum = time(NULL);
        change   = true;
    } else if (strcmp(fn, "ecsRcSwitch") == 0) {
        r->seen_sw    = true;
        r->present_sw = present;
        if (present && cJSON_IsNumber(jvr)) {
            r->swval = (int)jvr->valuedouble;
            snprintf(r->mode, sizeof(r->mode), "%s", cJSON_IsString(jmode) ? jmode->valuestring : "?");
        }
        r->t_sw = time(NULL);
        change  = true;
    }
    snap = *r;
    pthread_mutex_unlock(&g_state_mu);

    if (change) {
        DBG("  -> room idx=%d type=%d fn=%s present=%d name=%s", idx, type, fn, present, snap.name[0] ? snap.name : "?");
        broadcast_event("room", room_to_json(&snap, time(NULL)));
    } else
        DBG("  -> no match: fn=%s (not one of ecsRcTroom/Tset/Humidity/Switch)", fn);
}

static void on_mqtt_msg(__attribute__((unused)) struct mosquitto *m, __attribute__((unused)) void *u, const struct mosquitto_message *msg) {
    if (!msg->payload || msg->payloadlen <= 0) {
        DBG("mqtt msg topic=%s empty payload", msg->topic ? msg->topic : "?");
        return;
    }
    char *tmp = malloc((size_t)msg->payloadlen + 1);
    if (!tmp)
        return;
    memcpy(tmp, msg->payload, (size_t)msg->payloadlen);
    tmp[msg->payloadlen] = 0;
    const char *topic    = msg->topic ? msg->topic : "?";
    DBG("mqtt msg topic=%s len=%d payload=%.200s", topic, msg->payloadlen, tmp);
    cJSON *p = cJSON_Parse(tmp);
    free(tmp);
    if (!p) {
        DBG("  -> JSON parse failed");
        return;
    }

    if (strcmp(topic, "caleon/device") == 0)
        handle_device_msg(p);
    else if (strcmp(topic, "caleon/datetime") == 0)
        handle_datetime_msg(p);
    else if (strcmp(topic, "caleon/sensor") == 0)
        handle_room_msg(p);

    cJSON_Delete(p);
}

/* -------------------------------------------------------------- http side */

/* Favicon -- a red towel/heating rail (two vertical posts, five horizontal
 * rungs) inlined into the HTML <head> below as a data: URI so there's no
 * separate request. '#' must be %23-escaped in a data: URI; the SVG is
 * single-quoted so we can wrap the href in double quotes. */

static const char HTML_PAGE[] = "<!doctype html><html><head><meta charset=utf-8>"
                                "<title>caleon</title>"
                                "<link rel=\"icon\" type=\"image/svg+xml\" href=\"data:image/svg+xml;utf8,"
                                "<svg xmlns='http://www.w3.org/2000/svg' viewBox='0 0 64 64'>"
                                "<g stroke='%23d33' stroke-width='6' stroke-linecap='round' fill='none'>"
                                "<line x1='14' y1='6' x2='14' y2='58'/>"
                                "<line x1='50' y1='6' x2='50' y2='58'/>"
                                "<line x1='10' y1='14' x2='54' y2='14'/>"
                                "<line x1='10' y1='24' x2='54' y2='24'/>"
                                "<line x1='10' y1='34' x2='54' y2='34'/>"
                                "<line x1='10' y1='44' x2='54' y2='44'/>"
                                "<line x1='10' y1='54' x2='54' y2='54'/>"
                                "</g></svg>\">"
                                "<style>"
                                "body{font:14px/1.4 -apple-system,Helvetica,Arial,sans-serif;"
                                "background:#111;color:#ddd;margin:0;padding:1.2em}"
                                "h1{font-size:1.1em;color:#9cf;margin:0 0 .6em;font-weight:600}"
                                ".box{background:#1a1a1a;border:1px solid #333;border-radius:6px;"
                                "padding:.8em 1em;margin-bottom:1em;max-width:780px}"
                                ".box h2{font-size:.95em;color:#9cf;margin:0 0 .5em;font-weight:600}"
                                "table{width:100%;border-collapse:collapse;font-variant-numeric:tabular-nums}"
                                "th,td{text-align:left;padding:.25em .8em .25em 0;vertical-align:middle}"
                                "th{color:#888;font-weight:600;border-bottom:1px solid #333}"
                                "th.num,td.num{text-align:left;padding-right:1em}"
                                "th.num>.v,td.num>.v{display:inline-block;text-align:right;min-width:4em}"
                                "td.age{color:#777}"
                                ".absent{color:#666}"
                                ".mode{color:#fc6}"
                                "#status{color:#777;font-size:.85em;margin-top:.4em}"
                                "</style></head><body>"
                                "<div class=box><h2>Rooms</h2>"
                                "<table id=rooms><thead><tr>"
                                "<th>name</th><th>mode</th>"
                                "<th class=num><span class=v>temp</span></th>"
                                "<th class=num><span class=v>hum</span></th>"
                                "<th class=num><span class=v>set</span></th>"
                                "<th class=num><span class=v>age</span></th>"
                                "</tr></thead><tbody></tbody></table>"
                                "<div id=status>connecting...</div></div>"
                                "<div class=box><h2>Devices</h2>"
                                "<table id=devices><thead><tr>"
                                "<th>name</th><th>id</th>"
                                "<th class=num><span class=v>oem</span></th>"
                                "<th class=num><span class=v>variant</span></th>"
                                "<th class=num><span class=v>firmware</span></th>"
                                "<th class=num><span class=v>age</span></th>"
                                "</tr></thead><tbody></tbody></table></div>"
                                "<div class=box><h2>Events</h2>"
                                "<table id=events><thead><tr>"
                                "<th>kind</th><th>summary</th><th class=num><span class=v>age</span></th>"
                                "</tr></thead><tbody></tbody></table></div>"
                                "<script>\n"
                                "const rtbody=document.querySelector('#rooms tbody');\n"
                                "const dtbody=document.querySelector('#devices tbody');\n"
                                "const etbody=document.querySelector('#events tbody');\n"
                                "const status=document.getElementById('status');\n"
                                "const rooms=new Map(), devices=new Map(), events=[];\n"
                                "const EV_KEEP=5;\n"
                                "function rkey(r){return r.idx+':'+r.type}\n"
                                "function fmt(v,u){return v==null?'<span class=absent>--</span>':v.toFixed(1)+u}\n"
                                "function hex2(n){return '0x'+n.toString(16).toUpperCase().padStart(2,'0')}\n"
                                "function ageFmt(s){\n"
                                "  if(s==null) return '--';\n"
                                "  const dt=Math.max(0,Math.floor(s));\n"
                                "  if(dt<60) return dt+'s';\n"
                                "  if(dt<3600) return Math.floor(dt/60)+'m';\n"
                                "  if(dt<86400) return Math.floor(dt/3600)+'h';\n"
                                "  return Math.floor(dt/86400)+'d';\n"
                                "}\n"
                                "function youngest(r){\n"
                                "  const v=[r.age_temp,r.age_tset,r.age_hum,r.age_sw].filter(x=>x!=null);\n"
                                "  return v.length?Math.min.apply(null,v):null;\n"
                                "}\n"
                                "function renderRooms(){\n"
                                "  rtbody.innerHTML=[...rooms.values()].sort((a,b)=>a.idx-b.idx||a.type-b.type).map(r=>\n"
                                "    '<tr>'+\n"
                                "      '<td>'+(r.name?r.name:'<span class=absent>?</span>')+'</td>'+\n"
                                "      '<td class=mode>'+(r.mode==null?'<span class=absent>--</span>':r.mode)+'</td>'+\n"
                                "      '<td class=num><span class=v>'+fmt(r.temp,'\\u00b0C')+'</span></td>'+\n"
                                "      '<td class=num><span class=v>'+fmt(r.humidity,'%')+'</span></td>'+\n"
                                "      '<td class=num><span class=v>'+fmt(r.tset,'\\u00b0C')+'</span></td>'+\n"
                                "      '<td class=\"num age\"><span class=v>'+ageFmt(youngest(r))+'</span></td>'+\n"
                                "    '</tr>'\n"
                                "  ).join('');\n"
                                "}\n"
                                "function renderDevices(){\n"
                                "  dtbody.innerHTML=[...devices.values()].sort((a,b)=>a.subscriber_id-b.subscriber_id).map(d=>\n"
                                "    '<tr>'+\n"
                                "      '<td>'+(d.subscriber_name||'<span class=absent>?</span>')+'</td>'+\n"
                                "      '<td>'+hex2(d.subscriber_id)+'</td>'+\n"
                                "      '<td class=num><span class=v>'+hex2(d.oem_id||0)+'</span></td>'+\n"
                                "      '<td class=num><span class=v>'+(d.device_variant!=null?d.device_variant:'--')+'</span></td>'+\n"
                                "      '<td class=num><span class=v>'+(d.firmware!=null?d.firmware:'--')+'</span></td>'+\n"
                                "      '<td class=\"num age\"><span class=v>'+ageFmt(d.age)+'</span></td>'+\n"
                                "    '</tr>'\n"
                                "  ).join('');\n"
                                "}\n"
                                "function renderEvents(){\n"
                                "  etbody.innerHTML=events.map(e=>\n"
                                "    '<tr>'+\n"
                                "      '<td>'+(e.kind||'?')+'</td>'+\n"
                                "      '<td>'+(e.summary||'')+'</td>'+\n"
                                "      '<td class=\"num age\"><span class=v>'+ageFmt(e.age)+'</span></td>'+\n"
                                "    '</tr>'\n"
                                "  ).join('');\n"
                                "}\n"
                                "function render(){renderRooms();renderDevices();renderEvents();}\n"
                                "function mergeRoom(r){\n"
                                "  const k=rkey(r);const cur=rooms.get(k)||{idx:r.idx,type:r.type};\n"
                                "  for(const f of ['name','temp','tset','humidity','mode','swval'])\n"
                                "    if(f in r) cur[f]=r[f];\n"
                                "  for(const f of ['age_temp','age_tset','age_hum','age_sw'])\n"
                                "    if(f in r) cur[f]=r[f];\n"
                                "  rooms.set(k,cur);\n"
                                "}\n"
                                "function mergeDevice(d){\n"
                                "  const cur=devices.get(d.subscriber_id)||{subscriber_id:d.subscriber_id};\n"
                                "  for(const f of ['subscriber_name','oem_id','device_variant','firmware','age'])\n"
                                "    if(f in d) cur[f]=d[f];\n"
                                "  devices.set(d.subscriber_id,cur);\n"
                                "}\n"
                                "function mergeEvent(e){\n"
                                "  if(events.length && events[0].kind===e.kind && events[0].summary===e.summary) return;\n"
                                "  events.unshift({...e});\n"
                                "  if(events.length>EV_KEEP) events.length=EV_KEEP;\n"
                                "}\n"
                                "function tick(){\n"
                                "  for(const r of rooms.values())\n"
                                "    for(const f of ['age_temp','age_tset','age_hum','age_sw'])\n"
                                "      if(r[f]!=null) r[f]+=1;\n"
                                "  for(const d of devices.values()) if(d.age!=null) d.age+=1;\n"
                                "  for(const e of events) if(e.age!=null) e.age+=1;\n"
                                "  render();\n"
                                "}\n"
                                "setInterval(tick,1000);\n"
                                "function connect(){\n"
                                "  const es=new EventSource('/events');\n"
                                "  es.onopen=()=>{status.textContent='live'};\n"
                                "  es.onerror=()=>{status.textContent='reconnecting...'};\n"
                                "  es.addEventListener('snapshot',e=>{\n"
                                "    const o=JSON.parse(e.data);\n"
                                "    (o.rooms||[]).forEach(mergeRoom);\n"
                                "    (o.devices||[]).forEach(mergeDevice);\n"
                                "    (o.events||[]).slice().reverse().forEach(mergeEvent);\n"
                                "    render();\n"
                                "  });\n"
                                "  es.addEventListener('room',e=>{mergeRoom(JSON.parse(e.data));render()});\n"
                                "  es.addEventListener('device',e=>{mergeDevice(JSON.parse(e.data));render()});\n"
                                "  es.addEventListener('event',e=>{mergeEvent(JSON.parse(e.data));render()});\n"
                                "}\n"
                                "connect();\n"
                                "</script></body></html>";

static const char MESSAGE_404[] = "Not Found\n";

static void send_html(const int fd) {
    char hdr[256];
    const int hlen = snprintf(hdr, sizeof(hdr),
                              "HTTP/1.1 200 OK\r\n"
                              "Content-Type: text/html; charset=utf-8\r\n"
                              "Content-Length: %zu\r\n"
                              "Connection: close\r\n\r\n",
                              sizeof(HTML_PAGE) - 1);
    if (sse_write(fd, hdr, (size_t)hlen) >= 0)
        sse_write(fd, HTML_PAGE, sizeof(HTML_PAGE) - 1);
}

static void send_404(const int fd) {
    char hdr[256];
    const int hlen = snprintf(hdr, sizeof(hdr),
                              "HTTP/1.1 404 Not Found\r\n"
                              "Content-Type: text/plain\r\n"
                              "Content-Length: %zu\r\n"
                              "Connection: close\r\n\r\n",
                              sizeof(MESSAGE_404) - 1);
    if (sse_write(fd, hdr, (size_t)hlen) >= 0)
        sse_write(fd, MESSAGE_404, sizeof(MESSAGE_404) - 1);
}

static int start_sse(const int fd) {
    /* nonblocking so a slow client cannot stall the broadcast loop */
    fcntl(fd, F_SETFL, fcntl(fd, F_GETFL, 0) | O_NONBLOCK);

    static const char r[] = "HTTP/1.1 200 OK\r\n"
                            "Content-Type: text/event-stream\r\n"
                            "Cache-Control: no-cache\r\n"
                            "Connection: keep-alive\r\n"
                            "Access-Control-Allow-Origin: *\r\n"
                            "X-Accel-Buffering: no\r\n"
                            "\r\n";
    if (sse_write(fd, r, sizeof(r) - 1) < 0)
        return -1;

    pthread_mutex_lock(&g_state_mu);
    const time_t now = time(NULL);
    cJSON *snapo     = cJSON_CreateObject();
    cJSON *rarr      = cJSON_CreateArray();
    cJSON *darr      = cJSON_CreateArray();
    cJSON *earr      = cJSON_CreateArray();
    for (int i = 0; i < g_nrooms; i++)
        cJSON_AddItemToArray(rarr, room_to_json(&g_rooms[i], now));
    for (int i = 0; i < g_ndevices; i++)
        cJSON_AddItemToArray(darr, device_to_json(&g_devices[i], now));
    int en = 0;
    for (int i = 0; i < MAX_EVENTS; i++)
        if (g_events[i].used) {
            cJSON_AddItemToArray(earr, event_to_json(&g_events[i], now));
            en++;
        }
    cJSON_AddItemToObject(snapo, "rooms", rarr);
    cJSON_AddItemToObject(snapo, "devices", darr);
    cJSON_AddItemToObject(snapo, "events", earr);
    const int rn = g_nrooms, dn = g_ndevices;
    pthread_mutex_unlock(&g_state_mu);

    char *snap = cJSON_PrintUnformatted(snapo);
    cJSON_Delete(snapo);

    static const char hdr[] = "event: snapshot\ndata: ";
    if (sse_write(fd, hdr, sizeof(hdr) - 1) < 0 || sse_write(fd, snap, strlen(snap)) < 0 || sse_write(fd, "\n\n", 2) < 0) {
        free(snap);
        return -1;
    }
    free(snap);

    int ret = -1;
    pthread_mutex_lock(&g_clients_mu);
    if (g_nclients < MAX_CLIENTS) {
        g_clients[g_nclients++] = fd;
        DBG("sse client connected fd=%d snapshot_rooms=%d snapshot_devices=%d snapshot_events=%d total_clients=%d", fd, rn, dn, en, g_nclients);
        ret = 0;
    } else
        DBG("sse client rejected (MAX_CLIENTS=%d)", MAX_CLIENTS);
    pthread_mutex_unlock(&g_clients_mu);
    return ret;
}

static void handle_client(const int fd) {
    /* Defensive: a client that connects but never speaks shouldn't wedge the
     * accept loop. 5s is plenty for an HTTP request header on a LAN. */
    const struct timeval to = { .tv_sec = 5, .tv_usec = 0 };
    setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &to, sizeof(to));

    char buf[2048];
    const ssize_t n = recv(fd, buf, sizeof(buf) - 1, 0);
    if (n <= 0) {
        close(fd);
        return;
    }
    buf[n] = 0;

    char path[128] = "";
    sscanf(buf, "GET %127s ", path);
    DBG("http GET %s", path[0] ? path : "(no path)");

    if (strcmp(path, "/") == 0 || strcmp(path, "/index.html") == 0 || strcmp(path, "/index.htm") == 0) {
        send_html(fd);
        close(fd);
    } else if (strcmp(path, "/events") == 0) {
        if (start_sse(fd) < 0) /* SSE socket retained in g_clients */
            close(fd);
    } else {
        send_404(fd);
        close(fd);
    }
}

/* keepalive comment every HEARTBEAT_SECS so dropped clients get pruned */
static void *heartbeat_thread(__attribute__((unused)) void *u) {
    while (!g_stop) {
        sleep(HEARTBEAT_SECS);
        pthread_mutex_lock(&g_clients_mu);
        for (int i = 0; i < g_nclients;)
            if (sse_write(g_clients[i], ":keepalive\n\n", 12) < 0) {
                close(g_clients[i]);
                g_clients[i] = g_clients[--g_nclients];
            } else
                i++;
        pthread_mutex_unlock(&g_clients_mu);
    }
    return NULL;
}

/* -------------------------------------------------------------- main / cli */

static int parse_mqtt_arg(const char *s, char *host, size_t hostlen, int *port) {
    *port = 1883;
    if (strncmp(s, "mqtt://", 7) == 0)
        s += 7;
    const char *col = strrchr(s, ':');
    if (col) {
        size_t l = (size_t)(col - s);
        if (l >= hostlen)
            l = hostlen - 1;
        memcpy(host, s, l);
        host[l] = 0;
        *port   = atoi(col + 1);
        if (*port <= 0 || *port > 65535)
            return -1;
    } else
        snprintf(host, hostlen, "%s", s);
    return 0;
}

static void usage(const char *p) {
    fprintf(stderr,
            "Usage: %s [--mqtt HOST[:PORT]] [--port HTTP_PORT] [--debug]\n"
            "       --mqtt   MQTT broker (default: localhost:1883)\n"
            "       --port   HTTP listen port (default: %d)\n"
            "       --debug  verbose stderr trace of MQTT/HTTP/SSE activity\n",
            p, DEFAULT_HTTP_PORT);
}

int main(int argc, char **argv) {
    char mqtt_host[128] = "localhost";
    int mqtt_port       = 1883;
    int http_port       = DEFAULT_HTTP_PORT;

    static struct option opts[] = {
        { "mqtt", required_argument, 0, 'm' }, { "port", required_argument, 0, 'p' }, { "debug", no_argument, 0, 'd' }, { "help", no_argument, 0, 'h' }, { 0, 0, 0, 0 },
    };
    int o;
    while ((o = getopt_long(argc, argv, "m:p:dh", opts, NULL)) != -1) {
        switch (o) {
        case 'm':
            if (parse_mqtt_arg(optarg, mqtt_host, sizeof(mqtt_host), &mqtt_port) != 0) {
                fprintf(stderr, "bad --mqtt argument: %s\n", optarg);
                return EXIT_FAILURE;
            }
            break;
        case 'p':
            http_port = atoi(optarg);
            if (http_port <= 0 || http_port > 65535) {
                fprintf(stderr, "bad --port: %s\n", optarg);
                return EXIT_FAILURE;
            }
            break;
        case 'd':
            g_debug = true;
            break;
        case 'h':
            usage(argv[0]);
            return EXIT_SUCCESS;
        default:
            usage(argv[0]);
            return EXIT_FAILURE;
        }
    }

    signal(SIGPIPE, SIG_IGN);
    signal(SIGINT, on_sig);
    signal(SIGTERM, on_sig);

    mosquitto_lib_init();
    struct mosquitto *m = mosquitto_new(NULL, true, NULL);
    if (!m) {
        fprintf(stderr, "mosquitto_new failed\n");
        return EXIT_FAILURE;
    }
    mosquitto_connect_callback_set(m, on_mqtt_connect);
    mosquitto_disconnect_callback_set(m, on_mqtt_disconnect);
    mosquitto_subscribe_callback_set(m, on_mqtt_subscribe);
    mosquitto_message_callback_set(m, on_mqtt_msg);
    const int rc = mosquitto_connect_async(m, mqtt_host, mqtt_port, 60);
    if (rc != MOSQ_ERR_SUCCESS)
        fprintf(stderr, "mosquitto_connect_async: %s (continuing; will retry)\n", mosquitto_strerror(rc));
    mosquitto_loop_start(m);

    const int lfd = socket(AF_INET, SOCK_STREAM, 0);
    if (lfd < 0) {
        perror("socket");
        return EXIT_FAILURE;
    }
    int one = 1;
    setsockopt(lfd, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one));

    const struct sockaddr_in sa = { .sin_family = AF_INET, .sin_addr.s_addr = htonl(INADDR_ANY), .sin_port = htons((uint16_t)http_port) };
    if (bind(lfd, (const struct sockaddr *)&sa, sizeof(sa)) < 0) {
        perror("bind");
        return EXIT_FAILURE;
    }
    if (listen(lfd, LISTEN_BACKLOG) < 0) {
        perror("listen");
        return EXIT_FAILURE;
    }
    fprintf(stderr, "caleon-web: mqtt=%s:%d  http=0.0.0.0:%d\n", mqtt_host, mqtt_port, http_port);

    pthread_t hb;
    pthread_create(&hb, NULL, heartbeat_thread, NULL);
    pthread_detach(hb);

    while (!g_stop) {
        struct pollfd pfd = { .fd = lfd, .events = POLLIN };
        const int n       = poll(&pfd, 1, 500);
        if (n < 0) {
            if (errno == EINTR)
                continue;
            perror("poll");
            break;
        }
        if (n == 0)
            continue;
        if (pfd.revents & POLLIN) {
            const int cfd = accept(lfd, NULL, NULL);
            if (cfd >= 0)
                handle_client(cfd);
        }
    }

    close(lfd);
    mosquitto_loop_stop(m, true);
    mosquitto_destroy(m);
    mosquitto_lib_cleanup();
    return EXIT_SUCCESS;
}
