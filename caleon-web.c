/*
 * caleon-web -- minimal HTTP UI in front of the caleon2mqtt MQTT stream.
 *
 * Build:  make caleon-web   (libmosquitto + libcjson + pthread)
 * Run:    ./caleon-web --mqtt localhost:1883 --port 3002
 *
 * Listens to the `caleon/sensor` topic published by caleon2mqtt, keeps an
 * in-memory snapshot of every room it has seen, and serves a tiny HTML page
 * plus a Server-Sent Events stream so connected browsers update live.
 *
 * Only the rooms panel is rendered for now; more panels (relays, HC
 * sensors, central plant, devices) can be added the same way later.
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
static int g_nrooms               = 0;
static pthread_mutex_t g_state_mu = PTHREAD_MUTEX_INITIALIZER;

static int g_clients[MAX_CLIENTS];
static int g_nclients               = 0;
static pthread_mutex_t g_clients_mu = PTHREAD_MUTEX_INITIALIZER;

static volatile sig_atomic_t g_stop = 0;
static void on_sig(int s) {
    (void)s;
    g_stop = 1;
}

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
        } else {
            cJSON_AddNullToObject(o, "mode");
        }
        cJSON_AddNumberToObject(o, "age_sw", (double)(now - r->t_sw));
    }
    return o;
}

/* Write the full buffer; -1 means caller should drop the client. */
static int sse_write(int fd, const char *buf, size_t len) {
    while (len) {
        ssize_t n = send(fd, buf, len, MSG_NOSIGNAL);
        if (n < 0) {
            if (errno == EINTR)
                continue;
            return -1; /* EAGAIN on a slow client also drops -- intentional */
        }
        buf += n;
        len -= n;
    }
    return 0;
}

static void broadcast_event(const char *event, const char *json) {
    char head[64];
    int hlen    = snprintf(head, sizeof(head), "event: %s\ndata: ", event);
    size_t jlen = strlen(json);
    pthread_mutex_lock(&g_clients_mu);
    int dropped = 0, sent = 0;
    int i = 0;
    while (i < g_nclients) {
        int fd = g_clients[i];
        if (sse_write(fd, head, (size_t)hlen) < 0 || sse_write(fd, json, jlen) < 0 || sse_write(fd, "\n\n", 2) < 0) {
            close(fd);
            g_clients[i] = g_clients[--g_nclients];
            dropped++;
            continue;
        }
        sent++;
        i++;
    }
    int total = g_nclients;
    pthread_mutex_unlock(&g_clients_mu);
    DBG("sse broadcast event=%s bytes=%zu sent=%d dropped=%d clients=%d", event, jlen, sent, dropped, total);
}

static void broadcast_room(const room_t *r) {
    time_t now = time(NULL);
    cJSON *o   = room_to_json(r, now);
    char *s    = cJSON_PrintUnformatted(o);
    broadcast_event("room", s);
    free(s);
    cJSON_Delete(o);
}

/* -------------------------------------------------------------- mqtt side */

static void on_mqtt_connect(struct mosquitto *m, void *u, int rc) {
    (void)u;
    if (rc != 0) {
        fprintf(stderr, "mqtt: connect failed: %s\n", mosquitto_connack_string(rc));
        return;
    }
    fprintf(stderr, "mqtt: connected, subscribing caleon/sensor\n");
    int mid = 0;
    int sub = mosquitto_subscribe(m, &mid, "caleon/sensor", 0);
    DBG("mqtt subscribe rc=%d mid=%d topic=caleon/sensor", sub, mid);
}

static void on_mqtt_disconnect(struct mosquitto *m, void *u, int rc) {
    (void)m;
    (void)u;
    fprintf(stderr, "mqtt: disconnected rc=%d (%s)\n", rc, rc ? "unexpected -- async reconnect will retry" : "clean");
}

static void on_mqtt_subscribe(struct mosquitto *m, void *u, int mid, int qos_count, const int *granted_qos) {
    (void)m;
    (void)u;
    DBG("mqtt SUBACK mid=%d qos_count=%d granted_qos[0]=%d", mid, qos_count, qos_count > 0 ? granted_qos[0] : -1);
}

static void on_mqtt_msg(struct mosquitto *m, void *u, const struct mosquitto_message *msg) {
    (void)m;
    (void)u;
    if (!msg->payload || msg->payloadlen <= 0) {
        DBG("mqtt msg topic=%s empty payload", msg->topic ? msg->topic : "?");
        return;
    }

    char *tmp = malloc((size_t)msg->payloadlen + 1);
    if (!tmp)
        return;
    memcpy(tmp, msg->payload, (size_t)msg->payloadlen);
    tmp[msg->payloadlen] = 0;
    DBG("mqtt msg topic=%s len=%d payload=%.200s", msg->topic ? msg->topic : "?", msg->payloadlen, tmp);
    cJSON *p = cJSON_Parse(tmp);
    free(tmp);
    if (!p) {
        DBG("  -> JSON parse failed");
        return;
    }

    cJSON *jfn = cJSON_GetObjectItem(p, "function");
    cJSON *jri = cJSON_GetObjectItem(p, "room_index");
    cJSON *jrt = cJSON_GetObjectItem(p, "room_type");
    if (!cJSON_IsString(jfn) || !cJSON_IsNumber(jri) || !cJSON_IsNumber(jrt)) {
        DBG("  -> skip: missing function/room_index/room_type"
            " (fn=%s ri=%s rt=%s)",
            cJSON_IsString(jfn) ? "ok" : "no", cJSON_IsNumber(jri) ? "ok" : "no", cJSON_IsNumber(jrt) ? "ok" : "no");
        goto out;
    }

    int idx = (int)jri->valuedouble, type = (int)jrt->valuedouble;
    /* (RI=0, RT=0) is the central plant pseudo-room, not a real room. */
    if (idx == 0 && type == 0) {
        DBG("  -> skip: central plant (RI=0, RT=0) fn=%s", jfn->valuestring);
        goto out;
    }

    cJSON *jv    = cJSON_GetObjectItem(p, "value");
    cJSON *jvr   = cJSON_GetObjectItem(p, "value_raw");
    cJSON *jpres = cJSON_GetObjectItem(p, "present");
    cJSON *jname = cJSON_GetObjectItem(p, "room_name");
    cJSON *jmode = cJSON_GetObjectItem(p, "mode_name");

    bool present = !(cJSON_IsBool(jpres) && !jpres->valueint);
    if (cJSON_IsNull(jv))
        present = false;

    time_t now  = time(NULL);
    bool change = false;
    room_t snap;

    pthread_mutex_lock(&g_state_mu);
    room_t *r = find_or_add_room(idx, type);
    if (!r) {
        pthread_mutex_unlock(&g_state_mu);
        goto out;
    }

    if (cJSON_IsString(jname))
        snprintf(r->name, sizeof(r->name), "%s", jname->valuestring);

    const char *fn = jfn->valuestring;
    if (strcmp(fn, "ecsRcTroom") == 0) {
        r->seen_temp    = true;
        r->present_temp = present;
        if (present && cJSON_IsNumber(jv))
            r->temp = jv->valuedouble;
        r->t_temp = now;
        change    = true;
    } else if (strcmp(fn, "ecsRcTset") == 0) {
        r->seen_tset    = true;
        r->present_tset = present;
        if (present && cJSON_IsNumber(jv))
            r->tset = jv->valuedouble;
        r->t_tset = now;
        change    = true;
    } else if (strcmp(fn, "ecsRcHumidity") == 0) {
        r->seen_hum    = true;
        r->present_hum = present;
        if (present && cJSON_IsNumber(jv))
            r->humidity = jv->valuedouble;
        r->t_hum = now;
        change   = true;
    } else if (strcmp(fn, "ecsRcSwitch") == 0) {
        r->seen_sw    = true;
        r->present_sw = present;
        if (present && cJSON_IsNumber(jvr)) {
            r->swval = (int)jvr->valuedouble;
            snprintf(r->mode, sizeof(r->mode), "%s", cJSON_IsString(jmode) ? jmode->valuestring : "?");
        }
        r->t_sw = now;
        change  = true;
    }
    snap = *r;
    pthread_mutex_unlock(&g_state_mu);

    if (change) {
        DBG("  -> room idx=%d type=%d fn=%s present=%d name=%s", idx, type, fn, present, snap.name[0] ? snap.name : "?");
        broadcast_room(&snap);
    } else {
        DBG("  -> no match: fn=%s (not one of ecsRcTroom/Tset/Humidity/Switch)", fn);
    }
out:
    cJSON_Delete(p);
}

/* -------------------------------------------------------------- http side */

static const char HTML_PAGE[] = "<!doctype html><html><head><meta charset=utf-8>"
                                "<title>caleon</title>"
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
                                "td.num{text-align:right;padding-right:1em}"
                                "td.age{color:#777}"
                                ".absent{color:#666}"
                                ".mode{color:#fc6}"
                                "#status{color:#777;font-size:.85em;margin-top:.4em}"
                                "</style></head><body>"
                                "<h1>caleon</h1>"
                                "<div class=box><h2>Rooms</h2>"
                                "<table id=rooms><thead><tr>"
                                "<th>idx</th><th>type</th><th>name</th>"
                                "<th class=num>temp</th><th class=num>set</th>"
                                "<th>mode</th><th class=num>hum</th><th class=num>age</th>"
                                "</tr></thead><tbody></tbody></table>"
                                "<div id=status>connecting...</div></div>"
                                "<script>\n"
                                "const tbody=document.querySelector('#rooms tbody');\n"
                                "const status=document.getElementById('status');\n"
                                "const rooms=new Map();\n"
                                "function key(r){return r.idx+':'+r.type}\n"
                                "function fmt(v,u){return v==null?'<span class=absent>--</span>':v.toFixed(1)+u}\n"
                                "function youngest(r){\n"
                                "  const v=[r.age_temp,r.age_tset,r.age_hum,r.age_sw].filter(x=>x!=null);\n"
                                "  return v.length?Math.min.apply(null,v):null;\n"
                                "}\n"
                                "function render(){\n"
                                "  const ord=[...rooms.values()].sort((a,b)=>a.idx-b.idx||a.type-b.type);\n"
                                "  tbody.innerHTML=ord.map(r=>{\n"
                                "    const age=youngest(r);\n"
                                "    return '<tr>'+\n"
                                "      '<td>'+r.idx+'</td>'+\n"
                                "      '<td>'+r.type+'</td>'+\n"
                                "      '<td>'+(r.name?r.name:'<span class=absent>?</span>')+'</td>'+\n"
                                "      '<td class=num>'+fmt(r.temp,'\\u00b0C')+'</td>'+\n"
                                "      '<td class=num>'+fmt(r.tset,'\\u00b0C')+'</td>'+\n"
                                "      '<td class=mode>'+(r.mode==null?'<span class=absent>--</span>':r.mode)+'</td>'+\n"
                                "      '<td class=num>'+fmt(r.humidity,'%')+'</td>'+\n"
                                "      '<td class=age>'+(age==null?'--':Math.round(age)+'s')+'</td>'+\n"
                                "    '</tr>';\n"
                                "  }).join('');\n"
                                "}\n"
                                "function merge(r){\n"
                                "  const k=key(r);const cur=rooms.get(k)||{idx:r.idx,type:r.type};\n"
                                "  for(const f of ['name','temp','tset','humidity','mode','swval'])\n"
                                "    if(f in r) cur[f]=r[f];\n"
                                "  for(const f of ['age_temp','age_tset','age_hum','age_sw'])\n"
                                "    if(f in r) cur[f]=r[f];\n"
                                "  rooms.set(k,cur);\n"
                                "}\n"
                                "function tick(){\n"
                                "  for(const r of rooms.values())\n"
                                "    for(const f of ['age_temp','age_tset','age_hum','age_sw'])\n"
                                "      if(r[f]!=null) r[f]+=1;\n"
                                "  render();\n"
                                "}\n"
                                "setInterval(tick,1000);\n"
                                "function connect(){\n"
                                "  const es=new EventSource('/events');\n"
                                "  es.onopen=()=>{status.textContent='live'};\n"
                                "  es.onerror=()=>{status.textContent='reconnecting...'};\n"
                                "  es.addEventListener('snapshot',e=>{JSON.parse(e.data).forEach(merge);render()});\n"
                                "  es.addEventListener('room',e=>{merge(JSON.parse(e.data));render()});\n"
                                "}\n"
                                "connect();\n"
                                "</script></body></html>";

static const char SSE_HEAD[] = "HTTP/1.1 200 OK\r\n"
                               "Content-Type: text/event-stream\r\n"
                               "Cache-Control: no-cache\r\n"
                               "Connection: keep-alive\r\n"
                               "Access-Control-Allow-Origin: *\r\n"
                               "X-Accel-Buffering: no\r\n"
                               "\r\n";

static void send_html(int fd) {
    char hdr[256];
    int hlen = snprintf(hdr, sizeof(hdr),
                        "HTTP/1.1 200 OK\r\n"
                        "Content-Type: text/html; charset=utf-8\r\n"
                        "Content-Length: %zu\r\n"
                        "Connection: close\r\n\r\n",
                        sizeof(HTML_PAGE) - 1);
    sse_write(fd, hdr, (size_t)hlen);
    sse_write(fd, HTML_PAGE, sizeof(HTML_PAGE) - 1);
}

static void send_404(int fd) {
    static const char r[] = "HTTP/1.1 404 Not Found\r\n"
                            "Content-Type: text/plain\r\n"
                            "Content-Length: 10\r\n"
                            "Connection: close\r\n\r\nNot Found\n";
    sse_write(fd, r, sizeof(r) - 1);
}

static void start_sse(int fd) {
    /* nonblocking so a slow client cannot stall the broadcast loop */
    int fl = fcntl(fd, F_GETFL, 0);
    fcntl(fd, F_SETFL, fl | O_NONBLOCK);

    if (sse_write(fd, SSE_HEAD, sizeof(SSE_HEAD) - 1) < 0) {
        close(fd);
        return;
    }

    pthread_mutex_lock(&g_state_mu);
    time_t now = time(NULL);
    cJSON *arr = cJSON_CreateArray();
    for (int i = 0; i < g_nrooms; i++)
        cJSON_AddItemToArray(arr, room_to_json(&g_rooms[i], now));
    pthread_mutex_unlock(&g_state_mu);

    char *snap = cJSON_PrintUnformatted(arr);
    cJSON_Delete(arr);

    static const char hdr[] = "event: snapshot\ndata: ";
    if (sse_write(fd, hdr, sizeof(hdr) - 1) < 0 || sse_write(fd, snap, strlen(snap)) < 0 || sse_write(fd, "\n\n", 2) < 0) {
        free(snap);
        close(fd);
        return;
    }
    free(snap);

    pthread_mutex_lock(&g_clients_mu);
    if (g_nclients < MAX_CLIENTS) {
        g_clients[g_nclients++] = fd;
        DBG("sse client connected fd=%d snapshot_rooms=%d total_clients=%d", fd, g_nrooms, g_nclients);
    } else {
        DBG("sse client rejected (MAX_CLIENTS=%d)", MAX_CLIENTS);
        close(fd);
    }
    pthread_mutex_unlock(&g_clients_mu);
}

static void handle_client(int fd) {
    /* Defensive: a client that connects but never speaks shouldn't wedge the
     * accept loop. 5s is plenty for an HTTP request header on a LAN. */
    struct timeval to = { .tv_sec = 5, .tv_usec = 0 };
    setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &to, sizeof(to));

    char buf[2048];
    ssize_t n = recv(fd, buf, sizeof(buf) - 1, 0);
    if (n <= 0) {
        close(fd);
        return;
    }
    buf[n] = 0;

    char path[128] = "";
    sscanf(buf, "GET %127s ", path);
    DBG("http GET %s", path[0] ? path : "(no path)");

    if (strcmp(path, "/") == 0 || strcmp(path, "/index.html") == 0) {
        send_html(fd);
        close(fd);
    } else if (strcmp(path, "/events") == 0) {
        start_sse(fd); /* SSE socket retained in g_clients */
    } else {
        send_404(fd);
        close(fd);
    }
}

/* keepalive comment every HEARTBEAT_SECS so dropped clients get pruned */
static void *heartbeat_thread(void *u) {
    (void)u;
    while (!g_stop) {
        sleep(HEARTBEAT_SECS);
        pthread_mutex_lock(&g_clients_mu);
        int i = 0;
        while (i < g_nclients) {
            if (sse_write(g_clients[i], ":keepalive\n\n", 12) < 0) {
                close(g_clients[i]);
                g_clients[i] = g_clients[--g_nclients];
            } else {
                i++;
            }
        }
        pthread_mutex_unlock(&g_clients_mu);
    }
    return NULL;
}

/* -------------------------------------------------------------- main / cli */

static int parse_mqtt_arg(const char *s, char *host, size_t hostlen, int *port) {
    *port = 1883;
    if (strncmp(s, "mqtt://", 7) == 0)
        s += 7;
    if (strncmp(s, "tcp://", 6) == 0)
        s += 6;
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
    } else {
        snprintf(host, hostlen, "%s", s);
    }
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
                return 1;
            }
            break;
        case 'p':
            http_port = atoi(optarg);
            if (http_port <= 0 || http_port > 65535) {
                fprintf(stderr, "bad --port: %s\n", optarg);
                return 1;
            }
            break;
        case 'd':
            g_debug = true;
            break;
        case 'h':
            usage(argv[0]);
            return 0;
        default:
            usage(argv[0]);
            return 1;
        }
    }

    signal(SIGPIPE, SIG_IGN);
    signal(SIGINT, on_sig);
    signal(SIGTERM, on_sig);

    mosquitto_lib_init();
    struct mosquitto *m = mosquitto_new(NULL, true, NULL);
    if (!m) {
        fprintf(stderr, "mosquitto_new failed\n");
        return 1;
    }
    mosquitto_connect_callback_set(m, on_mqtt_connect);
    mosquitto_disconnect_callback_set(m, on_mqtt_disconnect);
    mosquitto_subscribe_callback_set(m, on_mqtt_subscribe);
    mosquitto_message_callback_set(m, on_mqtt_msg);
    int rc = mosquitto_connect_async(m, mqtt_host, mqtt_port, 60);
    if (rc != MOSQ_ERR_SUCCESS) {
        fprintf(stderr, "mosquitto_connect_async: %s (continuing; will retry)\n", mosquitto_strerror(rc));
    }
    mosquitto_loop_start(m);

    int lfd = socket(AF_INET, SOCK_STREAM, 0);
    if (lfd < 0) {
        perror("socket");
        return 1;
    }
    int one = 1;
    setsockopt(lfd, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one));

    struct sockaddr_in sa = { 0 };
    sa.sin_family         = AF_INET;
    sa.sin_addr.s_addr    = htonl(INADDR_ANY);
    sa.sin_port           = htons((uint16_t)http_port);
    if (bind(lfd, (struct sockaddr *)&sa, sizeof(sa)) < 0) {
        perror("bind");
        return 1;
    }
    if (listen(lfd, LISTEN_BACKLOG) < 0) {
        perror("listen");
        return 1;
    }
    fprintf(stderr, "caleon-web: mqtt=%s:%d  http=0.0.0.0:%d\n", mqtt_host, mqtt_port, http_port);

    pthread_t hb;
    pthread_create(&hb, NULL, heartbeat_thread, NULL);
    pthread_detach(hb);

    while (!g_stop) {
        struct pollfd pfd = { .fd = lfd, .events = POLLIN };
        int n             = poll(&pfd, 1, 500);
        if (n < 0) {
            if (errno == EINTR)
                continue;
            perror("poll");
            break;
        }
        if (n == 0)
            continue;
        if (pfd.revents & POLLIN) {
            int cfd = accept(lfd, NULL, NULL);
            if (cfd >= 0)
                handle_client(cfd);
        }
    }

    close(lfd);
    mosquitto_loop_stop(m, true);
    mosquitto_destroy(m);
    mosquitto_lib_cleanup();
    return 0;
}
