CC      ?= gcc
CFLAGS  ?= -O2 -g -Wall -Wextra -Wno-unused-parameter
LDFLAGS ?=

LIBS_MQTT = -lmosquitto -lcjson
LIBS_WEB  = -lmosquitto -lcjson -lpthread

PREFIX      ?= /usr/local
BINDIR       = $(PREFIX)/bin
SYSTEMDDIR  ?= /etc/systemd/system
DEFAULTDIR  ?= /etc/default

BINS     = caleon2mqtt caleon-web
SERVICES = caleon2mqtt.service caleon-web.service
C_SRCS   = caleon2mqtt.c caleon-web.c
JS_SRCS  = caleon-tui.js

# If a host-specific config caleon2mqtt.<hostname>.cfg exists in the source
# directory, install that as /etc/default/caleon2mqtt; otherwise fall back
# to the repo's caleon2mqtt.cfg. Same pattern as /opt/hostmon.
HOSTNAME := $(shell hostname)
CFG_SRC  := $(if $(wildcard caleon2mqtt.$(HOSTNAME).cfg),caleon2mqtt.$(HOSTNAME).cfg,caleon2mqtt.cfg)

.PHONY: all clean format \
        install install-caleon2mqtt install-caleon-web \
        install-caleon2mqtt-bin install-caleon2mqtt-cfg install-caleon2mqtt-service \
        install-caleon-web-bin  install-caleon-web-service \
        restart restart-caleon2mqtt restart-caleon-web \
        uninstall uninstall-caleon2mqtt uninstall-caleon-web

all: $(BINS)

format:
	clang-format -i $(C_SRCS)
	prettier --write $(JS_SRCS)

caleon2mqtt: caleon2mqtt.c
	$(CC) $(CFLAGS) $(LDFLAGS) -o $@ $< $(LIBS_MQTT)

caleon-web: caleon-web.c
	$(CC) $(CFLAGS) $(LDFLAGS) -o $@ $< $(LIBS_WEB)

clean:
	rm -f $(BINS)

# --- service helper -------------------------------------------------------
# Same shape as /opt/hostmon: stop + disable any prior unit, drop the new
# .service file, reload systemd, then enable + start. Safe to run when the
# service doesn't yet exist (the stop/disable calls just no-op).
define install_service
	-systemctl stop $(1) 2>/dev/null || true
	-systemctl disable $(1) 2>/dev/null || true
	install -d $(DESTDIR)$(SYSTEMDDIR)
	install -m 0644 $(1).service $(DESTDIR)$(SYSTEMDDIR)/$(1).service
	systemctl daemon-reload
	systemctl enable $(1)
	systemctl start $(1) || echo "Warning: failed to start $(1)"
endef

# --- caleon2mqtt ----------------------------------------------------------

install-caleon2mqtt-bin: caleon2mqtt
	install -d $(DESTDIR)$(BINDIR)
	install -m 0755 caleon2mqtt $(DESTDIR)$(BINDIR)/caleon2mqtt

install-caleon2mqtt-cfg: $(CFG_SRC)
	@echo "installing config from $(CFG_SRC) -> $(DESTDIR)$(DEFAULTDIR)/caleon2mqtt"
	install -d $(DESTDIR)$(DEFAULTDIR)
	install -m 0644 $(CFG_SRC) $(DESTDIR)$(DEFAULTDIR)/caleon2mqtt

install-caleon2mqtt-service: caleon2mqtt.service
	$(call install_service,caleon2mqtt)

install-caleon2mqtt: install-caleon2mqtt-bin install-caleon2mqtt-cfg install-caleon2mqtt-service

restart-caleon2mqtt:
	systemctl restart caleon2mqtt

uninstall-caleon2mqtt:
	-systemctl stop caleon2mqtt 2>/dev/null || true
	-systemctl disable caleon2mqtt 2>/dev/null || true
	rm -f $(DESTDIR)$(SYSTEMDDIR)/caleon2mqtt.service
	rm -f $(DESTDIR)$(DEFAULTDIR)/caleon2mqtt
	rm -f $(DESTDIR)$(BINDIR)/caleon2mqtt
	systemctl daemon-reload

# --- caleon-web (no config file; broker/port come in via CLI args) --------

install-caleon-web-bin: caleon-web
	install -d $(DESTDIR)$(BINDIR)
	install -m 0755 caleon-web $(DESTDIR)$(BINDIR)/caleon-web

install-caleon-web-service: caleon-web.service
	$(call install_service,caleon-web)

install-caleon-web: install-caleon-web-bin install-caleon-web-service

restart-caleon-web:
	systemctl restart caleon-web

uninstall-caleon-web:
	-systemctl stop caleon-web 2>/dev/null || true
	-systemctl disable caleon-web 2>/dev/null || true
	rm -f $(DESTDIR)$(SYSTEMDDIR)/caleon-web.service
	rm -f $(DESTDIR)$(BINDIR)/caleon-web
	systemctl daemon-reload

# --- combined -------------------------------------------------------------

install:   install-caleon2mqtt   install-caleon-web
restart:   restart-caleon2mqtt   restart-caleon-web
uninstall: uninstall-caleon2mqtt uninstall-caleon-web

claude:
	claude --resume 27a209a4-0796-45c3-9035-ac338f1306a3
