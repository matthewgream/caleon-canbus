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

.PHONY: all clean install install-bin install-cfg install-service uninstall format

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

install: install-bin install-cfg install-service

install-bin: $(BINS)
	install -d $(DESTDIR)$(BINDIR)
	install -m 0755 caleon2mqtt $(DESTDIR)$(BINDIR)/caleon2mqtt
	install -m 0755 caleon-web  $(DESTDIR)$(BINDIR)/caleon-web

install-cfg: $(CFG_SRC)
	@echo "installing config from $(CFG_SRC) -> $(DESTDIR)$(DEFAULTDIR)/caleon2mqtt"
	install -d $(DESTDIR)$(DEFAULTDIR)
	install -m 0644 $(CFG_SRC) $(DESTDIR)$(DEFAULTDIR)/caleon2mqtt

install-service: $(SERVICES)
	install -d $(DESTDIR)$(SYSTEMDDIR)
	install -m 0644 caleon2mqtt.service $(DESTDIR)$(SYSTEMDDIR)/caleon2mqtt.service
	install -m 0644 caleon-web.service  $(DESTDIR)$(SYSTEMDDIR)/caleon-web.service

uninstall:
	rm -f $(DESTDIR)$(BINDIR)/caleon2mqtt
	rm -f $(DESTDIR)$(BINDIR)/caleon-web
	rm -f $(DESTDIR)$(DEFAULTDIR)/caleon2mqtt
	rm -f $(DESTDIR)$(SYSTEMDDIR)/caleon2mqtt.service
	rm -f $(DESTDIR)$(SYSTEMDDIR)/caleon-web.service

claude:
	claude --resume 27a209a4-0796-45c3-9035-ac338f1306a3
