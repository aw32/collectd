/**
 * collectd - src/systemd-stats.c
 * Foobar
 *
 * Authors:
 *   Alex
 **/

#include "collectd.h"
#include "utils/common/common.h"
#include "plugin.h"
#include <stdint.h>
#include <systemd/sd-bus.h>

static const char *metrics_name[] =
{
    "CPUUsageNSec",
    "IPEgressBytes",
    "IPEgressPackets",
    "IPIngressBytes",
    "IPIngressPackets",
    "MemoryCurrent",
    "TasksCurrent"
};
static int metrics_num = STATIC_ARRAY_SIZE (metrics_name);
//static const char *metrics_short[] =
//{
//    "cpu",
//    "ipoutbytes",
//    "ipoutpackets",
//    "ipinbytes",
//    "ipinpackets",
//    "mem",
//    "tasks"
//};

static int metrics_type[] =
{
    DS_TYPE_DERIVE,
    DS_TYPE_DERIVE,
    DS_TYPE_DERIVE,
    DS_TYPE_DERIVE,
    DS_TYPE_DERIVE,
    DS_TYPE_GAUGE,
    DS_TYPE_GAUGE
};

struct service {
    char* name;
    char* path;
    int failed;
};

static struct service *services = NULL;

static int services_num = 0;

static sd_bus *bus = NULL;
  //sd_bus_message *msg = NULL;

static const char *config_keys[] =
{
  "Service"
};
static int config_keys_num = STATIC_ARRAY_SIZE (config_keys);

// try to get dbus path for given service
static int find_service(struct service *service) {
  int r = 0;
  if (bus == NULL) {
    return -1;
  }
  // check if bus is open
  if (sd_bus_is_open(bus) < 0) {
      ERROR("systemd_stats: reopen dbus connection");
    r = sd_bus_open_system(&bus);
    if (r < 0) {
      ERROR("systemd_stats: dbus connection failed");
      return -1;
    }
  }

  sd_bus_error error = SD_BUS_ERROR_NULL;
  sd_bus_message *m = NULL;
  char* path = NULL;

  // ask systemd for service path
  r = sd_bus_call_method(bus,
    "org.freedesktop.systemd1",
    "/org/freedesktop/systemd1",
    "org.freedesktop.systemd1.Manager",
    "GetUnit",
    &error,
    &m,
    "s",
    service->name);
  if (r < 0) {
    WARNING("systemd_stats: service %s not found: %s %s", service->name, error.name, error.message);
    service->failed = 1;
    return -1;
  }

  // get path from returned object
  r = sd_bus_message_read(m, "o", &path);
  if (r < 0) {
    WARNING("systemd_stats: service obj %s not found: %s %s", service->name, error.name, error.message);
    service->failed = 1;
    return -1;
  }

  service->path = strdup(path);
  sd_bus_error_free(&error);
  sd_bus_message_unref(m);
  return 0;
}

// add new struct service to services array
static struct service *append(struct service *services, int services_num, char* service_name) {
  struct service *list = NULL;
  if (services == NULL || services_num == 0) {
    list = calloc(1, sizeof(struct service));
    services_num = 1;
  } else {
    list = calloc(services_num+1, sizeof(struct service));
    memcpy(list, services, services_num*sizeof(struct service));
    free(services);
    services_num++;
  }
  list[services_num-1].name = service_name;
  return list;
}

// process "Service" entry in config
static int systemd_stats_config (const char *key, const char *value) {
  if (key == 0 || value == 0) {
    return -1;
  }
  if (strncmp(key, "Service", 7) != 0) {
    WARNING("systemd_stats: config key %s is unknown", key);
    return -1;
  }
  if (strlen(value) == 0) {
    WARNING("systemd_stats: config value is empty");
    return -1;
  }
  char* service_name = strdup(value);
  services = append(services, services_num, service_name);
  services_num++;
  return 0;
}

// open bus
static int systemd_stats_init () {
  if (bus != NULL) {
    return 0;
  }
  int r;
  sd_bus_error error = SD_BUS_ERROR_NULL;
  //uint64_t value;
  r = sd_bus_open_system(&bus);
  if (r < 0) {
    ERROR("systemd_stats: dbus connection failed");
    sd_bus_error_free(&error);
    return -1;
  }
  sd_bus_error_free(&error);
  for (int i=0; i<services_num; i++) {
    find_service(&(services[i]));
  }
    
  return 0;
}

// read values for given service and dispatch returned values
static void read_service(struct service *service) {
  int r = 0;
  uint64_t value = 0;
  // get service path
  // in case that service starts after collectd
  if (service->path == NULL) {
    r = find_service(service);
    if (r < 0) {
      if (service->failed == 0) {
        WARNING("systemd_stats: service %s not found", service->name);
        service->failed = 1;
      }
      return;
    }
  }

  // prepare data structure
  value_t values[metrics_num];
  memset(values, 0, sizeof(value_t)*metrics_num);
  value_list_t vl = VALUE_LIST_INIT;

  sd_bus_error error = SD_BUS_ERROR_NULL;
  //char state[13];
  char *state = NULL;
  // check if service is active
  r = sd_bus_get_property_string(bus,
    "org.freedesktop.systemd1",
    service->path,
    "org.freedesktop.systemd1.Unit",
    "ActiveState",
    &error,
    &state);
  if (r < 0) {

    if (service->failed == 0) {
      WARNING("systemd_stats: read failed for %s, dbus error: %s %s", service->name, error.name, error.message);
      service->failed = 1;
    }
    sd_bus_error_free(&error);
    return;
  }
  // https://www.freedesktop.org/wiki/Software/systemd/dbus/
  // possible states
  // active, reloading, inactive, failed, activating, deactivating
  if (strcmp(state, "active") != 0 && strcmp(state, "reloading") != 0) {
    // service is not running
    if (service->failed == 0) {
      WARNING("systemd_stats: read failed %s is in state: %s", service->name, state);
      service->failed = 1;
    }

    sd_bus_error_free(&error);
    free(state);
    return;
  }
  free(state);

  // get value for each metric
  for (int i=0; i<metrics_num; i++) {

    r = sd_bus_get_property_trivial(bus,
      "org.freedesktop.systemd1",
      service->path,
      "org.freedesktop.systemd1.Service",
      metrics_name[i],
      &error,
      't',
      &value);

    if (r < 0) {

      if (service->failed == 0) {
        WARNING("systemd_stats: read failed %s not found: %s %s", service->name, error.name, error.message);
        service->failed = 1;
      }

    } else {

      if (service->failed == 1) {
        service->failed = 0;
      }

      // set returned value
      switch (metrics_type[i]) {
        case DS_TYPE_DERIVE:
          values[i].derive = value;
          break;
        case DS_TYPE_COUNTER:
          values[i].counter = value;
          break;
        case DS_TYPE_GAUGE:
          values[i].gauge = value;
          break;
        case DS_TYPE_ABSOLUTE:
          values[i].absolute = value;
          break;
      }

    }
  }

  // set values properties
  vl.values = values;
  vl.values_len = metrics_num;
  sstrncpy (vl.plugin, "systemd_stats", sizeof (vl.plugin));
  sstrncpy (vl.type, "sd_service_stats", sizeof (vl.type));
  sstrncpy (vl.type_instance, service->name, sizeof(vl.type_instance));
  // send off new values
  plugin_dispatch_values(&vl);
  // clean up
  sd_bus_error_free(&error);
}

// read metrics for all services
static int systemd_stats_read() {
  if (bus == NULL) {
    return -1;
  }

  for (int i=0; i<services_num; i++) {
    read_service(&(services[i]));
  }
  return 0;
}

static int systemd_stats_shutdown() {
  // Close bus
  if (bus != NULL) {
    sd_bus_unref(bus);
    bus = NULL;
  }

  // free service array
  if (services != NULL) {
    for(int i=0; i<services_num; i++) {
      if (services[i].path != NULL) {
        free(services[i].path);
      }
      if (services[i].name != NULL) {
        free(services[i].name);
      }
    }
    free(services);
    services = NULL;
  }
  return 0;
}

void module_register() {
  plugin_register_config ("systemd_stats", systemd_stats_config,
       config_keys, config_keys_num);
  plugin_register_init ("systemd_stats", systemd_stats_init);
  plugin_register_shutdown ("systemd_stats", systemd_stats_shutdown);
  plugin_register_read("systemd_stats", systemd_stats_read);
}
