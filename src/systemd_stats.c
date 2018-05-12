/**
 * collectd - src/systemd-stats.c
 * Foobar
 *
 * Authors:
 *   Alex
 **/

#include "collectd.h"
#include "common.h"
#include "plugin.h"
#include <stdint.h>
#include <systemd/sd-bus.h>

#define METRIC_MEMORY_CURRENT   1
#define METRIC_TASKS_CURRENT    2
#define METRIC_CPU_NSEC         4
#define METRIC_IP_INPACKETS
#define METRIC_IP_INBYTES
#define METRIC_IP_OUTPACKETS
#define METRIC_IP_OUTBYTES

/*
CPUUsageNSec        t
IPEgressBytes       t   
IPEgressPackets     t   
IPIngressBytes      t   
IPIngressPackets    t   
MemoryCurrent       t   
TasksCurrent        t
*/
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
static const char *metrics_short[] =
{
    "cpu",
    "ipoutbytes",
    "ipoutpackets",
    "ipinbytes",
    "ipinpackets",
    "mem",
    "tasks"
};

//#define DS_TYPE_COUNTER 0
//#define DS_TYPE_GAUGE 1
//#define DS_TYPE_DERIVE 2
//#define DS_TYPE_ABSOLUTE 3

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
    char** metrics;
    int metrics_num;
    char* path;
    int failed;
    char* config;
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

static int find_service(struct service *service) {
  int r = 0;
  if (bus == NULL) {
    return -1;
  }
  if (sd_bus_is_open(bus) != 0) {
    r = sd_bus_open_system(&bus);
    if (r < 0) {
      ERROR("systemd_stats: dbus connection failed");
      return -1;
    }
  }

  sd_bus_error error = SD_BUS_ERROR_NULL;
  sd_bus_message *m = NULL;
  char* path = NULL;
/*
busctl --verbose call org.freedesktop.systemd1 /org/freedesktop/systemd1 org.freedesktop.systemd1.Manager GetUnit s "ejabberd.service"
MESSAGE "o" {
        OBJECT_PATH "/org/freedesktop/systemd1/unit/ejabberd_2eservice";
};
*/
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
  r = sd_bus_message_read(m, "o", &path);
  if (r < 0) {
    WARNING("systemd_stats: service obj %s not found: %s %s", service->name, error.name, error.message);
    service->failed = 1;
    return -1;
  }
  service->path = strdup(path);
  printf("check service %s\n", service->path);
  sd_bus_error_free(&error);
  sd_bus_message_unref(m);
  return 0;
}

static struct service *append(struct service *services, int services_num, char **value, int metrics_num, char* config) {
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
  list[services_num-1].name = value[0];
  list[services_num-1].metrics = &(value[1]);
  list[services_num-1].metrics_num = metrics_num;
  list[services_num-1].config = config;
  return list;
}

static int systemd_stats_config (const char *key, const char *value) {
  if (key == 0 || value == 0) {
    return -1;
  }
  if (strncmp(key, "Service", 7) != 0) {
    return -1;
  }
  char* config = strdup(value);
  // split value string into parts
  int parts_cap = 0;
  int i = 0;
  while (config[i] != 0) {
    if (config[i] == ' ' || config[i] == '\r' || config[i] == '\n' || config[i] == '\t') {
      parts_cap++;
    }
    i++;
  }
  parts_cap++;
  char **value_parts = calloc(parts_cap, sizeof(char*));
  int parts_num = strsplit(config, value_parts, parts_cap);
  if (parts_num <= 1) {
    WARNING("systemd_stats: invalid config: \"%s\"", value);
    // invalid
    free(value_parts);
    return -1;
  }
  services = append(services, services_num, value_parts, parts_num-1, config);
  services_num++;
  //int r = find_service(&(services[services_num-1]));
  return 0;
}

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
/*
  r = sd_bus_get_property(bus,
    "org.freedesktop.systemd1",
    "/org/freedesktop/systemd1/unit/ejabberd_2eservice",
    "org.freedesktop.systemd1.Service",
    "MemoryCurrent",
    &error,
    //&msg,
    't',
    &value);
*/
  for (int i=0; i<services_num; i++) {
    find_service(&(services[i]));
  }
    
  return 0;
}

static void read_service(struct service *service) {
  int r = 0;
  uint64_t value = 0;
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

  value_t values[metrics_num];
  value_list_t vl = VALUE_LIST_INIT;

  sd_bus_error error = SD_BUS_ERROR_NULL;
  for (int i=0; i<metrics_num; i++) {
    r = sd_bus_get_property_trivial(bus,
      "org.freedesktop.systemd1",
      service->path,
      "org.freedesktop.systemd1.Service",
//      service->metrics[i],
      metrics_name[i],
      &error,
      't',
      &value);
    if (r < 0) {
      if (service->failed == 0) {
//        printf("Read failed %s %s\n", service->path, service->metrics[i]);
        WARNING("systemd_stats: read failed %s not found: %s %s", service->name, error.name, error.message);
        service->failed = 1;
      }
    } else {
      if (service->failed == 1) {
        service->failed = 0;
      }
//      printf("Read %s %s %d\n", service->path, service->metrics[i], value);
      printf("Read %s %s %d\n", service->path, metrics_name[i], value);
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
  vl.values = values;
  vl.values_len = metrics_num;
  sstrncpy (vl.plugin, "systemd_stats", sizeof (vl.plugin));
  sstrncpy (vl.type, "sd_service_stats", sizeof (vl.type));
  sstrncpy (vl.type_instance, service->name, sizeof(vl.type_instance));
  plugin_dispatch_values(&vl);
}

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
      if (services[i].metrics != NULL) {
        free(&(services[i].metrics[-1]));
      }
      if (services[i].path != NULL) {
        free(services[i].path);
      }
      if (services[i].config != NULL) {
        free(services[i].config);
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
