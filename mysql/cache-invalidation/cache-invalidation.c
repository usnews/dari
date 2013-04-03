
#include <stdio.h>
#include <string.h>
#include <ctype.h>
#include <mysql/plugin.h>
#include <mysql/plugin_audit.h>
#include <zmq.h>

#if !defined(__attribute__) && (defined(__cplusplus) || !defined(__GNUC__)  || __GNUC__ == 2 && __GNUC_MINOR__ < 8)
#define __attribute__(A)
#endif

static void *context;
static void *publisher;

char *to_bytes(char *uuid) {
    char *bytes = (char *)malloc(16);
    
    char hex[3];
    hex[2] = '\0';

    int j = 0;
    for (int i = 0; i < 32; i += 2) {
        hex[0] = uuid[i];
        hex[1] = uuid[i + 1];

        int val = (int) strtol(hex, NULL, 16); 

        bytes[j++] = val;
    }

    return bytes;
}

static int cache_invalidation_plugin_init(void *arg __attribute__((unused))) {
    context = zmq_ctx_new ();
    publisher = zmq_socket (context, ZMQ_PUB);
    int rc = zmq_bind (publisher, "tcp://*:5556");

    if (rc != 0) {
        zmq_close (publisher);
        zmq_ctx_destroy (context);

        return 1;
    }

    return 0;
}

static int cache_invalidation_plugin_deinit(void *arg __attribute__((unused))) {
    zmq_close (publisher);
    zmq_ctx_destroy (context);

    return(0);
}

static void cache_invalidation_notify(MYSQL_THD thd,
                              unsigned int event_class,
                              const void *event) {

    const struct mysql_event_general *event_general = 
            (const struct mysql_event_general *) event;

    if (event_general->event_subclass == MYSQL_AUDIT_GENERAL_RESULT) {
        const char *query = event_general->general_query;
        if (event_general->general_query_length >= 21 &&
            query[0] != 'S' && strstr(query, "RecordUp")) {

            char *object_id = strchr(query, 'X');
            while (object_id && object_id < (query + event_general->general_query_length)) {
                object_id += 2;

                char *bytes = to_bytes(object_id);
                zmq_send (publisher, bytes, 16, 0);
                free(bytes);

                object_id++;
                object_id = strchr(object_id, 'X');
            }
        }
    }
}

static struct st_mysql_audit cache_invalidation_descriptor = {
    MYSQL_AUDIT_INTERFACE_VERSION,
    NULL,
    cache_invalidation_notify,
    { (unsigned long) MYSQL_AUDIT_GENERAL_CLASSMASK }
};

mysql_declare_plugin(cache_invalidation) {
    MYSQL_AUDIT_PLUGIN,
    &cache_invalidation_descriptor,
    "CACHE_INVALIDATION",
    "Perfect Sense Digital, LLC.",
    "Dari Cache Invalidation Plugin",
    PLUGIN_LICENSE_GPL,
    cache_invalidation_plugin_init,
    cache_invalidation_plugin_deinit,
    0x0003,
    NULL,
    NULL,
    NULL,
    0,
}
mysql_declare_plugin_end;

