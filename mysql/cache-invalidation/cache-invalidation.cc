
#ifndef MYSQL_SERVER
#define MYSQL_SERVER
#endif

#include <stdio.h>
#include <string.h>
#include <ctype.h>
#include <sql_class.h>
#include <mysql/plugin.h>
#include <mysql/plugin_audit.h>
#include <my_global.h>
#include <my_dir.h>
#include <zmq.h>

#if !defined(__attribute__) && (defined(__cplusplus) || !defined(__GNUC__)  || __GNUC__ == 2 && __GNUC_MINOR__ < 8)
#define __attribute__(A)
#endif

#define HEARTBEAT_MS 100 * 1000
#define INPROC_CONNECTION_STRING "inproc://#1"
#define TCP_CONNECTION_STRING "tcp://127.0.0.1:5556"
#define DARI_CF_PREFIX "/* DARI CF "

static void *ctx;
static pthread_t heartbeat_thread;
static pthread_t proxy_thread;
static void *frontend = NULL;
static void *backend = NULL;

static char *to_bytes(const char *uuid) {
    char *bytes = (char *) malloc(16);
    
    char hex[3];
    hex[2] = '\0';

    int j = 0;
    for (int i = 0; i < 32; i += 2) {
        hex[0] = uuid[i];
        hex[1] = uuid[i + 1];

        int val = (int) strtol(hex, NULL, 16); 

        bytes[j] = val;
	j++;
    }

    return bytes;
}

pthread_handler_t cache_heartbeat(void *p) {
	    
    void *socket = zmq_socket(ctx, ZMQ_XPUB);
    int rc = zmq_connect(socket, INPROC_CONNECTION_STRING); 
    if (rc != 0) {
	fprintf(stderr, "Failed to connect to endpoint. %s\n", zmq_strerror(zmq_errno()));
	return 0;
    }

    char buf;
    zmq_recv(socket, &buf, 1, 0);
     
    while (true) {
	rc = zmq_send(socket, "P", 1, 0);
	if (rc == -1) {
	    fprintf(stderr, "Stopping heartbeat thread.\n");
	    break;
	}

	usleep(HEARTBEAT_MS);
    }

    zmq_close(socket);

    return 0;
}

pthread_handler_t cache_proxy(void *p) {
    fprintf(stderr, "Starting cache invalidation proxy thread.\n");

    // Proxy backend publish events to the frontend subscribers.
    zmq_proxy (backend, frontend, NULL);

    fprintf(stderr, "Stopping cache invalidation proxy thread.\n");

    return 0;
}

static int cache_invalidation_plugin_init(void *p) {
    fprintf(stderr, "Starting cache invalidation plugin.\n");
    
    // Create and bind "frontend" socket. This is the socket cache invalidation
    // clients connect to.
    ctx = zmq_ctx_new();
    frontend = zmq_socket(ctx, ZMQ_XPUB);
    int rc = zmq_bind(frontend, "tcp://*:5556");
    if (rc != 0) {
	fprintf(stderr, "Failed to listen on 5556. %s\n", zmq_strerror(zmq_errno()));
	goto failed;
    }
    
    // Create and bind "backend" socket. This is the socket cache notify
    // threads (aka audit plugin mysql threads) connect to.
    backend = zmq_socket(ctx, ZMQ_XSUB);
    rc = zmq_bind(backend, INPROC_CONNECTION_STRING);
    if (rc != 0) {
	fprintf(stderr, "Failed to create endpoint socket.\n");
	goto failed;
    }

    // Create heartbeat thread.
    pthread_attr_t attr;
    pthread_attr_init(&attr);
    pthread_attr_setdetachstate(&attr,
				PTHREAD_CREATE_JOINABLE);
    if (pthread_create(&heartbeat_thread, &attr, cache_heartbeat, (void *)ctx) != 0) {
	fprintf(stderr, "Could not create heartbeat thread!\n");
	goto failed;
    }

    if (pthread_create(&proxy_thread, &attr, cache_proxy, (void *)ctx) != 0) {
	fprintf(stderr, "Could not create proxy thread!\n");
	goto failed;
    }
    
    return 0;

failed:
    if (frontend) zmq_close (frontend);
    if (backend) zmq_close (backend);
    zmq_ctx_destroy (ctx);

    void *dummy_retval;
    pthread_cancel(heartbeat_thread);
    pthread_join(heartbeat_thread, &dummy_retval);

    pthread_cancel(proxy_thread);
    pthread_join(proxy_thread, &dummy_retval);

    return 1;
}

static int cache_invalidation_plugin_deinit(void *p) {
    void *dummy_retval;

    zmq_close (frontend);
    zmq_close (backend);
    zmq_ctx_destroy (ctx);

    pthread_cancel(heartbeat_thread);
    pthread_join(heartbeat_thread, &dummy_retval);

    pthread_cancel(proxy_thread);
    pthread_join(proxy_thread, &dummy_retval);

    return(0);
}

static void cache_invalidation_notify(MYSQL_THD thd,
                              unsigned int event_class,
                              const void *event) {

    const struct mysql_event_general *event_general = 
            (const struct mysql_event_general *) event;

    if (event_general->event_subclass == MYSQL_AUDIT_GENERAL_RESULT) {
        const char *query = event_general->general_query;

	if (event_general->general_query_length >= 11 && 
		strstr(query, DARI_CF_PREFIX)) {
	    const char *object_id = query + 11;

	    // Connect to 'endpoint' and publish this event.
	    void *socket = zmq_socket(ctx, ZMQ_XPUB);
	    int rc = zmq_connect(socket, INPROC_CONNECTION_STRING); 
	    if (rc != 0) {
		fprintf(stderr, "Failed to connect to endpoint. (%d) %s.\n", 
			zmq_errno(), zmq_strerror(zmq_errno()));
	    }

	    char buf;
	    zmq_recv(socket, &buf, 1, 0);

	    char *bytes = to_bytes(object_id);
	    double timestamp = 0.0;
	    sscanf(object_id + 32, "%lf", &timestamp);

	    // Protocol is: "C<16 byte id><8 byte double>"
	    char message[27];
	    message[0] = 'C';               // Cache flush bit.
	    memcpy(&message[1], bytes, 16); // 16byte UUID for object.
	    memcpy(&message[17], (void *)&timestamp, sizeof(timestamp));

	    rc = zmq_send(socket, message, 26, 0);
	    if (rc == -1) {
		fprintf(stderr, "Failed to send cache invalidation. (%d) %s.\n", 
			zmq_errno(), zmq_strerror(zmq_errno()));
	    }

	    zmq_close(socket);
	    free(bytes);
        }
    }
}

static struct st_mysql_audit cache_invalidation_descriptor = {
    MYSQL_AUDIT_INTERFACE_VERSION,
    NULL,
    cache_invalidation_notify,
    { (unsigned long) MYSQL_AUDIT_GENERAL_CLASSMASK |
		      MYSQL_AUDIT_CONNECTION_CLASSMASK }
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
