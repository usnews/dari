
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <mysql.h>
#include <ctype.h>
#include <jansson.h>
#include <snappy-c.h>

#define KEEP      "@__KEEP__@"
#define KEEP_GLOB "@__KEEP_*_@"
#define DEFAULT_BUFFER_SIZE 1024

#define INCREMENT_BUFFER 20
#define AMOUNT_DECIMAL_PLACES 6
#define AMOUNT_DECIMAL_SHIFT 1000000
#define DATE_DECIMAL_SHIFT 60000
#define DATE_BYTE_SIZE 4
#define AMOUNT_BYTE_SIZE 8

my_bool dari_get_fields_init(UDF_INIT *initid, UDF_ARGS *args, char *message);
void dari_get_fields_deinit(UDF_INIT *initid);
char *dari_get_fields(UDF_INIT *initid, UDF_ARGS *args, char *result,
                      unsigned long *length, char *is_null, char *error);

my_bool dari_get_value_init(UDF_INIT *initid, UDF_ARGS *args, char *message);
void dari_get_value_deinit(UDF_INIT *initid);
char *dari_get_value(UDF_INIT *initid, UDF_ARGS *args, char *result,
                     unsigned long *length, char *is_null, char *error);

my_bool dari_increment_metric_init(UDF_INIT *initid, UDF_ARGS *args, char *message);
void dari_increment_metric_deinit(UDF_INIT *initid);
char *dari_increment_metric(UDF_INIT *initid, UDF_ARGS *args, char *result,
                     unsigned long *length, char *is_null, char *error);

/** 
 * Decode data field into a json_t object, uncompressing from Snappy 
 * if necessary.
 */
static json_t *decode_data(const char *data, size_t length) {
    
    if (!data) {
        return NULL;
    }
    
    json_error_t json_error;
    json_t *json = NULL;
    
    if (*data == 's') {
        size_t output_length;
        if (snappy_uncompressed_length(data + 1, length - 1, &output_length) != SNAPPY_OK) {
            return NULL;
        }
        
        char *output = (char *) malloc(output_length);
        if (snappy_uncompress(data + 1, length - 1, output, &output_length) == SNAPPY_OK) {
            json = json_loadb(output, output_length, 0, &json_error);
        }
        
        free(output);
    } else {
        json = json_loadb(data, length, 0, &json_error);
    }

    if (!json) {
        return NULL;
    }
    
    return json;
}

/**
 * Process the json_t object and mark any objects that should be kept
 * based on the key. Anything not marked will be removed during the
 * filter_keys() call later.
 *
 * If the key uses slash notation, such as "articles/author/firstName"
 * this should return a json_t object that represents the following:
 *
 * {"articles": { "author" : { "firstName": "John" } } }
 *
 */
static int process_key(json_t *json, const char *key) {

    if (!key || json == NULL || !json_is_object(json)) {
        return 1;
    }

    char *token, *ptr;
    char *sep = "/";
    char key_buffer[512];

    snprintf(key_buffer, 512, "%s", key);
    token = strtok_r(key_buffer, sep, &ptr);

    if (!token) {
        return 1;
    }

    // If token is a glob then mark all values so they're saved and move
    // on.
    if (*token == '*') {
        return 1;
    }

    // Fetch the requested key and filter it with the
    // remaining parts of the key path.
    json_t *value = json_object_get(json, token);
    if (value) {
        int glob = process_key(value, ptr);

        // Mark the current key so it won't be removed later by filter_keys().
        char mark_buffer[512];
        if (glob) {
            snprintf(mark_buffer, 512, "%s.%s", token, KEEP_GLOB);
        } else {
            snprintf(mark_buffer, 512, "%s.%s", token, KEEP);
        }

        json_object_set(json, mark_buffer, json_true());
    }

    return 0;
}

/**
 * Process json_t object removing any object that hasn't
 * been marked during a previous call to process_key.
 *
 */
static void filter_keys(json_t *json) {

    if (json == NULL || !json_is_object(json)) {
        return;
    }

    char mark_buffer[512];
    char mark_buffer_glob[512];
    const char *k;
    json_t *v;

    json_object_foreach(json, k, v) {
        snprintf(mark_buffer, 512, "%s.%s", k, KEEP);
        snprintf(mark_buffer_glob, 512, "%s.%s", k, KEEP_GLOB);

        if (!json_object_get(json, mark_buffer) &&
            !json_object_get(json, mark_buffer_glob) &&
            !strstr(k, KEEP) && !strstr(k, KEEP_GLOB)) {
            json_object_del(json, k);
        } else if (!json_object_get(json, mark_buffer_glob)) {
            filter_keys(v);
        }

        json_object_del(json, mark_buffer);
        json_object_del(json, mark_buffer_glob);
    }
}

/**
 * MySQL UDF implementation that parses the 'data' column of a 
 * Dari Record in a json_t object and filters it such that it only
 * contains the keys requested.
 *
 * Usage:
 *
 *    dari_get_fields(data, 'objectOriginals/name')
 *
 * Output:
 * 
 *    {"objectOriginals": {"name": "Blog Main"}}
 */
char *dari_get_fields(UDF_INIT *initid, UDF_ARGS *args, char *result,
                      unsigned long *length, char *is_null, char *error) {

    json_t *json = decode_data((const char *) args->args[0],
                               (size_t) args->lengths[0]);

    if (!json) {
        *error = 1;
        return NULL;
    }

    // Process each requested key.
    int i;
    for (i = 1; i < args->arg_count; i++) {
        process_key(json, args->args[i]);
    }

    // Remove keys that were not marked by a previous call to process_key().
    filter_keys(json);

    char *output = json_dumps(json, 0);
    if (output) {
        *length = strlen(output);

        if (*length > initid->max_length) {
            initid->ptr = realloc(initid->ptr, *length);
        }

        memcpy((void *)initid->ptr, output, *length);
        free(output);
    }

    json_decref(json);

    return initid->ptr;
}

my_bool dari_get_fields_init(UDF_INIT *initid, UDF_ARGS *args, char *message) {

    if (args->arg_count < 2) {
        char *error = "Requires two or more arguments.";
        memcpy(message, error, strlen(error));
        return 1;
    }

    initid->ptr = malloc(DEFAULT_BUFFER_SIZE);
    initid->max_length = DEFAULT_BUFFER_SIZE;

    return 0;
}

void dari_get_fields_deinit(UDF_INIT *initid) {
    free(initid->ptr);
}

/**
 * Traverse the json_t object based on the key and return the resulting
 * json_t value or NULL if not found.
 *
 */
static json_t *get_value(json_t *json, const char *key) {

    if (key == NULL || json == NULL || !json_is_object(json)) {
        return json;
    }

    char *token, *ptr;
    char *sep = "/";
    char key_buffer[512];

    snprintf(key_buffer, 512, "%s", key);
    token = strtok_r(key_buffer, sep, &ptr);

    if (!token) {
        return json;
    }

    json_t *value = json_object_get(json, token);

    return get_value(value, ptr);
}

char *dari_get_value(UDF_INIT *initid, UDF_ARGS *args, char *result,
                     unsigned long *length, char *is_null, char *error) {

    json_t *json = decode_data((const char *) args->args[0],
                               (size_t) args->lengths[0]);

    if (!json) {
        *error = 1;
        return NULL;
    }

    json_t *value = get_value(json, args->args[1]);
    json_incref(value);
    json_decref(json);

    if (value) {
        char *output = NULL;
        if (json_is_string(value)) {
            output = (char *)json_string_value(value);
        } else {
            output = json_dumps(value, JSON_ENCODE_ANY);
        }

        if (output) {
            *length = strlen(output);

            if (*length > initid->max_length) {
                initid->ptr = realloc(initid->ptr, *length);
            }

            memcpy((void *)initid->ptr, output, *length);

            if (!json_is_string(value)) {
                free(output);
            }

            json_decref(value);

            return initid->ptr;
        }

        json_decref(value);
    }

    return NULL;
}

my_bool dari_get_value_init(UDF_INIT *initid, UDF_ARGS *args, char *message) {

    if (args->arg_count != 2) {
        char *error = "Requires two arguments.";
        memcpy(message, error, strlen(error));
        return 1;
    }
    
    initid->ptr = malloc(DEFAULT_BUFFER_SIZE);
    initid->max_length = DEFAULT_BUFFER_SIZE;

    return 0;
}

void dari_get_value_deinit(UDF_INIT *initid) {
    free(initid->ptr);
}

static long amount_from_bytes(const char *bytes, int offset) {
    int i = 0;
    long amount = 0;

    for (i = 0; i < AMOUNT_BYTE_SIZE; ++i) {
        amount = (amount << 8) | (bytes[i + offset] & 0xff);
    }

    return amount;
}

/**
 * MySQL UDF implementation that parses the 'data' column of a 
 * Dari Metric record and increments the values.
 *
 * Internal API.
 *
 * Usage:
 *
 *    dari_increment_metric(data, <cumulative amount>, <interval amount>)
 *    dari_increment_metric(data, 1, 1)
 *
 */
char *dari_increment_metric(UDF_INIT *initid, UDF_ARGS *args, char *result,
                     unsigned long *length, char *is_null, char *error) {

    /* Data is in the format of:
     *
     * Bytes 1-4 is timestamp.
     * Bytes 5-12 is cumulative amount.
     * Bytes 13-20 is interval amount.
     *
     */

    int timestamp;
    long cumulative;
    long interval;

    const char *data = (const char *) args->args[0];
    double cumulative_increment = *((double *) args->args[1]);
    double interval_increment = *((double *) args->args[2]);

    cumulative = amount_from_bytes(data, DATE_BYTE_SIZE);
    interval = amount_from_bytes(data, DATE_BYTE_SIZE + AMOUNT_BYTE_SIZE);

    fprintf(stderr, "%s, %d %ld (%lf) %ld (%lf)\n", 
            data,
            timestamp,
            cumulative, cumulative_increment, 
            interval, interval_increment);

    cumulative += cumulative_increment;
    interval += interval_increment;

    memcpy(initid->ptr, data, 4);

    int i;
    int size = AMOUNT_BYTE_SIZE;
    int offset = 4;
    for (i = 0; i < size; ++i) {
        initid->ptr[i + offset] = (cumulative >> ((size - i - 1) << 3));
    }

    offset += size;
    for (i = 0; i < size; ++i) {
        initid->ptr[i + offset] = (interval >> ((size - i - 1) << 3));
    }

    *length = INCREMENT_BUFFER;

    return initid->ptr;
}

my_bool dari_increment_metric_init(UDF_INIT *initid, UDF_ARGS *args, char *message) {
    if (args->arg_count != 3) {
        char *error = "Requires three arguments.";
        memcpy(message, error, strlen(error));
        return 1;
    }

    args->arg_type[0] = STRING_RESULT;
    args->arg_type[1] = REAL_RESULT;
    args->arg_type[2] = REAL_RESULT;

    initid->ptr = malloc(INCREMENT_BUFFER);
    initid->max_length = INCREMENT_BUFFER;

    return 0;
}

void dari_increment_metric_deinit(UDF_INIT *initid) {
    free(initid->ptr);
}
