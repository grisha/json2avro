/*
 * Copyright 2013 Gregory Trubetskoy
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License.  You
 * may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 *
 */

/*
 * TODO
 * Enums
 *
 */

#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include <jansson.h>
#include <avro.h>

#define MAX_SCHEMA_LEN ((off_t) 1024*1024)

char *read_schema_file(char *file_name) {
    FILE *schema_file = fopen(file_name, "rt");
    if (errno != 0) {
        fprintf(stderr, "Could not find or access file: %s\n", file_name);
        return 0;
    }

    // Get file size
    fseek(schema_file, 0, SEEK_END);
    off_t file_size = ftell(schema_file);
    fseek(schema_file, 0, SEEK_SET);

    if (file_size == 0) {
        fprintf(stderr, "Empty schema file: %s\n", file_name);
        return 0;
    }

    if (file_size > MAX_SCHEMA_LEN) {
        fprintf(stderr, "Schema file size is too big: %lld bytes > %lld maximum supported length\n", file_size, MAX_SCHEMA_LEN);
        return 0;
    }

    // Allocate buffer for the schema and read the data
    char *buf = (char*) malloc(file_size);
    fread(buf, 1, file_size, schema_file);
    fclose(schema_file);

    return buf;
}

void memory_status() {
    /* This is obviously Linux-only */
    const char* statm_path = "/proc/self/statm";
    long size, resident, share, text, lib, data, dt;
    FILE *f = fopen(statm_path,"r");
    fscanf(f,"%ld %ld %ld %ld %ld %ld %ld",&size,&resident,&share,&text,&lib,&data,&dt);
    printf("--memory-- sz: %ld res: %ld shr: %ld txt: %ld lib: %ld data: %ld dt: %ld\n", size,resident,share,text,lib,data,dt);
    fclose(f);
}

int schema_traverse(const avro_schema_t schema, json_t *json, json_t *dft,
                    avro_value_t *current_val, int quiet, int strjson, size_t max_str_sz) {

    json = json ? json : dft;
    if (!json) {
        fprintf(stderr, "ERROR: Avro schema does not match JSON\n");
        return 1;
    }

    switch (schema->type) {
    case AVRO_RECORD:
    {
        if (!json_is_object(json)) {
            if (!quiet)
                fprintf(stderr, "ERROR: Expecting JSON object for Avro record, got something else\n");
            return 1;
        }

        int len = avro_schema_record_size(schema), i;
        for (i=0; i<len; i++) {

            const char *name = avro_schema_record_field_name(schema, i);
            avro_schema_t field_schema = avro_schema_record_field_get_by_index(schema, i);

            json_t *json_val = json_object_get(json, name);
            json_t *dft = avro_schema_record_field_default_get_by_index(schema, i);

            avro_value_t field;
            avro_value_get_by_index(current_val, i, &field, NULL);

            if (schema_traverse(field_schema, json_val, dft, &field, quiet, strjson, max_str_sz))
                return 1;
        }
    } break;

    case AVRO_LINK:
        /* TODO */
        fprintf(stderr, "ERROR: AVRO_LINK is not implemented\n");
        return 1;
        break;

    case AVRO_STRING:
        if (!json_is_string(json)) {
            if (json && strjson) {
                /* -j specified, just dump the remaining json as string */
                char * js = json_dumps(json, JSON_COMPACT|JSON_SORT_KEYS|JSON_ENCODE_ANY);
                if (max_str_sz && (strlen(js) > max_str_sz))
                    js[max_str_sz] = 0; /* truncate the string - this will result in invalid JSON! */
                avro_value_set_string(current_val, js);
                free(js);
                break;
            }
            if (!quiet)
                fprintf(stderr, "ERROR: Expecting JSON string for Avro string, got something else\n");
            return 1;
        } else {
            const char *js = json_string_value(json);
            if (max_str_sz && (strlen(js) > max_str_sz)) {
                /* truncate the string */
                char *jst = malloc(strlen(js));
                strcpy(jst, js);
                jst[max_str_sz] = 0;
                avro_value_set_string(current_val, jst);
                free(jst);
            } else
                avro_value_set_string(current_val, js);
        }
        break;

    case AVRO_BYTES:
        if (!json_is_string(json)) {
            if (!quiet)
                fprintf(stderr, "ERROR: Expecting JSON string for Avro string, got something else\n");
            return 1;
        }
        /* NB: Jansson uses null-terminated strings, so embedded nulls are NOT
           supported, not even escaped ones */
        const char *s = json_string_value(json);
        avro_value_set_bytes(current_val, (void *)s, strlen(s));
        break;

    case AVRO_INT32:
        if (!json_is_integer(json)) {
            if (!quiet)
                fprintf(stderr, "ERROR: Expecting JSON integer for Avro int, got something else\n");
            return 1;
        }
        avro_value_set_int(current_val, json_integer_value(json));
        break;

    case AVRO_INT64:
        if (!json_is_integer(json)) {
            if (!quiet)
                fprintf(stderr, "ERROR: Expecting JSON integer for Avro long, got something else\n");
            return 1;
        }
        avro_value_set_long(current_val, json_integer_value(json));
        break;

    case AVRO_FLOAT:
        if (!json_is_real(json)) {
            if (!quiet)
                fprintf(stderr, "ERROR: Expecting JSON real for Avro float, got something else\n");
            return 1;
        }
        avro_value_set_float(current_val, json_real_value(json));
        break;

    case AVRO_DOUBLE:
        if (!json_is_real(json)) {
            if (!quiet)
                fprintf(stderr, "ERROR: Expecting JSON real for Avro double, got something else\n");
            return 1;
        }
        avro_value_set_double(current_val, json_real_value(json));
        break;

    case AVRO_BOOLEAN:
        if (!json_is_boolean(json)) {
            if (!quiet)
                fprintf(stderr, "ERROR: Expecting JSON boolean for Avro boolean, got something else\n");
            return 1;
        }
        avro_value_set_boolean(current_val, json_is_true(json));
        break;

    case AVRO_NULL:
        if (!json_is_null(json)) {
            if (!quiet)
                fprintf(stderr, "ERROR: Expecting JSON null for Avro null, got something else\n");
            return 1;
        }
        avro_value_set_null(current_val);
        break;

    case AVRO_ENUM:
        // TODO ???
        break;

    case AVRO_ARRAY:
        if (!json_is_array(json)) {
            if (!quiet)
                fprintf(stderr, "ERROR: Expecting JSON array for Avro array, got something else\n");
            return 1;
        } else {
            int i, len = json_array_size(json);
            avro_schema_t items = avro_schema_array_items(schema);
            avro_value_t val;
            for (i=0; i<len; i++) {
                avro_value_append(current_val, &val, NULL);
                if (schema_traverse(items, json_array_get(json, i), NULL, &val, quiet, strjson, max_str_sz))
                    return 1;
            }
        }
        break;

    case AVRO_MAP:
        if (!json_is_object(json)) {
            if (!quiet)
                fprintf(stderr, "ERROR: Expecting JSON object for Avro map, got something else\n");
            return 1;
        } else {
            avro_schema_t values = avro_schema_map_values(schema);
            void *iter = json_object_iter(json);
            avro_value_t val;
            while (iter) {
                avro_value_add(current_val, json_object_iter_key(iter), &val, 0, 0);
                if (schema_traverse(values, json_object_iter_value(iter), NULL, &val, quiet, strjson, max_str_sz))
                    return 1;
                iter = json_object_iter_next(json, iter);
            }
        }
        break;

    case AVRO_UNION:
    {
        int i;
        avro_value_t branch;
        for (i=0; i<avro_schema_union_size(schema); i++) {
            avro_value_set_branch(current_val, i, &branch);
            avro_schema_t type = avro_schema_union_branch(schema, i);
            if (!schema_traverse(type, json, NULL, &branch, 1, strjson, max_str_sz))
                break;
        }
        if (i==avro_schema_union_size(schema)) {
            fprintf(stderr, "ERROR: No type in the Avro union matched the JSON type we got\n");
            return 1;
        }
        break;
    }
    case AVRO_FIXED:
        if (!json_is_string(json)) {
            if (!quiet)
                fprintf(stderr, "ERROR: Expecting JSON string for Avro fixed, got something else\n");
            return 1;
        }
        /* NB: Jansson uses null-terminated strings, so embedded nulls are NOT
           supported, not even escaped ones */
        const char *f = json_string_value(json);
        if (!avro_value_set_fixed(current_val, (void *)f, strlen(f))) {
            fprintf(stderr, "ERROR: Setting Avro fixed value FAILED\n");
            return 1;
        }
        break;

    default:
        fprintf(stderr, "ERROR: Unknown type: %d\n", schema->type);
        return 1;
    }
    return 0;
}

void process_file(FILE *input, avro_file_writer_t out, avro_schema_t schema,
                  int verbose, int memstat, int errabort, int strjson, size_t max_str_sz) {

    json_error_t err;
    json_t *json;
    int n = 0;

    json = json_loadf(input, JSON_DISABLE_EOF_CHECK, &err);
    while (!feof(input)) {
        n++;
        if (verbose && !(n % 1000))
            printf("Processing record %d\n", n);
        if (!json) {
            if (errabort) {
                fprintf(stderr, "JSON error on line %d, column %d, pos %d: %s, aborting.\n", n, err.column, err.position, err.text);
                return;
            }
            fprintf(stderr, "JSON error on line %d, column %d, pos %d: %s, skipping to EOL\n", n, err.column, err.position, err.text);
            while (getc(input) != '\n' && !feof(input)) {};
            json = json_loadf(input, JSON_DISABLE_EOF_CHECK, &err);
            continue;
        }

        avro_value_t record;
        avro_value_iface_t *iface = avro_generic_class_from_schema(schema);
        avro_generic_value_new(iface, &record);

        if (!schema_traverse(schema, json, NULL, &record, 0, strjson, max_str_sz)) {

            if (avro_file_writer_append_value(out, &record)) {
                fprintf(stderr, "ERROR: avro_file_writer_append_value() FAILED: %s\n", avro_strerror());
                exit(EXIT_FAILURE);
            }

        } else
            fprintf(stderr, "Error processing record %d, skipping...\n", n);

        avro_value_iface_decref(iface);
        avro_value_decref(&record);

        json_decref(json);
        if (memstat && !(n % 1000))
            memory_status();

        json = json_loadf(input, JSON_DISABLE_EOF_CHECK, &err);
    }

    if (memstat) memory_status();

    avro_schema_decref(schema);
}

void print_help(char *program_name) {
    fprintf(stderr, "Usage: %s [options] [input_file.json] <output_file.avro>\n", program_name);
    fprintf(stderr, "\n");
    fprintf(stderr, "Where options are:\n");
    fprintf(stderr, " -s schema (required) Avro schema to use for conversion.\n");
    fprintf(stderr, " -S file   (required) JSON file to read the avro schema from.\n");
    fprintf(stderr, " -c algo   (optional) Set output compression algorithm: null, snappy, deflate, lzma\n");
    fprintf(stderr, "                      Default: no compression\n");
    fprintf(stderr, " -b bytes  (optional) Set output block size in bytes. Default: 16384\n");
    fprintf(stderr, " -d        (optional) Turn on debug mode.\n");
    fprintf(stderr, " -j        (optional) Dump unexpected JSON objects as strings.\n");
    fprintf(stderr, " -x        (optional) Abort on JSON parsing errors. Default: skip invalid json.\n");
    fprintf(stderr, " -z bytes  (optional) Maximum JSON string size. Default: no limit.\n");
    fprintf(stderr, " -m        (optional) Linux only, enable periodic memory stats information output.\n");
    fprintf(stderr, " -h                   Show this help and exit.\n");
    fprintf(stderr, "\n");
    fprintf(stderr, "If infile.json is not specified, STDIN is assumed. outfile.avro of '-' means STDOUT.\n");
    fprintf(stderr, "\n");
}

void usage_error(char *program_name, char *message) {
    if (message) fprintf(stderr, "ERROR: %s\n\n", message);
    print_help(program_name);
    exit(EXIT_FAILURE);
}

int main(int argc, char *argv[]) {
    FILE *input;

    avro_schema_t schema;
    avro_file_writer_t out;
    const char *key;

    int opt, opterr = 0, verbose = 0, memstat = 0, errabort = 0, strjson = 0;
    char *schema_arg = NULL;
    char *codec = NULL;
    char *endptr = NULL;
    char *outpath = NULL;
    size_t block_sz = 0;
    size_t max_str_sz = 0;
    extern char *optarg;
    extern int optind, optopt;

    while ((opt = getopt(argc, argv, "c:s:S:b:z:dmxjh")) != -1) {
        switch (opt) {
        case 's':
            schema_arg = optarg;
            break;
        case 'S':
            schema_arg = read_schema_file(optarg);
            break;
        case 'b':
            block_sz = strtol(optarg, &endptr, 0);
            if (*endptr) {
                fprintf(stderr, "ERROR: Invalid block size for -b: %s\n", optarg);
                opterr++;
            }
            break;
        case 'z':
            max_str_sz = strtol(optarg, &endptr, 0);
            if (*endptr) {
                fprintf(stderr, "ERROR: Invalid maximum string size for -z: %s\n", optarg);
                opterr++;
            }
            break;
        case 'c':
            codec = optarg;
            break;
        case 'd':
            verbose = 1;
            break;
        case 'x':
            errabort = 1;
            break;
        case 'j':
            strjson = 1;
            break;
        case 'm':
            #if defined(__linux__)
              memstat = 1;
            #else
              usage_error(argv[0], "Memory stats is a Linux-only feature!");
            #endif
            break;
        case 'h':
            print_help(argv[0]);
            exit(0);
        case ':':
            fprintf(stderr, "ERROR: Option -%c requires an operand\n", optopt);
            opterr++;
            break;
        case '?':
            fprintf(stderr, "ERROR: Unrecognized option: -%c\n", optopt);
            opterr++;
        }
    }

    int file_args_cnt = (argc - optind);
    if (file_args_cnt == 0) {
        usage_error(argv[0], "Please provide at least one file name argument");
    }
    if (file_args_cnt > 2) {
        fprintf(stderr, "Too many file name arguments: %d!\n", file_args_cnt);
        usage_error(argv[0], 0);
    }

    if (opterr) usage_error(argv[0], 0);
    if (!schema_arg) usage_error(argv[0], "Please provide correct schema!");

    if (!codec) codec = "null";
    else if (strcmp(codec, "snappy") && strcmp(codec, "deflate") && strcmp(codec, "lzma") && strcmp(codec, "null")) {
        fprintf(stderr, "ERROR: Invalid codec %s, valid codecs: snappy, deflate, lzma, null\n", codec);
        exit(EXIT_FAILURE);
    }

    if ((argc - optind) == 1) {
        input = stdin;
        outpath = argv[optind];
    } else {
        outpath = argv[optind+1];
        input = fopen(argv[optind], "rb");
        if ( errno != 0 ) {
            fprintf(stderr, "ERROR: Cannot open input file: %s: ", argv[optind]);
            perror(0);
            exit(EXIT_FAILURE);
        }
    }

    if (avro_schema_from_json_length(schema_arg, strlen(schema_arg), &schema)) {
        fprintf(stderr, "ERROR: Unable to parse schema: '%s'\n", schema_arg);
        exit(EXIT_FAILURE);
    }

    if (!strcmp(outpath, "-")) {
        if (avro_file_writer_create_with_codec_fp(stdout, outpath, 0, schema, &out, codec, block_sz)) {
            fprintf(stderr, "ERROR: avro_file_writer_create_with_codec_fp FAILED: %s\n", avro_strerror());
            exit(EXIT_FAILURE);
        }

    } else {
        remove(outpath);
        if (avro_file_writer_create_with_codec(outpath, schema, &out, codec, block_sz)) {
            fprintf(stderr, "ERROR: avro_file_writer_create_with_codec FAILED: %s\n", avro_strerror());
            exit(EXIT_FAILURE);
        }
    }

    if (verbose)
        fprintf(stderr, "Using codec: %s\n", codec);

    process_file(input, out, schema, verbose, memstat, errabort, strjson, max_str_sz);

    if (verbose)
        printf("Closing writer....\n");
    avro_file_writer_close(out);
}
