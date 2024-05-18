#include "src/remote/remote.h"
#include "src/remote/clients/pinecone/pinecone.h"
#include "src/remote/clients/pinecone/pinecone_api.h"
#include "postgres.h"

#include "access/tupdesc.h"
#include "catalog/index.h"

#include <stdio.h>
#include <stdlib.h>

#include <storage/bufmgr.h>
#include "catalog/pg_operator_d.h"
#include "utils/rel.h"
#include "utils/builtins.h"
#include <time.h>
#include "common/hashfn.h"

#include <catalog/index.h>
#include <access/heapam.h>
#include <access/tableam.h>

#include <math.h>

#include "src/remote/cJSON.h"
#include "src/remote/remote.h"  
#include "src/vector.h"
#include "postgres.h" // bool

#include "utils/lsyscache.h" // deconstruct array

const char* vector_metric_to_pinecone_metric[VECTOR_METRIC_COUNT] = {
    "",
    "euclidean",
    "cosine",
    "dotproduct"
};

int extract_dimension(const cJSON *root) {
    const cJSON *dimension = cJSON_GetObjectItemCaseSensitive(root, "dimension");
    if (dimension == NULL || !cJSON_IsNumber(dimension)) {
        fprintf(stderr, "Error: 'dimension' value not found or not a number.\n");
        return -1;
    }
    return dimension->valueint;
}

char* get_index_name(char* host) {
    const char* prefix = "pgvr-";
    const char* start;
    const char* end;
    char* name = NULL;

    // Find the prefix in the URL
    start = strstr(host, prefix);
    if (start == NULL) {
        fprintf(stderr, "Prefix not found in URL.\n");
        return NULL;
    }

    // Find the end of the name
    end = start + strlen(prefix);
    while (isdigit(*end)) {
        end++;
    }

    // Allocate memory for the name
    name = (char*)malloc(end - start + 1);
    if (name == NULL) {
        fprintf(stderr, "Memory allocation failed.\n");
        return NULL;
    }

    // Copy the name into the allocated memory
    strncpy(name, start, end - start);
    name[end - start] = '\0'; // Null-terminate the string

    return name;
}

void pinecone_spec_validator(const char* spec) {
    if (spec == NULL || cJSON_Parse(spec) == NULL) {
        ereport(ERROR,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                 errmsg("Invalid spec"),
                 errhint("Spec should be a valid JSON object e.g. WITH (spec='{\"serverless\":{\"cloud\":\"aws\",\"region\":\"us-west-2\"}}').\n \
                         Refer to https://docs.pinecone.io/reference/create_index")));
    }
}


/* text_array_get_json */
cJSON* text_array_get_json(Datum value) {
    ArrayType *array = DatumGetArrayTypeP(value);
    int nelems = ArrayGetNItems(ARR_NDIM(array), ARR_DIMS(array));
    Datum* elems;
    bool* nulls;
    int16 elmlen;
    bool elmbyval;
    char elmalign;
    Oid elmtype = ARR_ELEMTYPE(array);
    cJSON *json_array = cJSON_CreateArray();

    // get array element type info
    get_typlenbyvalalign(elmtype, &elmlen, &elmbyval, &elmalign);

    // deconstruct array
    deconstruct_array(array, elmtype, elmlen, elmbyval, elmalign, &elems, &nulls, &nelems);

    // copy array elements to json array
    for (int j = 0; j < nelems; j++) {
        if (!nulls[j]) {
            Datum elem = elems[j];
            char* cstr = TextDatumGetCString(elem);
            cJSON_AddItemToArray(json_array, cJSON_CreateString(cstr));
        }
    }
    return json_array;
}
// char* CreateRemoteIndexAndWait(Relation index, cJSON* spec_json, VectorMetric metric, char* remote_index_name, int dimensions) {
        // host = remote_index_interface->create_host_from_spec(dimensions, metric, remote_index_name, spec);
char* pinecone_create_host_from_spec(int dimensions, VectorMetric metric, char* spec, Relation index) {
    char* host = palloc(100);
    const char* remote_metric_name = vector_metric_to_pinecone_metric[metric];
    cJSON* spec_json = cJSON_Parse(spec);
    cJSON *create_response, *index_stats;
    // pgvr-<Oid>
    char* pinecone_index_name = palloc(20);
    sprintf(pinecone_index_name, "pgvr-%u", index->rd_id);
    elog(WARNING,"Pinecone api key passed is %s index name is %s", pinecone_api_key, pinecone_index_name);
    // TODO: remote index name
    create_response = remote_create_index(pinecone_api_key, pinecone_index_name, dimensions, remote_metric_name, spec_json);
    host = cJSON_GetStringValue(cJSON_GetObjectItemCaseSensitive(create_response, "host"));
    // now we wait until the remote index is done initializing
    // todo: timeout and error handling
    // we don't want to ping pinecone.io with describe_index because pinecone.io may be aware of the index before
    // the host is actually live. We poll the host for liveness with get_index_stats instead.
    index_stats = remote_get_index_stats(pinecone_api_key, host);
    while (index_stats == NULL) {
        elog(DEBUG1, "Waiting for remote index to initialize...");
        pg_usleep(1000000); // 1 second
        index_stats = remote_get_index_stats(pinecone_api_key, host);
    }
    cJSON_Delete(index_stats);
    return host;
}

// CREATE AND MISC
void pinecone_validate_host_schema(char* host, int dimensions, VectorMetric metric, Relation index) {
    // TODO: check that the host's schema matches the table
    char* index_name = get_index_name(host);
    cJSON* index_details = describe_index( pinecone_api_key, index_name);
    int pinecone_dimensions = extract_dimension(index_details);
    if(pinecone_dimensions != dimensions){
        elog(ERROR, "Vector Dimension of the local table is %d but the dimension specified in the remote index is %d", dimensions, pinecone_dimensions);
    }
    return;
}

int pinecone_est_network_cost(void) {
    return pinecone_network_cost;
}

int pinecone_count_live(char* host) {
    cJSON* index_stats_response;
    index_stats_response = remote_get_index_stats(pinecone_api_key, host);
    return cJSON_GetObjectItemCaseSensitive(index_stats_response, "totalVectorCount")->valueint;
}

PreparedQuery pinecone_prepare_query(Relation index, ScanKey keys, int nkeys, Vector* vec, int top_k) {
    cJSON* filter = pinecone_build_filter(index, keys, nkeys);
    cJSON* query_vector_values = cJSON_CreateFloatArray(vec->x, vec->dim);
    cJSON* request_body = cJSON_CreateObject();
    cJSON_AddItemToObject(request_body, "topK", cJSON_CreateNumber(top_k));
    cJSON_AddItemToObject(request_body, "vector", query_vector_values);
    cJSON_AddItemToObject(request_body, "filter", filter);
    cJSON_AddItemToObject(request_body, "includeValues", cJSON_CreateFalse());
    cJSON_AddItemToObject(request_body, "includeMetadata", cJSON_CreateFalse());
    return (PreparedQuery) request_body;
}

cJSON* pinecone_build_filter(Relation index, ScanKey keys, int nkeys) {
    cJSON *filter = cJSON_CreateObject();
    cJSON *and_list = cJSON_CreateArray();
    const char* pinecone_filter_operators[] = {"$lt", "$lte", "$eq", "$gte", "$gt", "$ne", "$in"};
    for (int i = 0; i < nkeys; i++)
    {
        cJSON *key_filter = cJSON_CreateObject();
        cJSON *condition = cJSON_CreateObject();
        cJSON *condition_value = NULL;
        FormData_pg_attribute* td = TupleDescAttr(index->rd_att, keys[i].sk_attno - 1);

        if (td->atttypid == TEXTARRAYOID && keys[i].sk_strategy == REMOTE_STRATEGY_ARRAY_CONTAINS) {
            // contains (list_of_strings @> ARRAY[tag1, tag2])
            // $and: [ {list_of_strings: {$in: [tag1]}}, {list_of_strings: {$in: [tag2]}} ]
            cJSON* tags = text_array_get_json(keys[i].sk_argument);
            cJSON* tag;
            cJSON_ArrayForEach(tag, tags) {
                cJSON* condition_contains_tag = cJSON_CreateObject(); // list_of_strings: {$in: [tag1]}
                cJSON* predicate_contains_tag = cJSON_CreateObject(); // {$in: [tag1]}
                cJSON* single_tag_list = cJSON_CreateArray(); // [tag1]
                cJSON_AddItemToArray(single_tag_list, cJSON_Duplicate(tag, true)); // [tag1]
                cJSON_AddItemToObject(predicate_contains_tag, "$in", single_tag_list); // {$in: [tag1]}
                cJSON_AddItemToObject(condition_contains_tag, td->attname.data, predicate_contains_tag); // list_of_strings: {$in: [tag1]}
                cJSON_AddItemToArray(and_list, condition_contains_tag);
            }
            // cJSON_Delete(tags);
        } else {
            switch (td->atttypid)
            {
                case BOOLOID:
                    condition_value = cJSON_CreateBool(DatumGetBool(keys[i].sk_argument));
                    break;
                case FLOAT8OID:
                    condition_value = cJSON_CreateNumber(DatumGetFloat8(keys[i].sk_argument));
                    break;
                case TEXTOID:
                    condition_value = cJSON_CreateString(text_to_cstring(DatumGetTextP(keys[i].sk_argument)));
                    break;
                case TEXTARRAYOID:
                    // overlap
                    if (keys[i].sk_strategy != REMOTE_STRATEGY_ARRAY_OVERLAP) {
                        ereport(ERROR,
                                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                                 errmsg("Unsupported operator for text[] datatype. Must be && (overlap)")));
                    }
                    condition_value = text_array_get_json(keys[i].sk_argument);
                    break;
                    // contains (list_of_strings @> ARRAY[tag1, tag2])
                    // $and: [ {list_of_strings: {$in: [tag1]}}, {list_of_strings: {$in: [tag2]}} ]
                default:
                    ereport(ERROR,
                            (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                             errmsg("Unsupported datatype for remote index scan. Must be one of bool, float8, text, text[]")));
                    break;
            }
            // this only works if all datatypes use the same strategy naming convention. todo: document this
            cJSON_AddItemToObject(condition, pinecone_filter_operators[keys[i].sk_strategy - 1], condition_value);
            cJSON_AddItemToObject(key_filter, td->attname.data, condition);
            cJSON_AddItemToArray(and_list, key_filter);
        }
    }
    cJSON_AddItemToObject(filter, "$and", and_list);
    return filter;
}


ItemPointerData* pinecone_extract_ctids_from_fetch_response(cJSON* fetch_response, int* n_results) {
    cJSON* vectors = cJSON_GetObjectItemCaseSensitive(fetch_response, "vectors");
    cJSON* vector;
    ItemPointerData *results;
    int k = 0;
    *n_results = cJSON_GetArraySize(vectors);
    results = palloc0(sizeof(ItemPointerData) * *n_results); // 0 is an invalid ItemPointerData
    cJSON_ArrayForEach(vector, vectors) {
        char* id_str = vector->string;
        ItemPointerData ctid = pinecone_id_get_heap_tid(id_str);
        results[k++] = ctid;
    }
    return results;
}

cJSON* checkpoints_get_pinecone_ids(RemoteCheckpoint* checkpoints, int n_checkpoints) {
    cJSON* ids = cJSON_CreateArray();
    for (int i = 0; i < n_checkpoints; i++) {
        cJSON* id = cJSON_CreateString(pinecone_id_from_heap_tid(checkpoints[i].tid));
        cJSON_AddItemToArray(ids, id);
    }
    return ids;
}


ItemPointerData* pinecone_query_with_fetch(char* host, int top_k, PreparedQuery query, bool include_vector_ids, RemoteCheckpoint* checkpoints, int n_checkpoints, RemoteCheckpoint* best_checkpoint_return, int* n_remote_tids) {
    cJSON* request_body = (cJSON*) query;
    cJSON* ids = checkpoints_get_pinecone_ids(checkpoints, n_checkpoints);
    cJSON** responses = remote_query_with_fetch(pinecone_api_key, host, request_body, include_vector_ids, ids);
    ItemPointerData *results = palloc0(sizeof(ItemPointerData) * top_k); // 0 is an invalid ItemPointerData
    // find the best checkpoint
    // 1. extract the ctids from the fetch response
    cJSON* fetch_response = responses[1];
    int n_results;
    ItemPointerData* fetched_ctids = pinecone_extract_ctids_from_fetch_response(fetch_response, &n_results);
    // 2. return the best (first) checkpoint which is among the fetched
    for (int i = 0; i < n_checkpoints; i++) {
        RemoteCheckpoint checkpoint = checkpoints[i];
        for (int j = 0; j < n_results; j++) {
            if (ItemPointerEquals(&checkpoint.tid, &fetched_ctids[j])) {
                *best_checkpoint_return = checkpoint;
                break;
            }
        }
    }
    // extract the ctids from the response
    {
        cJSON* query_response = responses[0];
        cJSON* matches = cJSON_GetObjectItemCaseSensitive(query_response, "matches");
        cJSON* match = matches->child;
        int n = 0;
        while (match != NULL) {
            cJSON* vector_id = cJSON_GetObjectItemCaseSensitive(match, "id");
            char* remote_id = cJSON_GetStringValue(vector_id);
            ItemPointerData ctid = pinecone_id_get_heap_tid(remote_id);
            results[n++] = ctid;
            match = match->next;
        }
        *n_remote_tids = n;
        return results;
    }
}

// prepare_bulk_insert
PreparedBulkInsert pinecone_begin_prepare_bulk_insert(Relation index) {
    cJSON* json_vectors = cJSON_CreateArray();
    return (PreparedBulkInsert) json_vectors;
}
void pinecone_append_prepare_bulk_insert(PreparedBulkInsert prepared_vectors, TupleDesc tupdesc, Datum* values, bool* nulls, ItemPointer ctid) {
    cJSON* json_vectors = (cJSON*) prepared_vectors;
    char* vector_id = pinecone_id_from_heap_tid(*ctid);
    cJSON* json_vector = tuple_get_remote_vector(tupdesc, values, nulls, vector_id);
    cJSON_AddItemToArray(json_vectors, json_vector);
}
void pinecone_end_prepare_bulk_insert(PreparedBulkInsert prepared_vectors) {
    return;
}
void pinecone_delete_prepared_bulk_insert(PreparedBulkInsert prepared_vectors) {
    cJSON_Delete((cJSON*) prepared_vectors);
}

bool pinecone_bulk_upsert(char* host, PreparedBulkInsert prepared_vectors,  int n_prepared_tuples) {
    cJSON *json_vectors = (cJSON*) prepared_vectors;
    cJSON *response;
    response = remote_bulk_upsert(pinecone_api_key, host, json_vectors, pinecone_vectors_per_request);
    elog(DEBUG1, "Pinecone bulk upsert response: %s", cJSON_Print(response));
    if (response != NULL && cJSON_GetObjectItemCaseSensitive(response, "upsertedCount") != NULL) {
        return true;
    }
    return false;
}



RemoteIndexInterface pinecone_remote_index_interface = {
    // create index
    .create_host_from_spec = pinecone_create_host_from_spec,
    .validate_host_schema = pinecone_validate_host_schema,
    .count_live = pinecone_count_live,
    .est_network_cost = pinecone_est_network_cost,
    .spec_validator = pinecone_spec_validator,
    // upsert
    .begin_prepare_bulk_insert = pinecone_begin_prepare_bulk_insert,
    .append_prepare_bulk_insert = pinecone_append_prepare_bulk_insert,
    .end_prepare_bulk_insert = pinecone_end_prepare_bulk_insert,
    .delete_prepared_bulk_insert = pinecone_delete_prepared_bulk_insert,
    .bulk_upsert = pinecone_bulk_upsert,
    // query
    .prepare_query = pinecone_prepare_query,
    .query_with_fetch = pinecone_query_with_fetch,
};
