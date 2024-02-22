#include "postgres.h"

#include "catalog/index.h"
#include "access/amapi.h"
#include "vector.h"
#include "pinecone_api.h"
#include "pinecone.h"
#include "cJSON.h"
#include <nodes/execnodes.h>
#include <nodes/pathnodes.h>
#include <utils/array.h>
#include "access/relscan.h"
#include <access/generic_xlog.h>
#include <storage/bufmgr.h>
#include "utils/guc.h"
#include "utils/builtins.h"
#include <access/reloptions.h>
#include <catalog/pg_attribute.h>
#include <unistd.h>
#include "executor/spi.h"
#include "utils/memutils.h"
#include "storage/lmgr.h"
#include "catalog/pg_operator_d.h"
#include "catalog/pg_type_d.h"
#include "lib/pairingheap.h"

#define PINECONE_METAPAGE_BLKNO 0
#define PINECONE_BUFFER_HEAD_BLKNO 1

typedef struct PineconeOptions
{
	int32		vl_len_;		/* varlena header (do not touch directly!) */
    int         spec; // spec is a string; this is its offset in the rd_options
}			PineconeOptions;

char *pinecone_api_key = NULL;
static relopt_kind pinecone_relopt_kind;

void
PineconeInit(void)
{
    pinecone_relopt_kind = add_reloption_kind();
    add_string_reloption(pinecone_relopt_kind, "spec",
                            "Specification of the Pinecone Index. Refer to https://docs.pinecone.io/reference/create_index",
                             "defa",
                            NULL,
                             AccessExclusiveLock);
    // add_int_reloption(pinecone_relopt_kind, "spec", "Pinecone configuration",
                      // 0, 0, 10, AccessExclusiveLock);
    DefineCustomStringVariable("pinecone.api_key",
                              "Pinecone API key",
                              "Pinecone API key",
                              &pinecone_api_key,
                              NULL,
                              PGC_SUSET, // restrict to superusers, takes immediate effect and is not saved in the configuration file 
                              0,
                              NULL,
                              NULL,
                              NULL);
    MarkGUCPrefixReserved("pinecone");
}


IndexBuildResult *pinecone_build(Relation heap, Relation index, IndexInfo *indexInfo)
{
    cJSON *create_response;
    char *spec;
    char *host;
    int dimensions;
    char *pinecone_index_name = (char *) palloc(100);
    cJSON *describe_index_response;
    PineconeOptions *opts = (PineconeOptions *) index->rd_options;
    IndexBuildResult *result = palloc(sizeof(IndexBuildResult));
    // test using spi
    // SPI_connect();
    // SPI_execute("SELECT 1", false, 1);
    // for (int i = 0; i < SPI_processed; i++)
    // {
        // elog(NOTICE, "row %d", i);
    // }
    // SPI_finish();
    // the user specified the column as vector(1536), we need to tell pinecone the dimensionality=1536
    dimensions = TupleDescAttr(index->rd_att, 0)->atttypmod;
    // create a pinecone_index_name like _pgvector_remote_{rd_id}
    snprintf(pinecone_index_name, 100, "pgvector-remote-index-oid-%d", index->rd_id);
    //
    spec = GET_STRING_RELOPTION(opts, spec);
    create_response = create_index(pinecone_api_key, pinecone_index_name, dimensions, "euclidean", spec);
    // log the response host
    host = cJSON_GetStringValue(cJSON_GetObjectItemCaseSensitive(create_response, "host"));
    CreateMetaPage(index, dimensions, host, pinecone_index_name, MAIN_FORKNUM);
    // create buffer
    CreateBufferHead(index, MAIN_FORKNUM);
    // now we need to wait until the pinecone index is done initializing
    sleep(1); // sleep for 1 second to allow pinecone to register the index.
    while (true)
    {
        describe_index_response = describe_index(pinecone_api_key, pinecone_index_name);
        if (cJSON_IsTrue(cJSON_GetObjectItem(cJSON_GetObjectItem(describe_index_response, "status"), "ready")))
        {
            break;
        }
        elog(NOTICE, "Waiting for remote index to initialize...");
        sleep(1);
    }
    result->heap_tuples = 0;
    result->index_tuples = 0;
    return result;
}
void no_buildempty(Relation index){};

#define PineconePageGetOpaque(page)	((PineconeBufferOpaque) PageGetSpecialPointer(page))
#define PineconePageGetMeta(page)	((PineconeMetaPageData *) PageGetContents(page))

void CreateMetaPage(Relation index, int dimensions, char *host, char *pinecone_index_name, int forkNum)
{
    Buffer buf;
    Page page; // a page is a block of memory, formatted as a page
    PineconeMetaPage metap;
    GenericXLogState *state;
    // create a new buffer
    buf = ReadBufferExtended(index, forkNum, P_NEW, RBM_NORMAL, NULL);
    LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE); // lock the buffer in exclusive mode meaning no other process can access it
    // register and initialize the page
    state = GenericXLogStart(index);
    page = GenericXLogRegisterBuffer(state, buf, GENERIC_XLOG_FULL_IMAGE);
    PageInit(page, BufferGetPageSize(buf), 0); // third arg is the sizeof the page's opaque data
    metap = PineconePageGetMeta(page);
    metap->dimensions = dimensions;
    metap->buffer_fullness = 0;
    strcpy(metap->host, host);
    strcpy(metap->pinecone_index_name, pinecone_index_name);
    ((PageHeader) page)->pd_lower = ((char *) metap - (char *) page) + sizeof(PineconeMetaPageData);
    // cleanup
    GenericXLogFinish(state);
    UnlockReleaseBuffer(buf);
}

void incrMetaPageBufferFullness(Relation index)
{
    Buffer buf;
    Page page;
    PineconeMetaPage metap;
    GenericXLogState *state;
    buf = ReadBuffer(index, PINECONE_METAPAGE_BLKNO);
    LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE);
    state = GenericXLogStart(index);
    page = GenericXLogRegisterBuffer(state, buf, 0);
    metap = PineconePageGetMeta(page);
    metap->buffer_fullness++;
    GenericXLogFinish(state);
    UnlockReleaseBuffer(buf);
}

void setMetaPageBufferFullnessZero(Relation index)
{
    Buffer buf;
    Page page;
    PineconeMetaPage metap;
    GenericXLogState *state;
    buf = ReadBuffer(index, PINECONE_METAPAGE_BLKNO);
    LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE);
    state = GenericXLogStart(index);
    page = GenericXLogRegisterBuffer(state, buf, 0);
    metap = PineconePageGetMeta(page);
    metap->buffer_fullness = 0;
    GenericXLogFinish(state);
    UnlockReleaseBuffer(buf);
}

void CreateBufferHead(Relation index, int forkNum)
{
    Buffer buf;
    Page page; // a page is a block of memory, formatted as a page
    GenericXLogState *state;
    // create a new buffer
    buf = ReadBufferExtended(index, forkNum, P_NEW, RBM_NORMAL, NULL);
    LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE); // lock the buffer in exclusive mode meaning no other process can access it
    // register and initialize the page
    state = GenericXLogStart(index);
    page = GenericXLogRegisterBuffer(state, buf, GENERIC_XLOG_FULL_IMAGE);
    PageInit(page, BufferGetPageSize(buf), sizeof(PineconeBufferOpaqueData)); // third arg is the sizeof the page's opaque data
    // initialize the opaque data
    PineconePageGetOpaque(page)->nextblkno = InvalidBlockNumber;
    // cleanup
    GenericXLogFinish(state);
    UnlockReleaseBuffer(buf);
}


PineconeMetaPageData ReadMetaPage(Relation index) {
    Buffer buf;
    Page page;
    PineconeMetaPage metap;
    buf = ReadBuffer(index, PINECONE_METAPAGE_BLKNO);
    LockBuffer(buf, BUFFER_LOCK_SHARE);
    page = BufferGetPage(buf);
    metap = PineconePageGetMeta(page);
    UnlockReleaseBuffer(buf);
    return *metap;
}

void pinecone_buildempty(Relation index)
{
}

/*
 * Insert a tuple into the index
 */
bool pinecone_insert(Relation index, Datum *values, bool *isnull, ItemPointer heap_tid,
                     Relation heap, IndexUniqueCheck checkUnique
#if PG_VERSION_NUM >= 140000
                     ,
                     bool indexUnchanged
#endif
                     ,
                     IndexInfo *indexInfo)
{
    PineconeMetaPageData pinecone_meta;
    cJSON *json_vectors;
    // pinecone_upsert_one(pinecone_api_key, pinecone_meta.host, json_vector);
    // insert into the buffer refer to ivfflatinsert and the InsertTuple function in ivfinsert.c
    InsertBufferTupleMemCtx(index, values, isnull, heap_tid, heap, checkUnique, indexInfo);
    incrMetaPageBufferFullness(index);
    pinecone_meta = ReadMetaPage(index);
    elog(NOTICE, "Buffer fullness: %d", pinecone_meta.buffer_fullness);
    // if the buffer is full, flush it to the remote index
    if (pinecone_meta.buffer_fullness == 4) {
        elog(NOTICE, "Buffer fullness = 10, flushing to remote index");
        json_vectors = get_buffer_pinecone_vectors(index);
        elog(NOTICE, "payload from get_buffer_pinecone_vectors: %s", cJSON_Print(json_vectors));
        pinecone_bulk_upsert(pinecone_api_key, pinecone_meta.host, json_vectors, 2);
        elog(NOTICE, "Buffer flushed to remote index. Now clearing buffer");
        clear_buffer(index);
        setMetaPageBufferFullnessZero(index);
    }
    return false;
}

void clear_buffer(Relation index)
{
    Buffer buf;
    Page page;
    BlockNumber currentblkno = PINECONE_BUFFER_HEAD_BLKNO;
    buf = ReadBuffer(index, currentblkno);
    LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE);
    page = BufferGetPage(buf);
    // iterate through the pages and use indexMultiDelete to delete all the tuples on each page
    while (true)
    {
        // iterate through the tuples
        int nitems = PageGetMaxOffsetNumber(page);
        OffsetNumber *itemnos = palloc(sizeof(OffsetNumber) * nitems);
        for (int i = 1; i <= nitems; i++) {
            itemnos[i-1] = i;
        }
        elog(NOTICE, "deleting %d items", nitems);
        PageIndexMultiDelete(page, itemnos, nitems); // todo this needs to be WALed
        elog(NOTICE, "deleted %d items", nitems);
        // get the next page
        currentblkno = PineconePageGetOpaque(page)->nextblkno;
        if (BlockNumberIsValid(currentblkno))
        {
            // release the current buffer
            UnlockReleaseBuffer(buf);
            // get the next buffer
            buf = ReadBuffer(index, currentblkno);
            LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE);
            page = BufferGetPage(buf);
        }
        else
        {
            break;
        }
    }
    UnlockReleaseBuffer(buf);
}

cJSON* get_buffer_pinecone_vectors(Relation index)
{
    cJSON* json_vectors = cJSON_CreateArray();
    Buffer buf;
    Page page;
    BlockNumber currentblkno = PINECONE_BUFFER_HEAD_BLKNO;
    buf = ReadBuffer(index, currentblkno);
    LockBuffer(buf, BUFFER_LOCK_SHARE);
    page = BufferGetPage(buf);
    // iterate through the pages
    while (true)
    {
        // iterate through the tuples
        for (int i = 1; i <= PageGetMaxOffsetNumber(page); i++)
        {
            IndexTuple itup = (IndexTuple) PageGetItem(page, PageGetItemId(page, i));
            cJSON *json_vector = tuple_get_pinecone_vector(index, itup);
            cJSON_AddItemToArray(json_vectors, json_vector);
        }
        // get the next page
        currentblkno = PineconePageGetOpaque(page)->nextblkno;
        if (BlockNumberIsValid(currentblkno))
        {
            // release the current buffer
            UnlockReleaseBuffer(buf);
            // get the next buffer
            buf = ReadBuffer(index, currentblkno);
            LockBuffer(buf, BUFFER_LOCK_SHARE);
            page = BufferGetPage(buf);
        }
        else
        {
            break;
        }
    }
    UnlockReleaseBuffer(buf);
    return json_vectors;
}

cJSON* tuple_get_pinecone_vector(Relation index, IndexTuple itup)
{
    Datum *itup_values = (Datum *) palloc(sizeof(Datum) * index->rd_att->natts);
    bool *itup_isnull = (bool *) palloc(sizeof(bool) * index->rd_att->natts);
    cJSON *json_vector = cJSON_CreateObject();
    cJSON *metadata = cJSON_CreateObject();
    char vector_id[6 + 1]; // derive the vector_id from the heap_tid
    Vector *vector;
    cJSON *json_values;
    // get the values from the index tuple
    index_deform_tuple(itup, index->rd_att, itup_values, itup_isnull); // set itup_values in place
    vector = DatumGetVector(itup_values[0]);
    json_values = cJSON_CreateFloatArray(vector->x, vector->dim);
    // derive the vector_id from the heap_tid
    snprintf(vector_id, sizeof(vector_id), "%02x%02x%02x", itup->t_tid.ip_blkid.bi_hi, itup->t_tid.ip_blkid.bi_lo, itup->t_tid.ip_posid);
    // prepare metadata
    for (int i = 0; i < index->rd_att->natts; i++)
    {
        // use a macro to get the attribute datatype TupleDescAttr(index->rd_att, i)
        FormData_pg_attribute* td = TupleDescAttr(index->rd_att, i);
        if (td->atttypid == BOOLOID)
        {
            cJSON_AddItemToObject(metadata, td->attname.data, cJSON_CreateBool(DatumGetBool(itup_values[i])));
        } else if (td->atttypid == FLOAT8OID)
        {
            cJSON_AddItemToObject(metadata, td->attname.data, cJSON_CreateNumber(DatumGetFloat8(itup_values[i])));
        } else if (td->atttypid == TEXTOID)
        {
            cJSON_AddItemToObject(metadata, td->attname.data, cJSON_CreateString(TextDatumGetCString(itup_values[i])));
        }
    }
    // add to vector object
    cJSON_AddItemToObject(json_vector, "id", cJSON_CreateString(vector_id));
    cJSON_AddItemToObject(json_vector, "values", json_values);
    cJSON_AddItemToObject(json_vector, "metadata", metadata);
    return json_vector;
}

void InsertBufferTupleMemCtx(Relation index, Datum *values, bool *isnull, ItemPointer heap_tid, Relation heapRel, IndexUniqueCheck checkUnique, IndexInfo *indexInfo)
{
    MemoryContext oldCtx;
    MemoryContext insertCtx;
    insertCtx = AllocSetContextCreate(CurrentMemoryContext,
                                      "Pinecone insert temporary context",
                                      ALLOCSET_DEFAULT_SIZES);
    oldCtx = MemoryContextSwitchTo(insertCtx);
    InsertBufferTuple(index, values, isnull, heap_tid, heapRel);
    MemoryContextSwitchTo(oldCtx);
    MemoryContextDelete(insertCtx); // delete the temporary context
}

void InsertBufferTuple(Relation index, Datum *values, bool *isnull, ItemPointer heap_tid, Relation heapRel)
{
    IndexTuple itup;
    BlockNumber insertPage;
    Size itemsz;
    Buffer buf;
    Page page;
    GenericXLogState *state;
    bool success;
    // detoast the values
    // for (int i = 0; i < index->rd_att->natts; i++)
    // {
        // if (isnull[i]) continue;
        // if (TupleDescAttr(index->rd_att, i)->attlen == -1)
        // {
            // values[i] = PointerGetDatum(PG_DETOAST_DATUM(values[i]));
        // }
    // }
    // form tuple
    itup = index_form_tuple(RelationGetDescr(index), values, isnull);
    itup->t_tid = *heap_tid;
    // find insert page
    insertPage = PINECONE_BUFFER_HEAD_BLKNO;
    // get the size of the tuple
    itemsz = MAXALIGN(IndexTupleSize(itup));
    // look for the first page in the chain which has enough space to fit the tuple
    while (true)
    {
        buf = ReadBuffer(index, insertPage);
        LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE);
        state = GenericXLogStart(index);
        page = GenericXLogRegisterBuffer(state, buf, 0); // register current state of the page
        if (PageGetFreeSpace(page) >= itemsz) break;
        insertPage = PineconePageGetOpaque(page)->nextblkno;
        // if there is no next page, create a new page
        if (BlockNumberIsValid(insertPage))
        {
            // Move to next page
            GenericXLogAbort(state);
            UnlockReleaseBuffer(buf);
        }
        else
        {
            // we hold onto the lock for now because we'll need to update the nextblkno
            Buffer newbuf;
            Page newpage;
            // add a newpage
            LockRelationForExtension(index, ExclusiveLock);
            newbuf = ReadBufferExtended(index, MAIN_FORKNUM, P_NEW, RBM_NORMAL, NULL);
            LockBuffer(newbuf, BUFFER_LOCK_EXCLUSIVE);
            UnlockRelationForExtension(index, ExclusiveLock);
            // init new page
            newpage = GenericXLogRegisterBuffer(state, newbuf, GENERIC_XLOG_FULL_IMAGE);
            PageInit(newpage, BufferGetPageSize(newbuf), sizeof(PineconeBufferOpaqueData));
            PineconePageGetOpaque(newpage)->nextblkno = InvalidBlockNumber;
            // update insert page
            insertPage = BufferGetBlockNumber(newbuf);
            // update previous buffer and commit
            PineconePageGetOpaque(page)->nextblkno = insertPage;
            GenericXLogFinish(state);
            UnlockReleaseBuffer(buf);
            // prepare new buffer
            state = GenericXLogStart(index);
            buf = newbuf;
            page = GenericXLogRegisterBuffer(state, buf, 0);
            break;
        }
    }
    success = PageAddItem(page, (Item) itup, itemsz, InvalidOffsetNumber, false, false);
    if (!success) elog(ERROR, "failed to add item to page");
    GenericXLogFinish(state);
    UnlockReleaseBuffer(buf);
}



IndexBulkDeleteResult *no_bulkdelete(IndexVacuumInfo *info, IndexBulkDeleteResult *stats,
                                     IndexBulkDeleteCallback callback, void *callback_state)
{
    return NULL;
}

IndexBulkDeleteResult *no_vacuumcleanup(IndexVacuumInfo *info, IndexBulkDeleteResult *stats)
{
    return NULL;
}

void
no_costestimate(PlannerInfo *root, IndexPath *path, double loop_count,
					Cost *indexStartupCost, Cost *indexTotalCost,
					Selectivity *indexSelectivity, double *indexCorrelation,
					double *indexPages)
{
    if (list_length(path->indexorderbycols) == 0 || linitial_int(path->indexorderbycols) != 0) {
        elog(NOTICE, "Index must be ordered by the first column");
        *indexTotalCost = 1000000;
        return;
    }
};



bytea * no_options(Datum reloptions, bool validate)
{
	static const relopt_parse_elt tab[] = {
		{"spec", RELOPT_TYPE_STRING, offsetof(PineconeOptions, spec)},
	};
	return (bytea *) build_reloptions(reloptions, validate,
									  pinecone_relopt_kind,
									  sizeof(PineconeOptions),
									  tab, lengthof(tab));
}

bool
no_validate(Oid opclassoid)
{
	return true;
}

/*
 * Prepare for an index scan
 */
IndexScanDesc
pinecone_beginscan(Relation index, int nkeys, int norderbys)
{
	IndexScanDesc scan;
    PineconeScanOpaque so;
    AttrNumber attNums[] = {1}; // sort only on the first column
	Oid			sortOperators[] = {Float8LessOperator};
	Oid			sortCollations[] = {InvalidOid};
	bool		nullsFirstFlags[] = {false};
	scan = RelationGetIndexScan(index, nkeys, norderbys);
    so = (PineconeScanOpaque) palloc(sizeof(PineconeScanOpaqueData));

    // set support functions
    so->procinfo = index_getprocinfo(index, 1, 1); // lookup the first support function in the opclass for the first attribute
    so->collation = index->rd_indcollation[0]; // get the collation of the first attribute

    // create tuple description for sorting
    so->tupdesc = CreateTemplateTupleDesc(2);
    TupleDescInitEntry(so->tupdesc, (AttrNumber) 1, "distance", FLOAT8OID, -1, 0);
    TupleDescInitEntry(so->tupdesc, (AttrNumber) 2, "heaptid", TIDOID, -1, 0);

    // prep sort
    // TODO allocate 10MB for the sort (we should actually need a lot less)
    so->sortstate = tuplesort_begin_heap(so->tupdesc, 1, attNums, sortOperators, sortCollations, nullsFirstFlags, 10000, NULL, false);
    so->slot = MakeSingleTupleTableSlot(so->tupdesc, &TTSOpsMinimalTuple);
    //
    scan->opaque = so;
    // log scan->opaque
    return scan;
}

/*
 * Start or restart an index scan
 */
void
pinecone_rescan(IndexScanDesc scan, ScanKey keys, int nkeys, ScanKey orderbys, int norderbys)
{
    // {"$and": [{"flag": {"$eq": true}}, {"price": {"$lt": 10}}]}  // example filter
	Vector * vec;
	cJSON *query_vector_values;
	cJSON *pinecone_response;
	cJSON *matches;
    Datum query_datum; // query vector
    PineconeMetaPageData pinecone_metadata;
    PineconeScanOpaque so = (PineconeScanOpaque) scan->opaque;
    BlockNumber currentblkno = PINECONE_BUFFER_HEAD_BLKNO;
    TupleTableSlot *slot = MakeSingleTupleTableSlot(so->tupdesc, &TTSOpsVirtual);
    TupleDesc tupdesc = RelationGetDescr(scan->indexRelation); // used for accessing
    // double tuples = 0;
    // filter
    const char* pinecone_filter_operators[] = {"$lt", "$lte", "$eq", "$gte", "$gt", "$ne"};
    cJSON *filter;
    cJSON *and_list;
    // log the metadata
    elog(NOTICE, "nkeys: %d", nkeys);
    pinecone_metadata = ReadMetaPage(scan->indexRelation);    


    if (scan->numberOfOrderBys == 0 || orderbys[0].sk_attno != 1) {
        ereport(ERROR,
                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                 errmsg("Index must be ordered by the first column")));
    }
    
    // build the filter
    filter = cJSON_CreateObject();
    and_list = cJSON_CreateArray();
    // iterate thru the keys and build the filter
    for (int i = 0; i < nkeys; i++)
    {
        cJSON *key_filter = cJSON_CreateObject();
        cJSON *condition = cJSON_CreateObject();
        cJSON *condition_value;
        FormData_pg_attribute* td = TupleDescAttr(scan->indexRelation->rd_att, keys[i].sk_attno - 1);
        elog(NOTICE, "tuple attr %d desc %s", keys[i].sk_attno, td->attname.data);
        if (td->atttypid == BOOLOID)
        {
            condition_value = cJSON_CreateBool(DatumGetBool(keys[i].sk_argument));
        } else if (td->atttypid == FLOAT8OID)
        {
            condition_value = cJSON_CreateNumber(DatumGetFloat8(keys[i].sk_argument));
        } else 
        {
            condition_value = cJSON_CreateString(TextDatumGetCString(keys[i].sk_argument));
        } 
        // this only works if all datatypes use the same strategy naming convention.
        cJSON_AddItemToObject(condition, pinecone_filter_operators[keys[i].sk_strategy - 1], condition_value);
        cJSON_AddItemToObject(key_filter, td->attname.data, condition);
        elog(NOTICE, "key_filter: %s", cJSON_Print(condition));
        cJSON_AddItemToArray(and_list, key_filter);
    }
    cJSON_AddItemToObject(filter, "$and", and_list);
    elog(NOTICE, "filter: %s", cJSON_Print(filter));

	// get the query vector
    query_datum = orderbys[0].sk_argument;
    vec = DatumGetVector(query_datum);
    query_vector_values = cJSON_CreateFloatArray(vec->x, vec->dim);
    pinecone_response = pinecone_api_query_index(pinecone_api_key, pinecone_metadata.host, 10000, query_vector_values, filter);
    elog(NOTICE, "pinecone_response: %s", cJSON_Print(pinecone_response));
    // copy pinecone_response to scan opaque
    // response has a matches array, set opaque to the child of matches aka first match
    matches = cJSON_GetObjectItemCaseSensitive(pinecone_response, "matches");
    so->pinecone_results = matches->child;
    
    // TODO understand these
    /* Count index scan for stats */
    // pgstat_count_index_scan(scan->indexRelation);

    /* Safety check */
    if (scan->orderByData == NULL)
        elog(ERROR, "cannot scan pinecone index without order");

    /* Requires MVCC-compliant snapshot as not able to pin during sorting */
    /* https://www.postgresql.org/docs/current/index-locking.html */
    if (!IsMVCCSnapshot(scan->xs_snapshot))
        elog(ERROR, "non-MVCC snapshots are not supported with pinecone");

    // ADD BUFFER TO THE SORT AND PERFORM THE SORT
    // TODO skip normlizaton for now
    // TODO create the sortstate
    while (BlockNumberIsValid(currentblkno)) {
        Buffer buf;
        Page page;
        Offset maxoffno;
        buf = ReadBuffer(scan->indexRelation, currentblkno); // todo bulkread access method
        LockBuffer(buf, BUFFER_LOCK_SHARE);
        page = BufferGetPage(buf);
        maxoffno = PageGetMaxOffsetNumber(page);
        for (OffsetNumber offno = FirstOffsetNumber; offno <= maxoffno; offno = OffsetNumberNext(offno)) {
            IndexTuple itup;
            Datum datum;
            bool isnull;
            ItemId itemid = PageGetItemId(page, offno);

            itup = (IndexTuple) PageGetItem(page, itemid);
            datum = index_getattr(itup, 1, tupdesc, &isnull);
            if (isnull) elog(ERROR, "distance is null");


            // add the tuples
            ExecClearTuple(slot);
            slot->tts_values[0] = FunctionCall2Coll(so->procinfo, so->collation, datum, query_datum); // compute distance between entry and query
            slot->tts_isnull[0] = false;
            slot->tts_values[1] = ItemPointerGetDatum(&itup->t_tid);
            slot->tts_isnull[1] = false;
            ExecStoreVirtualTuple(slot);

            elog(NOTICE, "adding tuple to sortstate");
            tuplesort_puttupleslot(so->sortstate, slot);
            // log the number of tuples in the sortstate
            // elog(NOTICE, "tuples in sortstate: %d", so->sortstate->memtupcount);
        }

        currentblkno = PineconePageGetOpaque(page)->nextblkno;
        UnlockReleaseBuffer(buf);
    }

    tuplesort_performsort(so->sortstate);
}

/*
 * Fetch the next tuple in the given scan
 */
bool
pinecone_gettuple(IndexScanDesc scan, ScanDirection dir)
{
	// interpret scan->opaque as a cJSON object
	char *id_str;
	ItemPointerData match_heaptid;
    ItemPointer match_heaptid_pointer;
    PineconeScanOpaque so = (PineconeScanOpaque) scan->opaque;
    cJSON *match = so->pinecone_results;
    if (tuplesort_gettupleslot(so->sortstate, true, false, so->slot, NULL)) {
        bool isnull;
        Datum datum;
        elog(NOTICE, "a 2 ✓");
        // show the first slot which is distance double
        datum = slot_getattr(so->slot, 2, &isnull);
        if (isnull) elog(ERROR, "distance is null");
        if (!isnull) elog(NOTICE, "distance: %f", DatumGetFloat8(datum));
        elog(NOTICE, "a 3 ✓");
        match_heaptid_pointer = (ItemPointer) DatumGetPointer(slot_getattr(so->slot, 2, &isnull)); // TODO 
        elog(NOTICE, "a 4 ✓");
        match_heaptid = *match_heaptid_pointer;
        scan->xs_heaptid = match_heaptid;
        scan->xs_recheckorderby = false;
        scan->xs_recheck = true;
        return true;
    }
    return false;


    // OLD PINECONE_GETTUPLE
	if (match == NULL) {
		return false;
	}
	// get the id of the match // interpret the id as a string
	id_str = cJSON_GetStringValue(cJSON_GetObjectItemCaseSensitive(match, "id"));
	sscanf(id_str, "%02hx%02hx%02hx", &match_heaptid.ip_blkid.bi_hi, &match_heaptid.ip_blkid.bi_lo, &match_heaptid.ip_posid);
	scan->xs_recheckorderby = false;
	scan->xs_heaptid = match_heaptid;
	// ItemPointer heaptid;
	// scan->xs_heaptid = ItemPointerFromJson(pinecone_response);
	// NEXT
    elog(NOTICE, "next match: %s", cJSON_Print(match));
    so->pinecone_results = so->pinecone_results->next;
	return true;
}

void no_endscan(IndexScanDesc scan) {};

/*
 * Define index handler
 *
 * See https://www.postgresql.org/docs/current/index-api.html
 */
PGDLLEXPORT PG_FUNCTION_INFO_V1(pineconehandler);
Datum pineconehandler(PG_FUNCTION_ARGS)
{
    IndexAmRoutine *amroutine = makeNode(IndexAmRoutine);

    amroutine->amstrategies = 0;
    amroutine->amsupport = 1; /* number of support functions */
#if PG_VERSION_NUM >= 130000
    amroutine->amoptsprocnum = 0;
#endif
    amroutine->amcanorder = false;
    amroutine->amcanorderbyop = true;
    amroutine->amcanbackward = false; /* can change direction mid-scan */
    amroutine->amcanunique = false;
    amroutine->amcanmulticol = true; /* TODO: pinecone can support filtered search */
    amroutine->amoptionalkey = true;
    amroutine->amsearcharray = false;
    amroutine->amsearchnulls = false;
    amroutine->amstorage = false;
    amroutine->amclusterable = false;
    amroutine->ampredlocks = false;
    amroutine->amcanparallel = false;
    amroutine->amcaninclude = false;
#if PG_VERSION_NUM >= 130000
    amroutine->amusemaintenanceworkmem = false; /* not used during VACUUM */
    amroutine->amparallelvacuumoptions = 0;
#endif
    amroutine->amkeytype = InvalidOid;

    /* Interface functions */
    amroutine->ambuild = pinecone_build;
    amroutine->ambuildempty = pinecone_buildempty;
    amroutine->aminsert = pinecone_insert;
    amroutine->ambulkdelete = no_bulkdelete;
    amroutine->amvacuumcleanup = no_vacuumcleanup;
    // used to indicate if we support index-only scans; takes a attno and returns a bool;
    // included cols should always return true since there is little point in an included column if it can't be returned
    amroutine->amcanreturn = NULL; // do we support index-only scans?
    amroutine->amcostestimate = no_costestimate;
    amroutine->amoptions = no_options;
    amroutine->amproperty = NULL;            /* TODO AMPROP_DISTANCE_ORDERABLE */
    amroutine->ambuildphasename = NULL;      // maps build phase number to name
    amroutine->amvalidate = no_validate; // check that the operator class is valid (provide the opclass's object id)
#if PG_VERSION_NUM >= 140000
    amroutine->amadjustmembers = NULL;
#endif
    amroutine->ambeginscan = pinecone_beginscan;
    amroutine->amrescan = pinecone_rescan;
    amroutine->amgettuple = pinecone_gettuple;
    amroutine->amgetbitmap = NULL; // an alternative to amgettuple that returns a bitmap of matching tuples
    amroutine->amendscan = no_endscan;
    amroutine->ammarkpos = NULL;
    amroutine->amrestrpos = NULL;

    /* Interface functions to support parallel index scans */
    amroutine->amestimateparallelscan = NULL;
    amroutine->aminitparallelscan = NULL;
    amroutine->amparallelrescan = NULL;

    PG_RETURN_POINTER(amroutine);
}
