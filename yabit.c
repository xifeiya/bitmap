#include "postgres.h"
#include "fmgr.h"
#include "utils/rel.h"
#include "utils/lsyscache.h"
#include "access/heapam.h"
#include "access/xact.h"
#include "storage/lmgr.h"
#include "utils/builtins.h"
#include "utils/array.h"
#include "utils/elog.h"
#include "catalog/namespace.h"
#include "miscadmin.h"
#include "access/amapi.h"
#include "bitmap.h"
#include <stdio.h>
#include <stdlib.h>
#include <time.h>

PG_MODULE_MAGIC;

int *read_positions(const char *filename, int *count);
void fetch_tuples(Oid relid, int *positions, int count);

int yabit_debug = 0;

/* Initialize bitmap internal namespace */
Oid bitmap_internal_namespace = InvalidOid;

/* Extension initialization function */
void _PG_init(void)
{
	/* Set the internal namespace to the extension's namespace */
	bitmap_internal_namespace = get_namespace_oid("yabit_internal", true);
	
	/* If extension namespace is not available, fallback to public namespace */
	if (!OidIsValid(bitmap_internal_namespace))
	{
		bitmap_internal_namespace = get_namespace_oid("public", true);
	}
}

/*
 * Bitmap Index Access Method Handler
 */
PG_FUNCTION_INFO_V1(bmhandler);

Datum
bmhandler(PG_FUNCTION_ARGS)
{
    IndexAmRoutine *amroutine = makeNode(IndexAmRoutine);

    /* 设置索引访问方法的基本属性 */
    amroutine->amstrategies = 6;     /* 支持的操作符策略数量 */
    amroutine->amsupport = 1;        /* 支持函数数量 */
    amroutine->amcanorder = false;   /* 不支持按索引顺序扫描 */
    amroutine->amcanorderbyop = false; /* 不支持按操作符排序 */
    amroutine->amcanbackward = true; /* 支持反向扫描 */
    amroutine->amcanunique = false;  /* 不支持唯一索引 */
    amroutine->amcanmulticol = false; /* 不支持多列索引 */
    amroutine->amoptionalkey = true; /* 支持部分键扫描 */
    amroutine->amsearcharray = true; /* 支持数组搜索 */
    amroutine->amsearchnulls = true; /* 支持NULL搜索 */
    amroutine->amstorage = false;    /* 不存储数据 */
    amroutine->ampredlocks = true;   /* 支持谓词锁 */
    amroutine->amcanparallel = false; /* 不支持并行扫描 */
    amroutine->amcaninclude = false; /* 不支持INCLUDE子句 */
    amroutine->amusemaintenanceworkmem = false; /* 不使用maintenance_work_mem */
    amroutine->amparallelvacuumoptions = 0; /* 暂时设置为0，不使用特定的并行清理选项 */
    amroutine->amkeytype = InvalidOid; /* 索引键类型 */

    /* Function pointer for setting index access method */
    amroutine->ambuild = bmbuild_internal;
    amroutine->ambuildempty = bmbuildempty_internal;
    amroutine->aminsert = bminsert_internal;
    // amroutine->aminsertcleanup = bminsertcleanup;
    amroutine->ambulkdelete = bmbulkdelete_internal;
    amroutine->amvacuumcleanup = bmvacuumcleanup_internal;
    amroutine->amcanreturn = NULL; /* 不支持RETURN子句 */
    amroutine->amcostestimate = bmcostestimate_internal;    
    amroutine->amoptions = bmoptions_internal;
    amroutine->amproperty = NULL;
    amroutine->ambuildphasename = NULL;
    amroutine->amvalidate = bmvalidate_internal;
    amroutine->amadjustmembers = NULL;
    amroutine->ambeginscan = bmbeginscan_internal;
    amroutine->amrescan = bmrescan_internal;
    amroutine->amgettuple = NULL;
    amroutine->amgetbitmap = bmgetbitmap_internal;
    amroutine->amendscan = bmendscan_internal;
    amroutine->ammarkpos = NULL;
    amroutine->amrestrpos = NULL;

    /* interface functions to support parallel index scans */
    amroutine->amestimateparallelscan = NULL;
    amroutine->aminitparallelscan = NULL;
    amroutine->amparallelrescan = NULL;
    PG_RETURN_POINTER(amroutine);
}

PG_FUNCTION_INFO_V1(tpch_q6);

Datum tpch_q6(PG_FUNCTION_ARGS) {
    int count;
    int *positions;

    text *table_name_text = PG_GETARG_TEXT_P(0);
    text *file_path_text = PG_GETARG_TEXT_P(1);
    text *debug_info_text = PG_GETARG_TEXT_P(2); 
    char *table_name = text_to_cstring(table_name_text);
    char *file_path = text_to_cstring(file_path_text);
    char *debug_info = text_to_cstring(debug_info_text);

    Oid relid = get_relname_relid(table_name, get_namespace_oid("public", false));

    if (!OidIsValid(relid)) {
        ereport(ERROR, (errmsg("Table \"%s\" does not exist", table_name)));
    }
    elog(INFO, "Evaluating Q6 using Yabit.");

    if (!strcmp(debug_info, "debug")) {
        yabit_debug = 1;
        elog(INFO, "In debug mode.");
    }

    positions = read_positions(file_path, &count);

    fetch_tuples(relid, positions, count);

    PG_RETURN_VOID();
}

int *read_positions(const char *filename, int *count) {
    int *positions;
    int capacity;
    FILE *file;

    file = fopen(filename, "r");
    if (!file) {
        ereport(ERROR, (errmsg("Could not open file: %s", filename)));
    }

    *count = 0;
    capacity = 1000;
    positions = (int *) palloc(capacity * sizeof(int));

    while (fscanf(file, "%d", &positions[*count]) == 1) {
        (*count)++;
        if (*count >= capacity) {
            capacity *= 2;
            if (yabit_debug)  elog(INFO, "Reallocating positions array to %d", capacity);
            positions = (int *) repalloc(positions, capacity * sizeof(int));
        }
    }

    fclose(file);
    return positions;
}

void fetch_tuples(Oid relid, int *positions, int count) {
    double revenue = 0.0;
    struct timespec start, end;
    TupleTableSlot *slot;
    long tuples_processed = 0;
    // For lineitem, each page has 49 tuples. We are using 64 for fast calculation.
    // convert.py script can be used to convert the TID to the page number and tuple number.
    int tuples_per_page = 64; 

    double elapsed_time_ms;
    
    Relation rel = relation_open(relid, AccessShareLock);
    ItemPointerData tid;

    Snapshot snapshot = GetActiveSnapshot();
    TupleDesc tupdesc = RelationGetDescr(rel);

    // Find attribute numbers for l_extendedprice and l_discount
    int extendedprice_attnum = InvalidAttrNumber;
    int discount_attnum = InvalidAttrNumber;
    for (int j = 0; j < tupdesc->natts; j++) {
        Form_pg_attribute attr = TupleDescAttr(tupdesc, j);
        if (strcmp(NameStr(attr->attname), "l_extendedprice") == 0) {
            extendedprice_attnum = j + 1;
        } else if (strcmp(NameStr(attr->attname), "l_discount") == 0) {
            discount_attnum = j + 1;
        }
    }

    if (extendedprice_attnum == InvalidAttrNumber || discount_attnum == InvalidAttrNumber) {
        ereport(ERROR, (errmsg("Attributes l_extendedprice or l_discount not found")));
    }

    elog(INFO, "Fetching tuples from table %s\n", RelationGetRelationName(rel));
    if (yabit_debug)  elog(INFO, "Tuples per page: %d\n", tuples_per_page);

    slot = table_slot_create(rel, NULL);

    clock_gettime(CLOCK_MONOTONIC, &start); // Get the start time

    for (int i = 0; i < count; i++) {
        if (positions[i] < 1) {
            continue; // Skip invalid positions
        }

        ItemPointerSet(&tid, positions[i]/tuples_per_page, positions[i]%tuples_per_page);

        if (yabit_debug) {
            elog(INFO, "Fetching tuple for TID (%u, %u). Position: %d", BlockIdGetBlockNumber(&tid.ip_blkid), tid.ip_posid, positions[i]);
        }

        ExecClearTuple(slot);

        if (table_tuple_fetch_row_version(rel, &tid, snapshot, slot)) {
            HeapTuple tuple = ExecFetchSlotHeapTuple(slot, true, NULL);
            
            bool isnull;

            // Fetch l_shipdate
            Datum extendedprice_val = heap_getattr(tuple, extendedprice_attnum, tupdesc, &isnull);
            double extendedprice = DatumGetFloat8(DirectFunctionCall1(numeric_float8, extendedprice_val));

            // Fetch l_quantity
            Datum discount_val = heap_getattr(tuple, discount_attnum, tupdesc, &isnull);
            double discount = DatumGetFloat8(DirectFunctionCall1(numeric_float8, discount_val));

            if (yabit_debug) {
                elog(INFO, "l_extendedprice: %f, l_discount: %f\n", extendedprice, discount);
            }

            revenue += extendedprice * discount;

            tuples_processed ++;

            heap_freetuple(tuple);
        } else {
            elog(INFO, "Failed to fetch tuple for TID (%u, %u)\n", tid.ip_blkid.bi_lo, tid.ip_posid);
        }
    }

    ExecDropSingleTupleTableSlot(slot);

    clock_gettime(CLOCK_MONOTONIC, &end); // Get the end time
       // Calculate the elapsed time in seconds
    elapsed_time_ms = (end.tv_sec - start.tv_sec) * 1000 + (end.tv_nsec - start.tv_nsec) / 1e6;
    elog(INFO, "Time taken to execute the loop: %f ms.\n", elapsed_time_ms);

    elog(INFO, "Total revenue: %f. Number of tuples processed: %ld\n", revenue, tuples_processed);

    relation_close(rel, AccessShareLock);
}

/* Helper function to convert BM_WORD to binary string representation */
static char* 
word_to_binary(BM_WORD word)
{
    static char buffer[BM_WORD_SIZE + 1 + (BM_WORD_SIZE / 4)]; // 额外空间用于分隔符
    int i;
    char *ptr = buffer;
    
    for (i = BM_WORD_SIZE - 1; i >= 0; i--)
    {
        // 每四位添加一个空格作为分隔符
        if (i != BM_WORD_SIZE - 1 && (i + 1) % 4 == 0)
            *ptr++ = ' ';
        
        *ptr++ = ((word >> i) & 1) ? '1' : '0';
    }
    
    *ptr = '\0';
    return buffer;
}

/* iovitemdetail函数 - 实际上查询的是LOV项的详细信息 */
PG_FUNCTION_INFO_V1(iovitemdetail);
Datum
iovitemdetail(PG_FUNCTION_ARGS)
{
    text *index_name_text = PG_GETARG_TEXT_P(0);
    BlockNumber blockNumber = PG_GETARG_UINT32(1);
    OffsetNumber offsetNumber = (OffsetNumber)PG_GETARG_UINT32(2);
    char *index_name = text_to_cstring(index_name_text);
    Relation bitmap_rel;
    Buffer buffer;
    Page page;
    ItemId itemId;
    BMLOVItemData *lov_item;
    StringInfoData result;
    
    initStringInfo(&result);
    
    /* Open the bitmap index relation */
    bitmap_rel = relation_open(get_relname_relid(index_name, get_namespace_oid("public", false)), AccessShareLock);
    
    if (!bitmap_rel) {
        ereport(ERROR, (errmsg("Index \"%s\" does not exist", index_name)));
    }
    
    /* Read the LOV item from the index file */
    buffer = ReadBuffer(bitmap_rel, blockNumber);
    LockBuffer(buffer, BM_READ);
    page = BufferGetPage(buffer);
    
    itemId = PageGetItemId(page, offsetNumber);
    if (ItemIdIsValid(itemId)) {
        lov_item = (BMLOVItemData *) PageGetItem(page, itemId);
        
        appendStringInfo(&result, "IOV Item Details:\n");
        appendStringInfo(&result, "  Bitmap vector head: %u\n", lov_item->bm_lov_head);
        appendStringInfo(&result, "  Bitmap vector tail: %u\n", lov_item->bm_lov_tail);
        appendStringInfo(&result, "  Last complete word (hex): 0x%04X\n", lov_item->bm_last_compword);
        appendStringInfo(&result, "  Last complete word (binary): %s\n", word_to_binary(lov_item->bm_last_compword));
        appendStringInfo(&result, "  Last word (hex): 0x%04X\n", lov_item->bm_last_word);
        appendStringInfo(&result, "  Last word (binary): %s\n", word_to_binary(lov_item->bm_last_word));
        appendStringInfo(&result, "  Last TID location: %lu\n", lov_item->bm_last_tid_location);
        appendStringInfo(&result, "  Last set bit: %lu\n", lov_item->bm_last_setbit);
        appendStringInfo(&result, "  Words header: 0x%02X\n", lov_item->lov_words_header);
        
        /* Read bitmap vector pages */
        if (lov_item->bm_lov_head != InvalidBlockNumber) {
            BlockNumber bitmap_blkno = lov_item->bm_lov_head;
            BlockNumber bitmap_blkno_end = lov_item->bm_lov_tail;
            
            appendStringInfo(&result, "\nBitmap Vector Pages:\n");
            
            while (bitmap_blkno != InvalidBlockNumber && bitmap_blkno <= bitmap_blkno_end) {
                Buffer bitmap_buffer;
                Page bitmap_page_ptr;
                BMPageOpaque bitmap_opaque;
                BMBitmapVectorPageData *bitmap_data;
                BlockNumber next_blkno;
                int i;
                int used_header_words;
                
                bitmap_buffer = ReadBuffer(bitmap_rel, bitmap_blkno);
                LockBuffer(bitmap_buffer, BM_READ);
                bitmap_page_ptr = BufferGetPage(bitmap_buffer);
                bitmap_opaque = (BMPageOpaque) PageGetSpecialPointer(bitmap_page_ptr);
                bitmap_data = (BMBitmapVectorPageData *) PageGetContents(bitmap_page_ptr);
                
                appendStringInfo(&result, "  Page %u:\n", bitmap_blkno);
                appendStringInfo(&result, "    Words used: %u\n", bitmap_opaque->bm_hrl_words_used);
                appendStringInfo(&result, "    Next page: %u\n", bitmap_opaque->bm_bitmap_next);
                appendStringInfo(&result, "    Last TID location: %lu\n", bitmap_opaque->bm_last_tid_location);
                
                /* Calculate the number of header words actually used */
                used_header_words = BM_CALC_H_WORDS(bitmap_opaque->bm_hrl_words_used);
                
                appendStringInfo(&result, "    Header Words (hex): ");
                
                /* Print only used header words in hex */
                for (i = 0; i < used_header_words; i++) {
                    if (i > 0 && i % 8 == 0) {  /* 每行显示8个word */
                        appendStringInfo(&result, "\n    ");
                    }
                    appendStringInfo(&result, "0x%04X ", bitmap_data->hwords[i]);
                }
                appendStringInfo(&result, "\n");
                
                appendStringInfo(&result, "    Header Words (binary): ");
                
                /* Print only used header words in binary */
                for (i = 0; i < used_header_words; i++) {
                    if (i > 0 && i % 2 == 0) {  /* 每行显示2个word */
                        appendStringInfo(&result, "\n    ");
                    }
                    appendStringInfo(&result, "%s ", word_to_binary(bitmap_data->hwords[i]));
                }
                appendStringInfo(&result, "\n");
                
                appendStringInfo(&result, "    Content Words (hex): ");
                
                /* Print content words in hex */
                for (i = 0; i < bitmap_opaque->bm_hrl_words_used; i++) {
                    if (i > 0 && i % 8 == 0) {  /* 每行显示8个word */
                        appendStringInfo(&result, "\n    ");
                    }
                    appendStringInfo(&result, "0x%04X ", bitmap_data->cwords[i]);
                }
                appendStringInfo(&result, "\n");
                
                appendStringInfo(&result, "    Content Words (binary): ");
                
                /* Print content words in binary, 2 words per line */
                for (i = 0; i < bitmap_opaque->bm_hrl_words_used; i++) {
                    if (i > 0 && i % 2 == 0) {  /* 每行显示2个word */
                        appendStringInfo(&result, "\n    ");
                    }
                    appendStringInfo(&result, "%s ", word_to_binary(bitmap_data->cwords[i]));
                }
                appendStringInfo(&result, "\n");
                
                next_blkno = bitmap_opaque->bm_bitmap_next;
                
                LockBuffer(bitmap_buffer, BUFFER_LOCK_UNLOCK);
                ReleaseBuffer(bitmap_buffer);
                
                bitmap_blkno = next_blkno;
            }
        }
    } else {
        appendStringInfo(&result, "Invalid item ID at block %u, offset %u\n", blockNumber, offsetNumber);
    }
    
    LockBuffer(buffer, BUFFER_LOCK_UNLOCK);
    ReleaseBuffer(buffer);
    relation_close(bitmap_rel, AccessShareLock);
    
    PG_RETURN_TEXT_P(cstring_to_text(result.data));
}