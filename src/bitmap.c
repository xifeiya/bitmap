/*-------------------------------------------------------------------------
 *
 * bitmap.c
 *	Implementation of the Hybrid Run-Length (HRL) on-disk bitmap index.
 *
 * Copyright (c) 2007, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	$PostgreSQL$
 *
 * NOTES
 *	This file contains only the public interface routines.
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"
#include "bitmap.h"

#include "access/genam.h"
#include "access/xact.h"
#include "access/tableam.h"
#include "access/table.h"
#include "access/relscan.h"
#include "catalog/index.h"
#include "utils/rel.h"
#include "miscadmin.h"
#include "nodes/tidbitmap.h"
#include "storage/lmgr.h"
#include "storage/smgr.h"
#include "storage/bulk_write.h"
#include "parser/parse_oper.h"
#include "utils/memutils.h"
#include "storage/bufmgr.h" /* for buffer manager functions */
#include "storage/relfilelocator.h"

static void bmbuildCallback(Relation index,	ItemPointer tid, Datum *attdata,
							bool *nulls, bool tupleIsAlive,	void *state);
static void cleanup_pos(BMScanPosition pos);

/*
 * bmbuild() -- Build a new bitmap index.
 */
IndexBuildResult *
bmbuild_internal(Relation heap, Relation index, IndexInfo *indexInfo)
{
	double      reltuples = 0;
	BMBuildState bmstate;
	IndexBuildResult *result;

	/* Index must be empty when build starts */
	if (RelationGetNumberOfBlocks(index) != 0)
		ereport(ERROR,
				(errcode(ERRCODE_INDEX_CORRUPTED),
				 errmsg("index \"%s\" already contains data",
						RelationGetRelationName(index))));

	/* initialize bitmap index meta page */
	_bitmap_init(index,
				 XLogArchivingActive() && !index->rd_islocaltemp);

	/* init build state */
	_bitmap_init_buildstate(index, &bmstate);

	reltuples = table_index_build_scan(heap,
									  index,
									  indexInfo,
									  false,  /* allow_sync */
									  false,  /* progress */
									  bmbuildCallback,
									  (void *)&bmstate,
									  (TableScanDesc) NULL);

	/* cleanup build state */
	_bitmap_cleanup_buildstate(index, &bmstate);

	/* fsync unless building a local temp index */
	if (!(XLogArchivingActive() && !index->rd_islocaltemp))
	{
		FlushRelationBuffers(bmstate.bm_lov_heap);
		smgrimmedsync(bmstate.bm_lov_heap->rd_smgr, MAIN_FORKNUM);

		FlushRelationBuffers(bmstate.bm_lov_index);
		smgrimmedsync(bmstate.bm_lov_index->rd_smgr, MAIN_FORKNUM);

		FlushRelationBuffers(index);
		smgrimmedsync(index->rd_smgr, MAIN_FORKNUM);
	}

	/* return stats */
	result = (IndexBuildResult *) palloc(sizeof(IndexBuildResult));
	result->heap_tuples = reltuples;
	result->index_tuples = bmstate.ituples;

	return result;
}

/*
 * bmbuildempty() -- Build an empty bitmap index in the initialization fork
 */
void
bmbuildempty_internal(Relation index)
{
    BulkWriteState *bulkstate;
    BulkWriteBuffer metabuf;
    BulkWriteBuffer lovbuf;
    Page metapage;
    Page lovpage;
    BMPageOpaque opaque;
    BMMetaPage  bm_metapage;

    /* Ensure the storage manager handle is opened */
    RelationGetSmgr(index);
    
    /* 1. Start bulk write operation on the initialization fork */
    bulkstate = smgr_bulk_start_rel(index, INIT_FORKNUM);

    /* 2. Construct and write Meta Page (Block 0) */
    metabuf = smgr_bulk_get_buf(bulkstate);
    metapage = (Page) metabuf; 

    /* Initialize page header and special areas */
    PageInit(metapage, BLCKSZ, sizeof(BMPageOpaqueData));
    
    /* Set Opaque Data */
    opaque = (BMPageOpaque) PageGetSpecialPointer(metapage);
    opaque->bm_hrl_words_used = 0;
    opaque->bm_bitmap_next = InvalidBlockNumber;
    opaque->bm_last_tid_location = 0;
    opaque->bm_page_id = BM_PAGE_ID;

    /* Set Meta Data */
    bm_metapage = (BMMetaPage) PageGetContents(metapage);
    bm_metapage->bm_lov_heapId = InvalidOid; 
    bm_metapage->bm_lov_indexId = InvalidOid;
    bm_metapage->bm_lov_lastpage = BM_LOV_STARTPAGE; // Point to Block 1

    /* Write Meta Page to Block 0 */
    smgr_bulk_write(bulkstate, BM_METAPAGE, metabuf, true);

    /* 3. Build and Write the First LOV Page (Block 1) */
    lovbuf = smgr_bulk_get_buf(bulkstate);
    lovpage = (Page) lovbuf; 

    PageInit(lovpage, BLCKSZ, 0); 
    
    /* Add the first empty LOV item (corresponding to a NULL value) */
    {
        BMLOVItem lovItem = _bitmap_formitem(0); // 假设 _bitmap_formitem(0) 已定义
        OffsetNumber off;
        
        off = PageAddItem(lovpage, (Item)lovItem, sizeof(BMLOVItemData),
                          InvalidOffsetNumber, false, false);
        if (off == InvalidOffsetNumber)
             elog(ERROR, "failed to add NULL LOV item in buildempty");
        
        pfree(lovItem);
    }

    /* Write LOV Page to Block 1 */
    smgr_bulk_write(bulkstate, BM_LOV_STARTPAGE, lovbuf, true);

    /* 4. Finish the bulk write operation */
    smgr_bulk_finish(bulkstate);
}


/*
 * bminsert() -- insert an index tuple into a bitmap index.
 */
bool
bminsert_internal(Relation indexRelation,
		 Datum *values,
		 bool *isnull,
		 ItemPointer heap_tid,
		 Relation heapRelation,
		 IndexUniqueCheck checkUnique,
		 bool indexUnchanged,
		 IndexInfo *indexInfo)
{
	/* indexRelation is the index rel in which to insert */
	_bitmap_doinsert(indexRelation, *heap_tid, values, isnull);
	return true;
}

/* 
 * bminsertcleanup() -- clean up after insertions
 */
void
bminsertcleanup_internal(Relation index, IndexInfo *indexInfo)
{
    /**
	 * The current implementation seems to write directly to the buffer with each insert, without any persistent state,
	 * so this can be left empty here.
	 */
}

/*
 * bmgettuple() -- return the next tuple in a scan.
 */
Datum
bmgettuple_internal(PG_FUNCTION_ARGS)
{
	IndexScanDesc scan = (IndexScanDesc) PG_GETARG_POINTER(0);
	ScanDirection dir = (ScanDirection) PG_GETARG_INT32(1);
	BMScanOpaque  so = (BMScanOpaque)scan->opaque;

	bool res;

	/* 
	 * If we have already begun our scan, continue in the same direction.
	 * Otherwise, start up the scan.
	 */
	if (so->bm_currPos && so->cur_pos_valid)
		res = _bitmap_next(scan, dir);
	else
		res = _bitmap_first(scan, dir);

	PG_RETURN_BOOL(res);
}


/*
 * bmgetbitmap() -- gets all matching tuples, and adds them to a bitmap
 */
long
bmgetbitmap_internal(IndexScanDesc scan, TIDBitmap *tbm)
{
    int64 ntids = 0;
    ItemPointer heapTid;

    /* Fetch the first tuple */
    if (!_bitmap_first(scan, ForwardScanDirection))
    {
        elog(NOTICE, "=bmgetbitmap_internal: no tuples found");
        PG_RETURN_INT64(0);
    }

    /* Save TID */
    heapTid = &scan->xs_heaptid;
    tbm_add_tuples(tbm, heapTid, 1, false);
    ntids++;

    /* Iterate next tuples */
    while (_bitmap_next(scan, ForwardScanDirection))
    {
        heapTid = &scan->xs_heaptid;
        tbm_add_tuples(tbm, heapTid, 1, false);
        ntids++;
    }

    elog(NOTICE, "=bmgetbitmap_internal: added %ld tuples to bitmap, total size = %ld bytes", ntids, ntids * sizeof(ItemPointerData));
    return ntids;
}


/*
 * bmbeginscan() -- start a scan on the bitmap index.
 */
IndexScanDesc
bmbeginscan_internal(Relation indexRelation, int nkeys, int norderbys)
{
	IndexScanDesc scan;
	BMScanOpaque so;
	MemoryContext oldcxt;

	scan = RelationGetIndexScan(indexRelation, nkeys, norderbys);

	// Allocate three seconds of space in the current memory context
	so = (BMScanOpaque) palloc0(sizeof(BMScanOpaqueData));

	// Create a memory context for scanning
	so->scanMemoryContext =
        AllocSetContextCreate(
            CurrentMemoryContext,
            "BitmapIndexScanContext",
            ALLOCSET_DEFAULT_SIZES);
	
	// Switch to scan context
	oldcxt = MemoryContextSwitchTo(so->scanMemoryContext);

	// Initialize custom scan status
	so->bm_currPos = NULL;
	so->bm_markPos = NULL;
	so->cur_pos_valid = false;
	so->mark_pos_valid = false;

	// Restore the original memory context
	MemoryContextSwitchTo(oldcxt);

	scan->opaque = so;

	return scan;
}


/*
 * bmrescan() -- restart a scan on the bitmap index.
 */
void
bmrescan_internal(IndexScanDesc scan, ScanKey scankey, int nscankeys,
         ScanKey orderbys, int norderbys)
{
	BMScanOpaque	so = (BMScanOpaque) scan->opaque;

	if (!so || !so->scanMemoryContext)
        elog(ERROR, "bmrescan called without scan context");

	MemoryContextReset(so->scanMemoryContext);

    so->bm_currPos = NULL;
    so->bm_markPos = NULL;
    so->cur_pos_valid = false;
    so->mark_pos_valid = false;

    /* reset scankey */
    if (scankey && scan->numberOfKeys > 0)
        memmove(scan->keyData,
                scankey,
                scan->numberOfKeys * sizeof(ScanKeyData));
}

/*
 * bmendscan() -- close a scan.
 */
void
bmendscan_internal(IndexScanDesc scan)
{
	uint32 keyNo;
	BMScanOpaque	so = (BMScanOpaque) scan->opaque;

	if (so == NULL)
        return;

    /*
     * Do only one thing: delete the scan private context
     * All posvecs / batchWords / hwords / cwords
     * Will be released safely and all at once
     */

	for (keyNo=0; keyNo<so->bm_currPos->nvec; keyNo++)
	{
		if (BufferIsValid((so->bm_currPos->posvecs[keyNo]).bm_lovBuffer))
		{
			ReleaseBuffer((so->bm_currPos->posvecs[keyNo]).bm_lovBuffer);
		}
	}
    if (so->scanMemoryContext)
        MemoryContextDelete(so->scanMemoryContext);

    pfree(so);
    scan->opaque = NULL;
}

/*
 * bmmarkpos() -- save the current scan position.
 */
Datum
bmmarkpos_internal(PG_FUNCTION_ARGS)
{
	IndexScanDesc	scan = (IndexScanDesc) PG_GETARG_POINTER(0);
	BMScanOpaque	so = (BMScanOpaque) scan->opaque;
	BMVector	bmScanPos;
	uint32 vectorNo;

	/* free the space */
	if (so->mark_pos_valid)
	{
		/*
		 * release the buffers that have been stored for each
		 * related bitmap.
		 */
		bmScanPos = so->bm_markPos->posvecs;

		for (vectorNo=0; vectorNo < so->bm_markPos->nvec; vectorNo++)
		{
			if (BufferIsValid((bmScanPos[vectorNo]).bm_lovBuffer))
			{
				ReleaseBuffer((bmScanPos[vectorNo]).bm_lovBuffer);
				(bmScanPos[vectorNo]).bm_lovBuffer = InvalidBuffer;
			}
		}
		so->mark_pos_valid = false;
	}

	if (so->cur_pos_valid)
	{
		uint32	size = sizeof(BMScanPositionData);
		bool need_init = false;

		/* set the mark position */
		if (so->bm_markPos == NULL)
		{
			so->bm_markPos = (BMScanPosition) palloc(size);
			so->bm_markPos->posvecs = 
				(BMVector)palloc0(so->bm_currPos->nvec * sizeof(BMVectorData));
			need_init = true;
		}

		bmScanPos = so->bm_currPos->posvecs;

		for (vectorNo = 0; vectorNo < so->bm_currPos->nvec; vectorNo++)
		{
			BMVector p = &(so->bm_markPos->posvecs[vectorNo]);
			
			if (BufferIsValid((bmScanPos[vectorNo]).bm_lovBuffer))
				IncrBufferRefCount((bmScanPos[vectorNo]).bm_lovBuffer);

			if (need_init)
			{
				p->bm_batchWords = 
					(BMBatchWords *) palloc0(sizeof(BMBatchWords));
				_bitmap_init_batchwords(p->bm_batchWords,
										BM_NUM_OF_HRL_WORDS_PER_PAGE,
										CurrentMemoryContext);
			}
		}

		if (so->bm_currPos->nvec == 1)
		{
			so->bm_markPos->bm_batchWords = 
				so->bm_markPos->posvecs->bm_batchWords;
		}

		memcpy(so->bm_markPos->posvecs, bmScanPos,
			   so->bm_currPos->nvec *
			   sizeof(BMVectorData));
		memcpy(so->bm_markPos, so->bm_currPos, size);

		so->mark_pos_valid = true;
	}

	PG_RETURN_VOID();
}

/*
 * bmrestrpos() -- restore a scan to the last saved position.
 */
Datum
bmrestrpos_internal(PG_FUNCTION_ARGS)
{
	IndexScanDesc	scan = (IndexScanDesc) PG_GETARG_POINTER(0);
	BMScanOpaque	so = (BMScanOpaque) scan->opaque;

	BMVector	bmScanPos;
	uint32 vectorNo;

	/* free space */
	if (so->cur_pos_valid)
	{
		/* release the buffers that have been stored for each related bitmap.*/
		bmScanPos = so->bm_currPos->posvecs;

		for (vectorNo=0; vectorNo<so->bm_markPos->nvec;
			 vectorNo++)
		{
			if (BufferIsValid((bmScanPos[vectorNo]).bm_lovBuffer))
			{
				ReleaseBuffer((bmScanPos[vectorNo]).bm_lovBuffer);
				(bmScanPos[vectorNo]).bm_lovBuffer = InvalidBuffer;
			}
		}
		so->cur_pos_valid = false;
	}

	if (so->mark_pos_valid)
	{
		uint32	size = sizeof(BMScanPositionData);

		/* set the current position */
		if (so->bm_currPos == NULL)
		{
			so->bm_currPos = (BMScanPosition) palloc(size);
		}

		bmScanPos = so->bm_markPos->posvecs;

		for (vectorNo=0; vectorNo<so->bm_currPos->nvec;
			 vectorNo++)
		{
			if (BufferIsValid((bmScanPos[vectorNo]).bm_lovBuffer))
				IncrBufferRefCount((bmScanPos[vectorNo]).bm_lovBuffer);
		}		

		memcpy(so->bm_currPos->posvecs, bmScanPos,
			   so->bm_markPos->nvec *
			   sizeof(BMVectorData));
		memcpy(so->bm_currPos, so->bm_markPos, size);
		so->cur_pos_valid = true;
	}

	PG_RETURN_VOID();
}

/*
 * bmbulkdelete() -- bulk delete index entries
 *
 * Re-index is performed before retrieving the number of tuples
 * indexed in this index.
 */
IndexBulkDeleteResult *
bmbulkdelete_internal(IndexVacuumInfo *info, IndexBulkDeleteResult *stats,
             IndexBulkDeleteCallback callback, void *callback_state)
{
	Relation    rel = info->index;            

	/* allocate stats if first time through, else re-use existing struct */
	if (stats == NULL)
		stats = (IndexBulkDeleteResult *)
			palloc0(sizeof(IndexBulkDeleteResult));

	stats = (IndexBulkDeleteResult *) palloc0(sizeof(IndexBulkDeleteResult));

	_bitmap_vacuum(info, stats, callback, callback_state);
    
	stats->num_pages = RelationGetNumberOfBlocks(rel);
	/* Since we re-build the index, set this to number of heap tuples. */
	stats->num_index_tuples = info->num_heap_tuples;
	stats->tuples_removed = 0;
    
	return stats;
}

/*
 * bmvacuumcleanup() -- post-vacuum cleanup.
 *
 * We do nothing useful here.
 */
IndexBulkDeleteResult *
bmvacuumcleanup_internal(IndexVacuumInfo *info, IndexBulkDeleteResult *stats)
{
	Relation    rel = info->index;

	if (stats == NULL)
		stats = (IndexBulkDeleteResult *) palloc0(sizeof(IndexBulkDeleteResult));

	/* update statistics */
	stats->num_pages = RelationGetNumberOfBlocks(rel);
	stats->pages_deleted = 0;
	stats->pages_free = 0;
	/* XXX: dodgy hack to shutup index_scan() and vacuum_index() */
	stats->num_index_tuples = info->num_heap_tuples;

	return stats;
}

/*
 * Per-tuple callback from IndexBuildHeapScan
 */
static void
bmbuildCallback(Relation index, ItemPointer tid, Datum *attdata,
				bool *nulls, bool tupleIsAlive,	void *state)
{
    BMBuildState *bstate = (BMBuildState *) state;

#ifdef DEBUG_BMI
    elog(NOTICE,"[bmbuildCallback] BEGIN");
#endif

    _bitmap_buildinsert(index, tid, attdata, nulls, bstate);
    ++bstate->ituples;

#ifdef DEBUG_BMI
    elog(NOTICE,"[bmbuildCallback] END");
#endif
}

/* NOTA: la prossima funzione è stata commentata in quanto probabilmente obsoleta */
#if 0

/*
 * free the memory associated with the stream
 */

static void
stream_free(void *opaque)
{
	IndexStream *is = (IndexStream *)opaque;

	if (is)
	{
		BMStreamOpaque *so = (BMStreamOpaque *)is->opaque;

		if (so)
			pfree(so);
		pfree(is);
	}
}

#endif

static void
cleanup_pos(BMScanPosition pos) 
{
	if (pos->nvec == 0)
		return;
	
	/*
	 * Only cleanup bm_batchWords if we have more than one vector since
	 * _bitmap_cleanup_scanpos() will clean it up for the single vector
	 * case.
	 */
	if (pos->nvec > 1)
		 _bitmap_cleanup_batchwords(pos->bm_batchWords);
	_bitmap_cleanup_scanpos(pos->posvecs, pos->nvec);
}