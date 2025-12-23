/*-------------------------------------------------------------------------
 *
 * bitmapattutil.c
 *	Defines the routines to maintain all distinct attribute values
 *	which are indexed in the on-disk bitmap index.
 *
 * Copyright (c) 2007, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  $PostgreSQL$
 *-------------------------------------------------------------------------
 */
#include "postgres.h"
#include "bitmap.h"

#include "access/genam.h"
#include "access/tupdesc.h"
#include "access/nbtree.h"
#include "access/xact.h"
#include "nodes/execnodes.h"
#include "nodes/primnodes.h"
#include "nodes/makefuncs.h"
#include "catalog/dependency.h"
#include "catalog/heap.h"
#include "catalog/index.h"
#include "catalog/pg_type.h"
#include "catalog/namespace.h"
#include "catalog/pg_namespace.h"
#include "access/heapam.h"
#include "optimizer/clauses.h"
#include "utils/syscache.h"
#include "utils/lsyscache.h"
#include "utils/builtins.h"
#include "commands/defrem.h"
#include "commands/tablecmds.h"

static TupleDesc _bitmap_create_lov_heapTupleDesc(Relation rel);

/*
 * _bitmap_create_lov_heapandindex() -- create a new heap relation and
 *	a btree index for the list of values (LOV).
 */

void
_bitmap_create_lov_heapandindex(Relation rel, Oid *lovHeapId, Oid *lovIndexId)
{
	char		lovHeapName[NAMEDATALEN];
	char		lovIndexName[NAMEDATALEN];
	TupleDesc		tupDesc;
	IndexInfo  *indexInfo;
	ObjectAddress		objAddr, referenced;
	Oid		*classObjectId;
	Oid		heapid;
	Oid		indid;
	Oid 	typid;
	int		indattrs;
	int		i;
	List		*indexColNames;
	Relation    lovHeapRel;

	/* create the new names for the new lov heap and index */
	snprintf(lovHeapName, sizeof(lovHeapName), 
			 "pg_bm_%u", RelationGetRelid(rel)); 
	snprintf(lovIndexName, sizeof(lovIndexName), 
			 "pg_bm_%u_index", RelationGetRelid(rel)); 

	/*
	 * If this is happening during re-indexing, then such a heap should
	 * have existed already. Here, we delete this heap and its btree
	 * index first.
	 */
	heapid = get_relname_relid(lovHeapName, PG_BITMAPINDEX_NAMESPACE);
	if (OidIsValid(heapid))
	{
		ObjectAddress object;
		indid = get_relname_relid(lovIndexName, PG_BITMAPINDEX_NAMESPACE);

		Assert(OidIsValid(indid));

		/*
		 * Remove the dependency between the LOV heap relation, 
		 * the LOV index, and the parent bitmap index before 
		 * we drop the lov heap and index.
		 */
		deleteDependencyRecordsFor(RelationRelationId, heapid, false);
		deleteDependencyRecordsFor(RelationRelationId, indid, false);
		CommandCounterIncrement();

		object.classId = RelationRelationId;
		object.objectId = indid;
		object.objectSubId = 0;
		performDeletion(&object, DROP_RESTRICT, 0);

		object.objectId = heapid;
		performDeletion(&object, DROP_RESTRICT, 0);
	}

	/*
	 * create a new empty heap to store all attribute values with their
	 * corresponding block number and offset in LOV.
	 */
	tupDesc = _bitmap_create_lov_heapTupleDesc(rel);

	*lovHeapId = heap_create_with_catalog(lovHeapName, PG_BITMAPINDEX_NAMESPACE,
								 rel->rd_rel->reltablespace, InvalidOid,
								 InvalidOid, InvalidOid, GetUserId(), HEAP_TABLE_AM_OID,
								 tupDesc, NIL, RELKIND_RELATION,
								 rel->rd_rel->relpersistence,
								 rel->rd_rel->relisshared, false, ONCOMMIT_NOOP,
								 (Datum) 0, true, false, true,
								 InvalidOid, NULL);

	/*
	 * We must bump the command counter to make the newly-created relation
	 * tuple visible for opening.
	 */
	CommandCounterIncrement();

	lovHeapRel = table_open(*lovHeapId, AccessShareLock);

	objAddr.classId = RelationRelationId;
	objAddr.objectId = *lovHeapId;
	objAddr.objectSubId = 0 ;

	referenced.classId = RelationRelationId;
	referenced.objectId = RelationGetRelid(rel);
	referenced.objectSubId = 0;

	recordDependencyOn(&objAddr, &referenced, DEPENDENCY_INTERNAL);

	/*
	 * create a btree index on the newly-created heap. 
	 * The key includes all attributes to be indexed in this bitmap index.
	 */
	indattrs = tupDesc->natts - 2;
	indexInfo = makeNode(IndexInfo);
	memset(indexInfo, 0, sizeof(IndexInfo));

	indexInfo->ii_NumIndexAttrs = indattrs;
	indexInfo->ii_NumIndexKeyAttrs = indattrs;
	indexInfo->ii_Unique = true;
	indexInfo->ii_Expressions = NIL;
	indexInfo->ii_ExpressionsState = NIL;
	indexInfo->ii_Predicate = make_ands_implicit(NULL);
	indexInfo->ii_PredicateState = NULL;
	indexInfo->ii_ExclusionOps = NULL;
	indexInfo->ii_ExclusionProcs = NULL;
	indexInfo->ii_ExclusionStrats = NULL;
	indexInfo->ii_ParallelWorkers = 0;

	classObjectId = (Oid *) palloc(indattrs * sizeof(Oid));
	/* Create a list of column names for the LOV index */
	indexColNames = NIL;
	for (i = 0; i < indattrs; i++)
	{
		char *attname;
		indexInfo->ii_IndexAttrNumbers[i] = i + 1;
		typid = TupleDescAttr(tupDesc, i)->atttypid;
		attname = NameStr(TupleDescAttr(tupDesc, i)->attname);
		
		classObjectId[i] = GetDefaultOpClass(typid, BTREE_AM_OID);
		indexColNames = lappend(indexColNames, makeString(pstrdup(attname)));
	}

	/** Use the opened LOV Heap Relation (lovHeapRel) instead of the potentially
     * incorrect index relation (rel) as the base relation for the LOV index.
     * This avoids passing a relkind='i' relation to the start of index_create.
     */
	*lovIndexId = index_create(lovHeapRel, lovIndexName, InvalidOid,
				   InvalidOid, InvalidOid, InvalidOid, indexInfo, 
				   indexColNames, BTREE_AM_OID, 
				   rel->rd_rel->reltablespace, 
				   classObjectId, classObjectId, NULL, NULL, NULL, 
				   (Datum) 0, 0, 0, false, false, NULL);

	table_close(lovHeapRel, AccessShareLock);


	objAddr.classId = RelationRelationId;
	objAddr.objectId = *lovIndexId;
	objAddr.objectSubId = 0 ;

	recordDependencyOn(&objAddr, &referenced, DEPENDENCY_INTERNAL);

	/* Cleanup */
	if (classObjectId) pfree(classObjectId);
	list_free_deep(indexColNames);
	pfree(indexInfo);
}

/*
 * _bitmap_create_lov_heapTupleDesc() -- create the new heap tuple descriptor.
 */

TupleDesc
_bitmap_create_lov_heapTupleDesc(Relation rel)
{
	TupleDesc	tupDesc;
	TupleDesc	oldTupDesc;
	AttrNumber	attno;
	int			natts;

	oldTupDesc = RelationGetDescr(rel);
	natts = oldTupDesc->natts + 2;

	tupDesc = CreateTemplateTupleDesc(natts);

	for (attno = 1; attno <= oldTupDesc->natts; attno++)
	{
		/* copy the attribute to the new tuple descriptor. */
		TupleDescCopyEntry(tupDesc, attno, oldTupDesc, attno);
		/* set the attrelid to InvalidOid to mark it as a non-column attribute. */
		TupleDescAttr(tupDesc, attno - 1)->attrelid = InvalidOid;
		TupleDescAttr(tupDesc, attno - 1)->attnotnull = TupleDescAttr(oldTupDesc, attno - 1)->attnotnull;
		TupleDescAttr(tupDesc, attno - 1)->attisdropped = false;

	}

	/* the block number */
	TupleDescInitEntry(tupDesc, attno, "blockNumber", INT4OID, -1, 0);
	TupleDescAttr(tupDesc, attno - 1)->attnotnull = true;
    TupleDescAttr(tupDesc, attno - 1)->attrelid = InvalidOid;
	attno++;

	/* the offset number */
	TupleDescInitEntry(tupDesc, attno, "offsetNumber", INT4OID, -1, 0);
	TupleDescAttr(tupDesc, attno - 1)->attnotnull = true;
    TupleDescAttr(tupDesc, attno - 1)->attrelid = InvalidOid;

	return tupDesc;
}

/*
 * _bitmap_open_lov_heapandindex() -- open the heap relation and the btree
 *		index for LOV.
 */

void
_bitmap_open_lov_heapandindex(BMMetaPage metapage,
				  Relation *lovHeapP, Relation *lovIndexP,
				  LOCKMODE lockMode)
{
	*lovHeapP = table_open(metapage->bm_lov_heapId, lockMode);
	*lovIndexP = index_open(metapage->bm_lov_indexId, lockMode);
}

/*
 * _bitmap_insert_lov() -- insert a new data into the given heap and index.
 */
void
_bitmap_insert_lov(Relation lovHeap, Relation lovIndex, Datum *datum, 
				   bool *nulls, bool use_wal)
{
	TupleDesc	tupDesc;
	HeapTuple	tuple;
	bool		result;
	Datum	   *indexDatum;
	bool	   *indexNulls;

	tupDesc = RelationGetDescr(lovHeap);

	/* insert this tuple into the heap */
	tuple = heap_form_tuple(tupDesc, datum, nulls);
	heap_insert(lovHeap, tuple, GetCurrentCommandId(true), 0, NULL);

	/* insert a new tuple into the index */
	indexDatum = palloc0((tupDesc->natts - 2) * sizeof(Datum));
	indexNulls = palloc0((tupDesc->natts - 2) * sizeof(bool));
	memcpy(indexDatum, datum, (tupDesc->natts - 2) * sizeof(Datum));
	memcpy(indexNulls, nulls, (tupDesc->natts - 2) * sizeof(bool));
	result = index_insert(lovIndex, indexDatum, indexNulls, 
						  &(tuple->t_self), lovHeap, 
						  UNIQUE_CHECK_YES, false, NULL);

	pfree(indexDatum);
	pfree(indexNulls);
	Assert(result);

	heap_freetuple(tuple);
}


/*
 * _bitmap_close_lov_heapandindex() -- close the heap and the index.
 */
void
_bitmap_close_lov_heapandindex(Relation lovHeap, Relation lovIndex, 
							   LOCKMODE lockMode)
{
	table_close(lovHeap, lockMode);
	index_close(lovIndex, lockMode);
}

/*
 * _bitmap_findvalue() -- find a row in a given heap using
 *  a given index that satisfies the given scan key.
 * 
 * If this value exists, this function returns true. Otherwise,
 * returns false.
 *
 * If this value exists in the heap, this function also returns
 * the block number and the offset number that are stored in the same
 * row with this value. This block number and the offset number
 * are for the LOV item that points the bitmap vector for this value.
 */
bool
_bitmap_findvalue(Relation lovHeap, Relation lovIndex,
				  ScanKey scanKey, IndexScanDesc scanDesc,
				  BlockNumber *lovBlock, bool *blockNull,
				  OffsetNumber *lovOffset, bool *offsetNull)
{
	TupleDesc		tupDesc;
	TupleDesc 		heapTupDesc;
	HeapTuple		tuple;
	bool			found = false;

	TupleTableSlot *slot;
	Datum 		d;

	tupDesc = RelationGetDescr(lovIndex);
	slot = table_slot_create(lovHeap, NULL);

	/* Use the correct API to get the tuple */
	if (index_getnext_slot(scanDesc, ForwardScanDirection, slot))
	{
		tuple = ExecFetchSlotHeapTuple(slot, true, NULL);
		heapTupDesc = RelationGetDescr(lovHeap);

		d = heap_getattr(tuple, tupDesc->natts + 1, heapTupDesc, blockNull);
		*lovBlock =	DatumGetInt32(d);
		d = heap_getattr(tuple, tupDesc->natts + 2, heapTupDesc, offsetNull);
		*lovOffset = DatumGetInt16(d);
		heap_freetuple(tuple);
		found = true;
	}
	ExecDropSingleTupleTableSlot(slot);
	
	return found;
}