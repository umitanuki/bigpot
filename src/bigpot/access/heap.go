package access

import (
	"fmt"
	"os"

	"bigpot/storage"
	"bigpot/system"
)

type HeapRelation struct {
	RelId   system.Oid
	RelName system.Name
	RelDesc *TupleDesc
	RelNode system.RelFileNode
}

type HeapScan struct {
	rel        *HeapRelation
	Forward    bool
	ScanKeys   []ScanKey // non-pointer, as usually this is short life
	inited     bool
	nBlocks    system.BlockNumber
	startBlock system.BlockNumber
	bufMgr     storage.BufferManager
	// currently scanning buffer
	cBuf storage.Buffer
	// currently scanning block
	cBlock system.BlockNumber
	// currently scanning tuple
	cTuple *HeapTuple
}

func (rel *HeapRelation) initRelFileNode() {
	rel.RelNode.Dbid = 1 // TODO
	rel.RelNode.Tsid = system.DefaultTableSpaceOid
	rel.RelNode.Relid = rel.RelId
}

func HeapOpen(relid system.Oid, bufMgr storage.BufferManager) (*HeapRelation, error) {
	if relid == ClassRelId {
		relation := &HeapRelation{
			RelId:   relid,
			RelName: "bp_class",
			RelDesc: ClassTupleDesc,
		}
		relation.initRelFileNode()
		return relation, nil
	} else if relid == AttributeRelId {
		relation := &HeapRelation{
			RelId:   relid,
			RelName: "bp_attribute",
			RelDesc: AttributeTupleDesc,
		}
		relation.initRelFileNode()
		return relation, nil
	}

	/*
	 * Collect class information.  Currently, nothing but name is stored.
	 */
	class_rel, err := HeapOpen(ClassRelId, bufMgr)
	if err != nil {
		return nil, err
	}
	defer class_rel.Close()
	var scan_keys []ScanKey
	scan_keys = []ScanKey{
		{system.OidAttrNumber, system.Datum(relid)},
	}

	class_scan, err := class_rel.BeginScan(scan_keys, bufMgr)
	if err != nil {
		return nil, err
	}
	defer class_scan.EndScan()
	class_tuple, err := class_scan.Next()
	relation := &HeapRelation{
		RelId:   relid,
		RelName: class_tuple.Fetch(Anum_class_relname).(system.Name),
	}

	attr_rel, err := HeapOpen(AttributeRelId, bufMgr)
	if err != nil {
		return nil, err
	}
	defer attr_rel.Close()
	scan_keys = []ScanKey{
		{Anum_attribute_attrelid, system.Datum(relid)},
	}

	/*
	 * Collect attributes
	 */
	attr_scan, err := attr_rel.BeginScan(scan_keys, bufMgr)
	if err != nil {
		return nil, err
	}
	defer attr_scan.EndScan()
	var attributes []*Attribute
	for {
		attr_tuple, err := attr_scan.Next()
		if err != nil {
			break
		}
		typid := attr_tuple.Fetch(Anum_attribute_atttypid).(system.Oid)
		attribute := &Attribute{
			Name:   attr_tuple.Fetch(Anum_attribute_attname).(system.Name),
			TypeId: typid,
			Type:   system.TypeRegistry[typid],
		}
		attributes = append(attributes, attribute)
	}
	relation.RelDesc = &TupleDesc{
		Attrs: attributes,
	}

	relation.initRelFileNode()

	return relation, nil
}

func (rel *HeapRelation) GetNumberOfBlocks() (system.BlockNumber, error) {
	relpath := system.RelPath(rel.RelNode)
	fi, err := os.Stat(relpath)
	if err != nil {
		return 0, err
	}
	size := fi.Size()
	if size%system.BlockSize != 0 {
		return 0, fmt.Errorf("size of %s = %d is not module BlockSize", size, relpath)
	}

	return system.BlockNumber(size / system.BlockSize), nil
}

func (rel *HeapRelation) Close() {
}

func (rel *HeapRelation) BeginScan(keys []ScanKey, bufMgr storage.BufferManager) (Scan, error) {
	scan := &HeapScan{
		rel:      rel,
		Forward:  true,
		ScanKeys: keys,
		bufMgr:   bufMgr,
	}
	nBlocks, err := rel.GetNumberOfBlocks()
	if err != nil {
		return nil, err
	}
	scan.startBlock = 0
	scan.nBlocks = nBlocks

	return Scan(scan), nil
}

func (scan *HeapScan) getBuffer(blockNum system.BlockNumber) (storage.Buffer, system.BlockNumber, error) {

	// release previous scan buffer, if any
	if scan.cBuf.IsValid() {
		scan.bufMgr.ReleaseBuffer(scan.cBuf)
		scan.cBuf = storage.InvalidBuffer()
	}

	// read page
	buf, err := scan.bufMgr.ReadBuffer(scan.rel.RelNode, blockNum)
	if err != nil {
		return storage.InvalidBuffer(), system.InvalidBlockNumber, err
	}
	return buf, blockNum, nil
}

func (scan *HeapScan) Next() (Tuple, error) {

	var lineOff system.OffsetNumber
	var cBlock system.BlockNumber = system.InvalidBlockNumber
	tuple := scan.cTuple

	if !scan.inited {
		// return immediately if relation is empty
		if scan.nBlocks == 0 {
			tuple.SetData(nil, system.InvalidItemPointer)
			return nil, nil
		}

		cBlock = scan.startBlock
		if buf, block, err := scan.getBuffer(cBlock); err != nil {
			return nil, err
		} else {
			scan.cBuf, scan.cBlock = buf, block
		}
		lineOff = system.FirstOffsetNumber
		scan.inited = true
	} else {
		// continue from previously returned page/tuple
		cBlock = scan.cBlock
		lineOff = tuple.self.OffsetNumber().Next()
	}

	scan.cBuf.RLock()

	page := scan.cBuf.GetPage()
	nLines := page.MaxOffsetNumber()
	linesLeft := nLines - lineOff + 1

	itemId := page.ItemId(lineOff)
	for {
		for linesLeft > 0 {
			if itemId.IsNormal() {
				tid := system.MakeItemPointer(cBlock, lineOff)
				tuple.SetData(page.Item(itemId), tid)

				// TODO: valid = HeapTupleSatisfyiesVisibility()

				scan.cBuf.Unlock()
			}

			// otherwise move to the next item on the page
			linesLeft--

			lineOff++
			itemId = page.ItemId(lineOff)
		}

		// if we get here, it means we've exhausted the items on this page and
		// it's time to move to the next.
		scan.cBuf.Unlock()

		cBlock++
		if cBlock >= scan.nBlocks {
			cBlock = 0
		}
		finished := cBlock == scan.startBlock

		if finished {
			if scan.cBuf.IsValid() {
				scan.bufMgr.ReleaseBuffer(scan.cBuf)
			}
			scan.cBuf = storage.InvalidBuffer()
			scan.cBlock = system.InvalidBlockNumber
			tuple.SetData(nil, system.InvalidItemPointer)
			scan.inited = false
			return nil, nil
		}

		if buf, block, err := scan.getBuffer(cBlock); err != nil {
			return nil, err
		} else {
			scan.cBuf, scan.cBlock = buf, block
		}

		scan.cBuf.RLock()

		page = scan.cBuf.GetPage()
		nLines = page.MaxOffsetNumber()
		linesLeft = nLines
		lineOff = system.FirstOffsetNumber
		itemId = page.ItemId(lineOff)
	}
}

func (scan *HeapScan) EndScan() error {
	// TODO:
	return nil
}
