package access

import (
	"bigpot/system"
)

type HeapRelation struct {
	RelId   system.Oid
	RelName system.Name
	RelDesc *TupleDesc
}

type HeapScan struct {
	HeapRelation *HeapRelation
	Forward      bool
	ScanKeys     []ScanKey // non-pointer, as usually this is short life
}

func HeapOpen(relid system.Oid) (*HeapRelation, error) {
	if relid == ClassRelId {
		relation := &HeapRelation{
			RelId:   relid,
			RelName: "bp_class",
			RelDesc: ClassTupleDesc,
		}
		return relation, nil
	} else if relid == AttributeRelId {
		relation := &HeapRelation{
			RelId:   relid,
			RelName: "bp_attribute",
			RelDesc: AttributeTupleDesc,
		}
		return relation, nil
	}

	/*
	 * Collect class information.  Currently, nothing but name is stored.
	 */
	class_rel, err := HeapOpen(ClassRelId)
	if err != nil {
		return nil, err
	}
	defer class_rel.Close()
	var scan_keys []ScanKey
	scan_keys = []ScanKey{
		{system.OidAttrNumber, system.Datum(relid)},
	}

	class_scan, err := class_rel.BeginScan(scan_keys)
	if err != nil {
		return nil, err
	}
	defer class_scan.EndScan()
	class_tuple, err := class_scan.Next()
	relation := &HeapRelation{
		RelId:   relid,
		RelName: class_tuple.Fetch(Anum_class_relname).(system.Name),
	}

	attr_rel, err := HeapOpen(AttributeRelId)
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
	attr_scan, err := attr_rel.BeginScan(scan_keys)
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

	return relation, nil
}

func (rel *HeapRelation) Close() {
}

func (rel *HeapRelation) BeginScan(keys []ScanKey) (*HeapScan, error) {
	scan := &HeapScan{
		HeapRelation: rel,
		Forward:      true,
		ScanKeys:     keys,
	}

	return scan, nil
}

func (scan *HeapScan) Next() (Tuple, error) {
	panic("should not come here")
}

func (scan *HeapScan) EndScan() {
}
