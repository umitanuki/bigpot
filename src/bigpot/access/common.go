package access

import "encoding/csv"
import "os"
import "path/filepath"
import "strconv"

import "bigpot/system"

var DatabaseDir = "."

type Attribute struct {
	AttName system.Name
	AttType system.Oid
}

type TupleDesc struct {
	Attrs []*Attribute
}

var ClassTupleDesc = &TupleDesc{
	[]*Attribute{
		{"oid", system.OidType},
		{"relname", system.NameType},
	},
}
var ClassRelId system.Oid = 1259
var Anum_class_oid int32 = 1
var Anum_class_relname int32 = 2

var AttributeTupleDesc = &TupleDesc{
	[]*Attribute{
		{"attrelid", system.OidType},
		{"attname", system.NameType},
		{"attnum", system.Int4Type},
		{"atttype", system.OidType},
	},
}
var AttributeRelId system.Oid = 1249
var Anum_attribute_attrelid int32 = 1
var Anum_attribute_attname int32 = 2
var Anum_attribute_attnum int32 = 3
var Anum_attribute_atttype int32 = 4

type Relation struct {
	RelId   system.Oid
	RelName system.Name
	RelDesc *TupleDesc
}

type ScanKey struct {
	AttNum int32
	Val    system.Datum
}

type RelationScan struct {
	Relation *Relation
	Forward  bool
	ScanKeys []ScanKey // non-pointer, as usually this is short life
	Reader   *csv.Reader
	File     *os.File
}

type Tuple interface {
	Get(attnum int32) system.Datum
}

type CSVTuple struct {
	TupleDesc *TupleDesc
	Values    []string
}

func HeapOpen(relid system.Oid) (*Relation, error) {
	if relid == ClassRelId {
		relation := &Relation{
			RelId:   relid,
			RelName: "bp_class",
			RelDesc: ClassTupleDesc,
		}
		return relation, nil
	} else if relid == AttributeRelId {
		relation := &Relation{
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
		{Anum_class_oid, system.Datum(relid)},
	}

	class_scan, err := class_rel.BeginScan(scan_keys)
	if err != nil {
		return nil, err
	}
	defer class_scan.EndScan()
	class_tuple, err := class_scan.Next()
	relation := &Relation{
		RelId:   relid,
		RelName: class_tuple.Get(2).(system.Name),
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
		attribute := &Attribute{
			AttName: attr_tuple.Get(Anum_attribute_attname).(system.Name),
			AttType: attr_tuple.Get(Anum_attribute_atttype).(system.Oid),
		}
		attributes = append(attributes, attribute)
	}
	relation.RelDesc = &TupleDesc{
		Attrs: attributes,
	}

	return relation, nil
}

func (relation *Relation) Close() {
}

func (relation *Relation) BeginScan(keys []ScanKey) (*RelationScan, error) {
	filepath := filepath.Join(DatabaseDir, strconv.Itoa(int(relation.RelId)))
	file, err := os.Open(filepath)
	if err != nil {
		return nil, err
	}
	scan := &RelationScan{
		Relation: relation,
		Forward:  true,
		ScanKeys: keys,
		Reader:   csv.NewReader(file),
		File:     file,
	}

	return scan, nil
}

func (scan *RelationScan) Next() (Tuple, error) {
outer:
	for {
		values, err := scan.Reader.Read()
		if err != nil {
			return nil, err
		}

		if values == nil {
			return nil, nil
		}

		tuple := Tuple(&CSVTuple{
			TupleDesc: scan.Relation.RelDesc,
			Values:    values,
		})

		for _, key := range scan.ScanKeys {
			val := tuple.Get(key.AttNum)
			/* TODO check key.Val type and TupleDesc's type */
			if !val.Equals(key.Val) {
				continue outer
			}
		}
		return tuple, nil
	}

	panic("should not come here")
}

func (scan *RelationScan) EndScan() {
	scan.File.Close()
}

func (tuple *CSVTuple) Get(attnum int32) system.Datum {
	value := tuple.Values[attnum-1]
	atttype := tuple.TupleDesc.Attrs[attnum-1].AttType
	return system.DatumFromString(value, atttype)
}
