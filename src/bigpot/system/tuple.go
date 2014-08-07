package system

import(
	"fmt"
	"io"
)

type AttrNumber int16

type Attribute struct {
	Name		Name
	Type		Oid
	//Len			int
}

type TupleDesc struct {
	Attrs []*Attribute
}

type HeapTuple struct {
	Oid		Oid
	Values	[]Datum
}

func (tuple *HeapTuple) Len() int {
	var sz = 0
	var has_null = false
	for _, val := range tuple.Values {
		if val != nil {
			sz += val.Len()
		} else {
			has_null = true
		}
	}

	// Add bitmap size
	// TODO: bitmap type
	// TODO: alignof()
	if (has_null) {
		l := len(tuple.Values)
		sz += (l + 7) / 8
	}

	// Add Oid size
	if (tuple.Oid != InvalidOid) {
		sz += tuple.Oid.Len()
	}
	return sz
}

func (tuple *HeapTuple) Get(attno AttrNumber) (Datum, error) {
	if int(attno) < 1 || int(attno) > len(tuple.Values) {
		return nil, fmt.Errorf("out of bound attno: %d", attno)
	}
	return tuple.Values[int(attno)-1], nil
}

func HeapTupleFromBytes(tupdesc *TupleDesc, reader io.Reader) (tuple *HeapTuple, err error) {
	values := make([]Datum, len(tupdesc.Attrs))
	for i, attr := range tupdesc.Attrs {
		value := DatumFromBytes(reader, attr.Type)
		values[i] = value
	}

	tuple = &HeapTuple{
		Values: values,
	}
	return
}

