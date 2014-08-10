package system

type Xid uint32

const InvalidXid = 0
const BootstrapXid = 1
const FrozenXid = 2
const FirstNormalXid = 3
const MaxXid = 0xFFFFFFFF

func (xid Xid) IsValid() bool {
	return xid != InvalidXid
}

func (xid Xid) IsNormal() bool {
	return xid >= FirstNormalXid
}

func (xid Xid) Advance() Xid {
	xid++
	if xid < FirstNormalXid {
		xid = FirstNormalXid
	}
	return xid
}

func (xid Xid) Precedes(other Xid) bool {
	if !xid.IsNormal() || !other.IsNormal() {
		return xid < other
	}

	diff := int32(xid - other)
	return diff < 0
}

func (xid Xid) PrecedesOrEquals(other Xid) bool {
	if !xid.IsNormal() || !other.IsNormal() {
		return xid <= other
	}

	diff := int32(xid - other)
	return diff <= 0
}

func (xid Xid) Follows(other Xid) bool {
	if !xid.IsNormal() || !other.IsNormal() {
		return xid > other
	}

	diff := int32(xid - other)
	return diff > 0
}

func (xid Xid) FollowsOrEquals(other Xid) bool {
	if !xid.IsNormal() || !other.IsNormal() {
		return xid >= other
	}

	diff := int32(xid - other)
	return diff >= 0
}
