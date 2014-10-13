package system

// To be platform-dependent
const MaximumAlignof = 8

func TypeAlign(alignval, l uintptr) uintptr {
	return (l + alignval - 1) & ^(alignval - 1)
}

func MaxAlign(l uintptr) uintptr {
	return TypeAlign(MaximumAlignof, l)
}
