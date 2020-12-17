package util

func Max(x, y int64) int64{
	if x < y {
		return y
	} else {
		return x
	}
}

func Min(x, y int64) int64 {
	if x < y {
		return x
	} else {
		return y
	}
}
