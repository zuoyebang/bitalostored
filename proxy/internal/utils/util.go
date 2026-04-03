package utils

import "strconv"

func ServerGroupKey(addr string, gid int) string {
	return addr + strconv.Itoa(gid)
}
