package utils

import "testing"

func TestDing(t *testing.T) {
	msg := "123"
	s := SendDingding("", msg)
	t.Logf("err=%v", s)
}
