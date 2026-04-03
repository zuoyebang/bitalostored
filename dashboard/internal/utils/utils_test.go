package utils

import (
	"fmt"
	"testing"
)

func TestGetMajorVersion(t *testing.T) {
	v := "v7"
	fmt.Println(GetMajorVersion(v))
}
