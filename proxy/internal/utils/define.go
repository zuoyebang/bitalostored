package utils

import "github.com/zuoyebang/bitalostored/proxy/internal/errn"

var (
	MaxKeySize = 512
)

func CheckKeySize(keySize int) error {
	if keySize > MaxKeySize || keySize == 0 {
		return errn.ErrKeySize
	}
	return nil
}
