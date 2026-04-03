package flock

import (
	"errors"
	"os"
	"syscall"
)

type Flock struct {
	LockFile string
	lock     *os.File
}

var IsLocked bool

func Create(file string) (f *Flock, e error) {
	if file == "" {
		e = errors.New("cannot create flock on empty path")
		return
	}
	lock, e := os.Create(file)
	if e != nil {
		return
	}
	return &Flock{
		LockFile: file,
		lock:     lock,
	}, nil
}

func (f *Flock) Release() {
	if f != nil && f.lock != nil {
		f.lock.Close()
		os.Remove(f.LockFile)
	}
}

func (f *Flock) Lock() (e error) {
	if f == nil {
		e = errors.New("cannot use lock on a nil flock")
		return
	}
	return syscall.Flock(int(f.lock.Fd()), syscall.LOCK_EX|syscall.LOCK_NB)
}

func (f *Flock) Unlock() {
	if f != nil {
		syscall.Flock(int(f.lock.Fd()), syscall.LOCK_UN)
	}
}
