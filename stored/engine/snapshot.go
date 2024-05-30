// Copyright 2019-2024 Xu Ruibo (hustxurb@163.com) and Contributors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package engine

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/cockroachdb/errors"
	"github.com/zuoyebang/bitalostored/butils"
	"github.com/zuoyebang/bitalostored/butils/unsafe2"
	"github.com/zuoyebang/bitalostored/stored/internal/config"
	"github.com/zuoyebang/bitalostored/stored/internal/log"
)

const readBufferSize = 2048

var (
	StartHeader = byte('$')
	EndHeader   = byte('\n')
)

const (
	HeaderStartLen    = 1
	HeaderFileSizeLen = 8
	HeaderNameLen     = 2
	HeaderEndLen      = 1
)

type SnapshotDetail struct {
	SnapshotPath string
	UpdateIndex  uint64
}

func (detail SnapshotDetail) Clean() {
	dir := detail.SnapshotPath
	index := detail.UpdateIndex
	indexSnapshotDir := path.Join(dir, strconv.FormatUint(index, 10))

	e := os.RemoveAll(indexSnapshotDir)
	if e != nil {
		log.Warn("remove snapshot file : ", indexSnapshotDir, " ", e)
	} else {
		log.Info("remove snapshot file: ", indexSnapshotDir)
	}
}

type SnapshotFile struct {
	Size int64
	Name string
}

func (sf *SnapshotFile) headerLen() int {
	return HeaderStartLen + HeaderFileSizeLen + HeaderNameLen + len(sf.Name) + HeaderEndLen
}

func (sf *SnapshotFile) writeHeader(w io.Writer) error {
	var buf [8]byte
	var wn int

	buf[0] = StartHeader
	if n, err := w.Write(buf[0:HeaderStartLen]); err != nil {
		return err
	} else {
		wn += n
	}

	binary.BigEndian.PutUint64(buf[0:HeaderFileSizeLen], uint64(sf.Size))
	if n, err := w.Write(buf[0:HeaderFileSizeLen]); err != nil {
		return err
	} else {
		wn += n
	}

	binary.BigEndian.PutUint16(buf[0:HeaderNameLen], uint16(len(sf.Name)))
	if n, err := w.Write(buf[0:HeaderNameLen]); err != nil {
		return err
	} else {
		wn += n
	}

	if n, err := w.Write(unsafe2.ByteSlice(sf.Name)); err != nil {
		return err
	} else {
		wn += n
	}

	buf[0] = EndHeader
	if n, err := w.Write(buf[0:HeaderEndLen]); err != nil {
		return err
	} else {
		wn += n
	}

	sfLen := sf.headerLen()
	if wn != sfLen {
		return fmt.Errorf("write file header size err exp:%d act:%d", sfLen, wn)
	}

	return nil
}

func (sf *SnapshotFile) readHeader(r *bufio.Reader) error {
	var flagByte [HeaderStartLen]byte
	var fileSize [HeaderFileSizeLen]byte
	var nameSize [HeaderNameLen]byte

	_, err := io.ReadFull(r, flagByte[:])
	if err != nil {
		return err
	}
	if flagByte[0] != StartHeader {
		return fmt.Errorf("snapshotFile readHeader not invalid header start type '$', but %c", flagByte[0])
	}

	_, err = io.ReadFull(r, fileSize[:])
	if err != nil {
		return err
	}

	_, err = io.ReadFull(r, nameSize[:])
	if err != nil {
		return err
	}
	nameLen := int(binary.BigEndian.Uint16(nameSize[:]))
	if nameLen <= 0 {
		return errors.New("filename len is zero")
	}

	filename := make([]byte, nameLen)
	_, err = io.ReadFull(r, filename)
	if err != nil {
		return err
	}

	_, err = io.ReadFull(r, flagByte[:])
	if err != nil {
		return err
	}
	if flagByte[0] != EndHeader {
		return fmt.Errorf("snapshotFile readHeader not invalid header end type '\n', but %c", flagByte[0])
	}
	sf.Size = int64(binary.BigEndian.Uint64(fileSize[:]))
	sf.Name = string(filename)
	log.Infof("snapshotFile readHeader file:%s size:%d", sf.Name, sf.Size)
	return nil
}

func (sf *SnapshotFile) writeToFile(br *bufio.Reader, dbsyncpath string) error {
	buf := make([]byte, readBufferSize)
	size := sf.Size
	sfPath := path.Join(dbsyncpath, sf.Name)
	log.Info("snapshotFile writeToFile sfPath : ", sfPath)

	if index := strings.LastIndex(sfPath, "/"); index > 0 {
		dirpath := sfPath[:index]
		if _, err := os.Stat(dirpath); os.IsNotExist(err) {
			err = os.MkdirAll(dirpath, 0755)
			if err != nil {
				return err
			}
		}
	}

	hasContent := false
	f, err := os.Create(sfPath)
	if err != nil {
		return err
	}
	defer f.Close()

	for size > 0 {
		hasContent = true
		if size > 2048 {
			rn, err := io.ReadFull(br, buf)
			if err != nil {
				return err
			}
			if rn != readBufferSize {
				return errors.Errorf("SnapshotFile reader io byte err exp:%d act:%d", readBufferSize, rn)
			}

			size = size - int64(rn)
			if wn, err := f.Write(buf); err != nil {
				return err
			} else if wn != readBufferSize {
				return errors.Errorf("SnapshotFile writeToFile byte err exp:%d act:%d", readBufferSize, wn)
			}
		} else if size > 0 {
			last := size
			newBuf := make([]byte, size)
			if rn, err := io.ReadFull(br, newBuf); err == nil || err == io.EOF {
				if int64(rn) != size {
					return errors.Errorf("SnapshotFile last reader io byte err exp:%d act:%d", size, rn)
				}

				size = 0
				if wn, err1 := f.Write(newBuf); err1 != nil {
					return err1
				} else if int64(wn) != last {
					return errors.Errorf("SnapshotFile writeToFile byte err exp:%d, act:%d", last, wn)
				}
				if err == io.EOF {
					return err
				}
			} else {
				return err
			}
		}
	}

	if !hasContent {
		log.Warnf("snapshotFile writeToFile emtpy content to write sfPath:%s", sfPath)
	}

	return nil
}

func (b *Bitalos) DoSnapshot(snapshotPath string) (interface{}, error) {
	updateIndex := b.Meta.GetUpdateIndex()
	snapshotDir := path.Join(snapshotPath, strconv.FormatUint(updateIndex, 10))

	if last := b.Meta.SetSnapshotIndex(updateIndex); last > 0 {
		lastSnapshot := SnapshotDetail{SnapshotPath: snapshotPath, UpdateIndex: last}
		lastSnapshot.Clean()
	}

	if _, err := os.Stat(snapshotDir); err == nil {
		log.Infof(" remove all existed snapshotDir %s", snapshotDir)
		_ = os.RemoveAll(snapshotDir)
	}

	_ = os.MkdirAll(snapshotDir, 0755)

	if err := b.bitsdb.Checkpoint(snapshotDir); err != nil {
		return nil, errors.Errorf("prepare do bitsdb checkpoint err:%s", err.Error())
	}

	if err := b.Meta.Checkpoint(snapshotDir); err != nil {
		return nil, errors.Errorf("prepare do meta checkpoint err:%s", err.Error())
	}

	sd := &SnapshotDetail{
		SnapshotPath: snapshotDir,
		UpdateIndex:  updateIndex,
	}
	return sd, nil
}

func (b *Bitalos) SaveSnapshot(ctx interface{}, w io.Writer, done <-chan struct{}) error {
	sd, ok := ctx.(*SnapshotDetail)
	if !ok {
		err := errors.New("bitalos SaveSnapshot parse detail fail")
		log.Error(err)
		return err
	}

	log.Info("bitalos SaveSnapshot start detail", sd)
	defer log.Cost("bitalos SaveSnapshot finish ")()

	sf := &SnapshotFile{}
	walkErr := filepath.Walk(sd.SnapshotPath, func(fpath string, info os.FileInfo, we error) error {
		if info.IsDir() {
			return nil
		}

		filename, err := config.GetSuffixSnapshotFileName(fpath)
		if err != nil {
			log.Errorf("bitalos SaveSnapshot GetSuffixSnapshotFileName file:%s fail", fpath)
			return err
		}

		sf.Name = filename
		sf.Size = info.Size()

		log.Infof("bitalos SaveSnapshot write file start file:%s name:%s size:%s", fpath, filename, butils.FmtSize(uint64(sf.Size)))
		f, err := os.Open(fpath)
		if err != nil {
			log.Errorf("bitalos SaveSnapshot open file fail file:%s err:%s", fpath, err.Error())
			return err
		}
		defer f.Close()

		if err := sf.writeHeader(w); err != nil {
			log.Errorf("bitalos SaveSnapshot write file header fail file:%s err:%s", fpath, err.Error())
			return err
		}

		if n, err := io.Copy(w, f); err != nil {
			log.Errorf("bitalos SaveSnapshot write file fail file:%s err:%s", fpath, err.Error())
			return err
		} else if n != sf.Size {
			log.Errorf("bitalos SaveSnapshot write file size err file:%s exp:%d act:%d", fpath, sf.Size, n)
			return errors.New("send snapshot file size err")
		}

		return nil
	})

	return walkErr
}

func (b *Bitalos) RecoverFromSnapshot(r io.Reader, done <-chan struct{}) (string, error) {
	var err error
	var rn int64
	defer log.Cost("bitalos recoverFromSnapshot ")(func() []interface{} {
		return []interface{}{" reader from network io", fmt.Sprintf(" err:%v", err)}
	})

	dbsyncPath := config.GetBitalosRaftDbsyncPath()
	log.Infof("bitalos recoverFromSnapshot start dbsyncPath:%s", dbsyncPath)
	os.RemoveAll(dbsyncPath)
	os.MkdirAll(dbsyncPath, 0755)

	br := bufio.NewReader(r)
	sf := new(SnapshotFile)

	for {
		if err = sf.readHeader(br); err != nil {
			if err == io.EOF {
				break
			}
			return "", err
		}

		if err = sf.writeToFile(br, dbsyncPath); err != nil {
			if err == io.EOF {
				break
			}
			return "", err
		}

		rn += int64(sf.headerLen()) + sf.Size
	}

	idx := strings.Index(sf.Name, "/")
	if idx == -1 {
		log.Errorf("bitalos recoverFromSnapshot parse updateIndex err sfName:%s", sf.Name)
		return "", errors.New("bitalos recoverFromSnapshot parse updateIndex err")
	}
	updateIndex := sf.Name[:idx]
	dbsyncUpdateIndexPath := filepath.Join(dbsyncPath, updateIndex)
	log.Infof("bitalos recoverFromSnapshot finish readNum:%d updateIndex:%s indexPath:%s", rn, updateIndex, dbsyncUpdateIndexPath)

	return dbsyncUpdateIndexPath, nil
}
