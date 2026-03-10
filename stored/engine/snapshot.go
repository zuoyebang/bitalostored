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
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
	"strings"

	"github.com/zuoyebang/bitalostored/stored/internal/config"
	"github.com/zuoyebang/bitalostored/stored/internal/log"

	"github.com/zuoyebang/bitalostored/butils"
	"github.com/zuoyebang/bitalostored/butils/unsafe2"
)

const sfIOBufferSize = 8192

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
	IsRoot       bool
	ClusterId    uint64
}

func (detail SnapshotDetail) Clean() {
	deleteDir := detail.SnapshotPath
	if detail.IsRoot {
		deleteDir = path.Join(deleteDir, fmt.Sprintf("%d", detail.ClusterId))
	}

	e := os.RemoveAll(deleteDir)
	if e != nil {
		log.Warn("remove snapshot file : ", deleteDir, " ", e)
	} else {
		log.Info("remove snapshot file: ", deleteDir)
	}
}

type SnapshotFile struct {
	size int64
	name string
	buf  []byte
}

func (sf *SnapshotFile) headerLen() int {
	return HeaderStartLen + HeaderFileSizeLen + HeaderNameLen + len(sf.name) + HeaderEndLen
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

	binary.BigEndian.PutUint64(buf[0:HeaderFileSizeLen], uint64(sf.size))
	if n, err := w.Write(buf[0:HeaderFileSizeLen]); err != nil {
		return err
	} else {
		wn += n
	}

	binary.BigEndian.PutUint16(buf[0:HeaderNameLen], uint16(len(sf.name)))
	if n, err := w.Write(buf[0:HeaderNameLen]); err != nil {
		return err
	} else {
		wn += n
	}

	if n, err := w.Write(unsafe2.ByteSlice(sf.name)); err != nil {
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
	sf.size = int64(binary.BigEndian.Uint64(fileSize[:]))
	sf.name = string(filename)
	log.Infof("snapshotFile readHeader file:%s size:%d", sf.name, sf.size)
	return nil
}

func (sf *SnapshotFile) writeToFile(br *bufio.Reader, dbsyncpath string) error {
	sfPath := path.Join(dbsyncpath, sf.name)
	size := int(sf.size)
	if size == 0 {
		log.Warnf("snapshotFile writeToFile emtpy content to write sfPath:%s", sfPath)
		return nil
	}

	log.Infof("snapshotFile writeToFile sfPath:%s", sfPath)

	if index := strings.LastIndex(sfPath, "/"); index > 0 {
		dirpath := sfPath[:index]
		if _, err := os.Stat(dirpath); os.IsNotExist(err) {
			err = os.MkdirAll(dirpath, 0755)
			if err != nil {
				return err
			}
		}
	}

	f, err := os.Create(sfPath)
	if err != nil {
		return err
	}
	defer f.Close()

	var rn, wn int
	for size > 0 {
		if size > sfIOBufferSize {
			rn, err = io.ReadFull(br, sf.buf)
			if err != nil {
				return err
			}
			if rn != sfIOBufferSize {
				return fmt.Errorf("SnapshotFile writeToFile reade short n:%d", rn)
			}

			size = size - rn
			if wn, err = f.Write(sf.buf); err != nil {
				return err
			} else if wn != sfIOBufferSize {
				return fmt.Errorf("SnapshotFile writeToFile write short n:%d", wn)
			}
		} else {
			buf := sf.buf[:size]
			rn, err = io.ReadFull(br, buf)
			if err == nil || err == io.EOF {
				if rn != size {
					return fmt.Errorf("SnapshotFile writeToFile last reade short exp:%d act:%d", size, rn)
				}

				if wn, err = f.Write(buf); err != nil {
					return err
				} else if wn != size {
					return fmt.Errorf("SnapshotFile writeToFile last write short exp:%d act:%d", size, wn)
				}
			}

			return err
		}
	}

	return nil
}

func (b *Bitalos) DoSnapshot(snapshotRoot string, nodePrepare func(string) error, clusterId uint64) (interface{}, func(), error) {
	snapshotDir := path.Join(snapshotRoot, fmt.Sprintf("%d", clusterId))
	if _, err := os.Stat(snapshotDir); err == nil {
		log.Infof(" remove all existed snapshotDir %s", snapshotDir)
		_ = os.RemoveAll(snapshotDir)
	}

	_ = os.MkdirAll(snapshotDir, 0755)

	if nodePrepare != nil {
		if err := nodePrepare(snapshotDir); err != nil {
			return nil, nil, err
		}
	}

	ckCloser, err := b.Checkpoint(snapshotDir)
	if err != nil {
		return nil, nil, err
	}

	sd := &SnapshotDetail{
		SnapshotPath: snapshotDir,
		IsRoot:       false,
		ClusterId:    clusterId,
	}
	return sd, ckCloser, nil
}

func (b *Bitalos) SaveSnapshot(ctx interface{}, w io.Writer, done <-chan struct{}) error {
	sd, ok := ctx.(*SnapshotDetail)
	if !ok {
		err := errors.New("bitalos SaveSnapshot parse detail fail")
		log.Error(err)
		return err
	}

	log.Info("bitalos SaveSnapshot start detail", sd)
	defer log.Cost("bitalos SaveSnapshot finish")()

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

		sf.name = filename
		sf.size = info.Size()

		log.Infof("bitalos SaveSnapshot write file start file:%s name:%s size:%s", fpath, filename, butils.FmtSize(uint64(sf.size)))
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
		} else if n != sf.size {
			log.Errorf("bitalos SaveSnapshot write file size err file:%s exp:%d act:%d", fpath, sf.size, n)
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
	sf := &SnapshotFile{
		buf: make([]byte, sfIOBufferSize),
	}

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

		rn += int64(sf.headerLen()) + sf.size
	}

	idx := strings.Index(sf.name, "/")
	if idx == -1 {
		log.Errorf("bitalos recoverFromSnapshot parse updateIndex err sfName:%s", sf.name)
		return "", errors.New("bitalos recoverFromSnapshot parse updateIndex err")
	}
	snapshotName := sf.name[:idx]
	dbsyncSnapshotPath := filepath.Join(dbsyncPath, snapshotName)
	log.Infof("bitalos recoverFromSnapshot finish readNum:%d snapshotDir:%s dbsyncPath:%s", rn, snapshotName, dbsyncSnapshotPath)

	return dbsyncSnapshotPath, nil
}
