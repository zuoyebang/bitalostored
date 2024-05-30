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

package resp

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"net"
)

type Error string

func (err Error) Error() string { return string(err) }

type RespReader struct {
	br *bufio.Reader
}

func NewRespReader(conn net.Conn, size int) *RespReader {
	br := bufio.NewReaderSize(conn, size)
	r := &RespReader{br}
	return r
}

func (resp *RespReader) Parse() (interface{}, error) {
	line, err := readLine(resp.br)
	if err != nil {
		return nil, err
	}
	if len(line) == 0 {
		return nil, errors.New("short resp line")
	}
	switch line[0] {
	case '+':
		switch {
		case len(line) == 3 && line[1] == 'O' && line[2] == 'K':
			return ReplyOK, nil
		case len(line) == 5 && line[1] == 'P' && line[2] == 'O' && line[3] == 'N' && line[4] == 'G':
			return ReplyPONG, nil
		default:
			return string(line[1:]), nil
		}
	case '-':
		return Error(string(line[1:])), nil
	case ':':
		n, err := parseInt(line[1:])
		return n, err
	case '$':
		n, err := parseLen(line[1:])
		if n < 0 || err != nil {
			return nil, err
		}
		p := make([]byte, n)
		_, err = io.ReadFull(resp.br, p)
		if err != nil {
			return nil, err
		}
		if line, err := readLine(resp.br); err != nil {
			return nil, err
		} else if len(line) != 0 {
			return nil, errors.New("bad bulk rstring format")
		}
		return p, nil
	case '*':
		n, err := parseLen(line[1:])
		if n < 0 || err != nil {
			return nil, err
		}
		r := make([]interface{}, n)
		for i := range r {
			r[i], err = resp.Parse()
			if err != nil {
				return nil, err
			}
		}
		return r, nil
	}
	return nil, errors.New("unexpected response line")
}

func (resp *RespReader) ParseRequest() ([][]byte, error) {
	line, err := readLine(resp.br)
	if err != nil {
		return nil, err
	}
	if len(line) == 0 {
		return resp.ParseRequest()
	}
	switch line[0] {
	case '*':
		n, err := parseLen(line[1:])
		if n < 0 || err != nil {
			return nil, err
		}
		r := make([][]byte, n)
		for i := range r {
			r[i], err = parseBulk(resp.br)
			if err != nil {
				return nil, err
			}
		}
		return r, nil
	default:
		return nil, fmt.Errorf("not invalid array of bulk rstring type, but %c", line[0])
	}
}

func (resp *RespReader) ParseBulkTo(w io.Writer) error {
	line, err := readLine(resp.br)
	if err != nil {
		return err
	}
	if len(line) == 0 {
		return errors.New("ledis: short response line")
	}

	switch line[0] {
	case '-':
		return Error(line[1:])
	case '$':
		n, err := parseLen(line[1:])
		if n < 0 || err != nil {
			return err
		}

		var nn int64
		if nn, err = io.CopyN(w, resp.br, int64(n)); err != nil {
			return err
		} else if nn != int64(n) {
			return io.ErrShortWrite
		}

		if line, err := readLine(resp.br); err != nil {
			return err
		} else if len(line) != 0 {
			return errors.New("bad bulk rstring format")
		}
		return nil
	default:
		return fmt.Errorf("not invalid bulk rstring type, but %c", line[0])
	}
}

func readLine(br *bufio.Reader) ([]byte, error) {
	p, err := br.ReadSlice('\n')
	if err == bufio.ErrBufferFull {
		return nil, errors.New("long resp line")
	}
	if err != nil {
		return nil, err
	}
	i := len(p) - 2
	if i < 0 || p[i] != '\r' {
		return nil, errors.New("bad resp line terminator")
	}
	return p[:i], nil
}

func parseLen(p []byte) (int, error) {
	if len(p) == 0 {
		return -1, errors.New("malformed length")
	}

	if p[0] == '-' && len(p) == 2 && p[1] == '1' {
		return -1, nil
	}

	var n int
	for _, b := range p {
		n *= 10
		if b < '0' || b > '9' {
			return -1, errors.New("illegal bytes in length")
		}
		n += int(b - '0')
	}

	return n, nil
}

func parseInt(p []byte) (int64, error) {
	if len(p) == 0 {
		return 0, errors.New("malformed integer")
	}

	var negate bool
	if p[0] == '-' {
		negate = true
		p = p[1:]
		if len(p) == 0 {
			return 0, errors.New("malformed integer")
		}
	}

	var n int64
	for _, b := range p {
		n *= 10
		if b < '0' || b > '9' {
			return 0, errors.New("illegal bytes in length")
		}
		n += int64(b - '0')
	}

	if negate {
		n = -n
	}
	return n, nil
}

func parseBulk(br *bufio.Reader) ([]byte, error) {
	line, err := readLine(br)
	if err != nil {
		return nil, err
	} else if len(line) == 0 {
		return nil, errors.New("short resp line")
	}

	switch line[0] {
	case '$':
		n, err := parseLen(line[1:])
		if n < 0 || err != nil {
			return nil, err
		}
		p := make([]byte, n)
		if _, err = io.ReadFull(br, p); err != nil {
			return nil, err
		}
		if line, err := readLine(br); err != nil {
			return nil, err
		} else if len(line) != 0 {
			return nil, errors.New("bad bulk rstring format")
		} else {
			return p, nil
		}
	default:
		return nil, fmt.Errorf("not invalid bulk rstring type, but %c", line[0])
	}
}
