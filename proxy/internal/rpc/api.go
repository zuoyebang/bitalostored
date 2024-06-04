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

package rpc

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"time"

	"github.com/zuoyebang/bitalostored/proxy/internal/log"
	"github.com/zuoyebang/bitalostored/proxy/internal/trace"

	"github.com/cockroachdb/errors"
)

const (
	MethodGet  = "GET"
	MethodPut  = "PUT"
	MethodPost = "POST"
)

type ApiRes struct {
	Status int          `json:"status"`
	ErrMsg *RemoteError `json:"errmsg"`
	Data   interface{}  `json:"data"`
}

func NewApiResByData(status int, data interface{}, errmsg *RemoteError) *ApiRes {
	return &ApiRes{
		Status: status,
		Data:   data,
		ErrMsg: errmsg,
	}
}

var client *http.Client

func init() {
	tr := &http.Transport{}
	tr.Dial = func(network, addr string) (net.Conn, error) {
		return net.DialTimeout(network, addr, time.Second)
	}
	client = &http.Client{
		Transport: tr,
		Timeout:   time.Minute,
	}
	go func() {
		for {
			time.Sleep(time.Minute)
			tr.CloseIdleConnections()
		}
	}()
}

type RemoteError struct {
	Cause string      `json:"cause"`
	Stack trace.Stack `json:"stack"`
}

func (e *RemoteError) Error() string {
	return e.Cause
}

func (e *RemoteError) TracedError() error {
	return errors.New("[Remote Error] " + e.Cause)
}

func NewRemoteError(err error) *RemoteError {
	if err == nil {
		return nil
	}
	if v, ok := err.(*RemoteError); ok {
		return v
	}
	return &RemoteError{
		Cause: err.Error(),
		Stack: nil,
	}
}

func responseBodyAsBytes(rsp *http.Response) ([]byte, error) {
	b, err := io.ReadAll(rsp.Body)
	if err != nil {
		return nil, err
	}
	return b, nil
}

func responseBodyAsError(rsp *http.Response) (error, error) {
	b, err := responseBodyAsBytes(rsp)
	if err != nil {
		return nil, err
	}
	if len(b) == 0 {
		return nil, errors.Errorf("remote error is empty")
	}
	e := &RemoteError{}
	if err := json.Unmarshal(b, e); err != nil {
		return nil, err
	}
	return e.TracedError(), nil
}

func apiMarshalJson(v interface{}) ([]byte, error) {
	return json.MarshalIndent(v, "", "    ")
}

func apiRequestJson(method string, url string, args, reply interface{}) error {
	var body []byte
	if args != nil {
		b, err := apiMarshalJson(args)
		if err != nil {
			return err
		}
		body = b
	}
	req, err := http.NewRequest(method, url, bytes.NewReader(body))
	if err != nil {
		return err
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Content-Length", strconv.Itoa(len(body)))
	}

	var start = time.Now()

	rsp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer func() {
		io.Copy(io.Discard, rsp.Body)
		rsp.Body.Close()
		if method == "PUT" {
			log.Infof("call rpc [%s] %s in %v", method, url, time.Since(start))
		}
	}()

	switch rsp.StatusCode {
	case 200:
		b, err := responseBodyAsBytes(rsp)
		if err != nil {
			return err
		}
		if reply == nil {
			return nil
		}
		apiRes := &ApiRes{Data: reply}
		if err := json.Unmarshal(b, apiRes); err != nil {
			return err
		} else {
			return nil
		}
	case 800, 1500:
		e, err := responseBodyAsError(rsp)
		if err != nil {
			return err
		} else {
			return e
		}
	default:
		return errors.Errorf("[%d] %s - %s", rsp.StatusCode, http.StatusText(rsp.StatusCode), url)
	}
}

func ApiGetJson(url string, reply interface{}) error {
	return apiRequestJson(MethodGet, url, nil, reply)
}

func ApiPutJson(url string, args, reply interface{}) error {
	return apiRequestJson(MethodPut, url, args, reply)
}

func ApiPostJson(url string, args interface{}) error {
	return apiRequestJson(MethodPost, url, args, nil)
}

func ApiResponseError(err error) (int, string) {
	if err == nil {
		return 800, ""
	}
	apiRes := NewApiResByData(800, nil, NewRemoteError(err))
	b, err := apiMarshalJson(apiRes)
	if err != nil {
		return 800, ""
	} else {
		return 800, string(b)
	}
}

func ApiResponseJson(v interface{}) (int, string) {
	apiRes := NewApiResByData(200, v, &RemoteError{})
	b, err := apiMarshalJson(apiRes)
	if err != nil {
		return ApiResponseError(err)
	} else {
		return 200, string(b)
	}
}

func EncodeURL(host string, format string, args ...interface{}) string {
	var u url.URL
	u.Scheme = "http"
	u.Host = host
	u.Path = fmt.Sprintf(format, args...)
	return u.String()
}
