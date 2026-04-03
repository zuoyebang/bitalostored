// Copyright 2019-2024 Xu Ruibo (hustxurb@163.com) and Contributors
//
// Licensed under the Apache License, Version 2.0 (the \"License\");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an \"AS IS\" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package rpc

import (
	"bytes"
	jsoniter "github.com/json-iterator/go"
	"github.com/zuoyebang/bitalostored/paas/utils/config"
	"github.com/zuoyebang/bitalostored/paas/utils/errors"
	"io"
	"net/http"
)

type dingMsg struct {
	Msgtype  string    `json:"msgtype"`
	Markdown *markdown `json:"markdown"`
}

type markdown struct {
	Title string `json:"title"`
	Text  string `json:"text"`
}

const (
	OpErrTitle = "op failed"
	OpTitle    = "op notice"
)

func SendDingding(title, msg string) (err error) {
	md := &markdown{
		Title: title,
		Text:  msg,
	}
	message := &dingMsg{
		Msgtype:  "markdown",
		Markdown: md,
	}
	jsonBody, _ := jsoniter.Marshal(message)
	url := config.GetConf().Robot.OpDing

	contentType := "application/json"
	res, err := http.Post(url, contentType, bytes.NewReader(jsonBody))
	if err != nil {
		return err
	}
	body, _ := io.ReadAll(res.Body)
	type respBodyT struct {
		ErrCode int    `json:"errcode"`
		ErrMsg  string `json:"errmsg"`
	}
	var resp respBodyT
	if err := jsoniter.Unmarshal(body, &resp); err != nil {
		return err
	}
	if resp.ErrCode != 0 {
		return errors.New("errMsg=" + resp.ErrMsg)
	}

	return nil
}
