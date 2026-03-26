package utils

import (
	"bytes"
	"errors"
	jsoniter "github.com/json-iterator/go"
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
	url := "https://oapi.dingtalk.com/robot/send?access_token=222dddf0e8800e3240f7397f2c12f236f75aa4dca3a558eee068c5a17c49d4e0"

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
