package webclient

import (
	"bytes"
	jsoniter "github.com/json-iterator/go"
	"github.com/zuoyebang/bitalostored/paas/agent/internal/utils/logs"
	"io"
	"net/http"
)

func PostPaaS(path string, data map[string]interface{}) ([]byte, error) {
	b, e := jsoniter.Marshal(data)
	if e != nil {
		logs.Warn("post", path, e)
		return nil, e
	}

	res, err := http.Post(path, "application/json", bytes.NewBuffer(b))
	if err != nil {
		logs.Warn("post", path, err)
		return nil, err
	}
	defer res.Body.Close()
	logs.Info("post", path, data)
	return io.ReadAll(res.Body)
}
