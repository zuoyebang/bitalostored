package task

import (
	"context"
	tcos "github.com/tencentyun/cos-go-sdk-v5"
	"github.com/zuoyebang/bitalostored/paas/agent/internal/config"
	"net/http"
	"net/url"
)

func getTcosClient() *tcos.Client {
	u, _ := url.Parse(config.C.Cos.Url)
	b := &tcos.BaseURL{BucketURL: u}
	return tcos.NewClient(b, &http.Client{
		Transport: &tcos.AuthorizationTransport{
			SecretID:  config.C.Cos.Ak,
			SecretKey: config.C.Cos.Sk,
		},
	})
}

func DownloadFile(name, filePath string) error {
	_, err := getTcosClient().Object.GetToFile(context.Background(), name, filePath, nil)
	return err
}
