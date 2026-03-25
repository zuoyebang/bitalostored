package webclient

type PaaSResponse struct {
	Status int    `json:"status"`
	Msg    string `json:"msg"`
}
