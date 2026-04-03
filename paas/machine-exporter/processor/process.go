package processor

import (
	"machine-exporter/processor/bitalos"
	"machine-exporter/processor/proxy"
	"runtime"
	"time"

	"log"
)

func InitProcessor(b *bitalos.Bitalos, p *proxy.Proxy) {
	go func() {
		var i int64
		defer func() {
			if e := recover(); e != nil {
				buf := make([]byte, 2048)
				n := runtime.Stack(buf, false)
				buf = buf[0:n]
				log.Printf("processor run panic, [err:%v] [panic:%s]", e, string(buf))
			}
		}()
		for {
			b.CollectInfo()
			p.CollectInfo()

			if i%30 == 0 {
				b.UpdateCpuLimit()
				p.UpdateCpuLimit()
			}

			if i%3 == 0 {
				p.CollectInfoStat()
			}

			time.Sleep(10 * time.Second)
			i++
		}
	}()
}
