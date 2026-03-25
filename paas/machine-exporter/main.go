package main

import (
	"github.com/spf13/pflag"
	"log"
	"machine-exporter/helper"
	"machine-exporter/processor"
	"machine-exporter/processor/bitalos"
	"machine-exporter/processor/proxy"
	"net/http"
	"os"
	"runtime/debug"
	"strconv"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

func init() {
	configPath := pflag.String("conf", "", "run with the specific configuration.")
	pflag.Parse()
	err := helper.SetConf(*configPath)
	if err != nil {
		log.Fatalf("read config file failed.err:%+v", err)
		return
	}
}

func main() {
	helper.InitMysql()
	allNode := helper.GetAllNode()
	registry := prometheus.NewRegistry()
	instancesProxy, instancesBitalos := helper.Format(allNode)
	newProxy := proxy.NewProxy(instancesProxy)
	newBitalos := bitalos.NewBitalos(instancesBitalos)
	processor.InitProcessor(newBitalos.(*bitalos.Bitalos), newProxy.(*proxy.Proxy))
	recordPid()
	registry.MustRegister(newProxy)
	registry.MustRegister(newBitalos)
	http.Handle(helper.GetConf().Env.MetricPath, promhttp.HandlerFor(registry, promhttp.HandlerOpts{}))
	http.HandleFunc("/", RecoveryMiddleware(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(`<html>
<head><title>Matrix Exporter v` + BuildVersion + `</title></head>
<body>
<h1>Matrix Exporter ` + BuildVersion + `</h1>
<p><a href='` + helper.GetConf().Env.MetricPath + `'>Metrics</a></p>
</body>
</html>`))
	}))
	log.Fatal(http.ListenAndServe(helper.GetConf().Env.Address, nil))
}

func recordPid() {
	pidfile := "/home/homework/prometheus/machine-exporter/bin/machine_exporter.pid"
	if err := os.WriteFile(pidfile, []byte(strconv.Itoa(os.Getpid())), 0664); err != nil {
		log.Fatalf("write pidfile:%s failed err:%s", pidfile, err.Error())
	}
}

func RecoveryMiddleware(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			if err := recover(); err != nil {
				log.Printf("Recovered from panic: %v\nStack Trace:\n%s",
					err, debug.Stack())
				http.Error(w, "Internal Server Error", http.StatusInternalServerError)
			}
		}()
		next(w, r)
	}
}
