package main

import (
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/lunfardo314/proxima/node"
)

func main() {
	killChan := make(chan os.Signal, 1)
	signal.Notify(killChan, syscall.SIGINT, syscall.SIGTERM)

	n := node.New()
	go func() {
		<-killChan
		n.Stop()
	}()

	n.Start()
	n.WaitAllWorkProcessesToStop(5 * time.Second)
	n.WaitAllDBClosed()
	n.Log().Infof("total uptime: %v", n.UpTime())
	n.Log().Infof("Hasta la vista, baby! I'll be back")
}
