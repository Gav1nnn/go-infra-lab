package main

import (
	"log"

	"github.com/Gav1nnn/go-infra-lab/tree/main/distributed-fs/p2p"
)

func main() {
	tr := p2p.NewTCPTransport(":3000")

	if err := tr.ListenAndAccept(); err != nil {
		log.Fatal(err)
	}

	select {}

}
