//go:build gocqlv1

package main

import (
	"time"

	gocqlv1 "github.com/gocql/gocql"
	lz4v1 "github.com/gocql/gocql/lz4"
)

func getSession() (*gocqlv1.Session, error) {
	cluster := gocqlv1.NewCluster(*contactPoints)
	if *compression {
		cluster.Compressor = lz4v1.LZ4Compressor{}
	}
	cluster.DefaultTimestamp = false
	cluster.ProtoVersion = *protoVersion
	cluster.Timeout = 30 * time.Second

	return cluster.CreateSession()
}
