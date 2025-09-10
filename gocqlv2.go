//go:build !gocqlv1

package main

import (
	"time"

	gocqlv2 "github.com/apache/cassandra-gocql-driver/v2"
	lz4v2 "github.com/apache/cassandra-gocql-driver/v2/lz4"
)

func getSession() *gocqlv2.Session {
	cluster := gocqlv2.NewCluster(*contactPoints)
	if *compression {
		cluster.Compressor = lz4v2.LZ4Compressor{}
	}
	cluster.DefaultTimestamp = false
	cluster.ProtoVersion = *protoVersion
	cluster.Timeout = 30 * time.Second

	return cluster.CreateSession()
}
