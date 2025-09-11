//go:build !gocqlv1

package main

import (
	"errors"
	"fmt"

	"github.com/apache/cassandra-gocql-driver/v2"
	lz4v2 "github.com/apache/cassandra-gocql-driver/v2/lz4"
)

type sessionImpl struct {
	session *gocql.Session
}

func (recv *sessionImpl) Close() {
	recv.session.Close()
}
func (recv *sessionImpl) Exec(qry string, args ...interface{}) error {
	return recv.session.Query(qry, args...).Exec()
}
func (recv *sessionImpl) Query(qry string, args []interface{}, dest ...interface{}) error {
	iter := recv.session.Query(qry, args...).Iter()

	if !iter.Scan(dest...) {
		if err := iter.Close(); err != nil {
			return fmt.Errorf("SELECT failed: %w", err)
		}
		return errors.New("SELECT failed")
	}

	return nil
}

func getSession(cfg ClusterConfig) (Session, error) {
	cluster := gocql.NewCluster(cfg.Hosts...)
	if cfg.Compression {
		cluster.Compressor = lz4v2.LZ4Compressor{}
	}
	cluster.DefaultTimestamp = cfg.DefaultTimestamp
	cluster.ProtoVersion = cfg.ProtoVersion
	cluster.Timeout = cfg.Timeout

	session, err := cluster.CreateSession()
	if err != nil {
		return nil, err
	}
	fmt.Println("Creating gocql v2 Session.")
	return &sessionImpl{session: session}, nil
}
