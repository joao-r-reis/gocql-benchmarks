package main

import (
	"flag"
	"fmt"
	"math/rand"
	"runtime/debug"
	"strings"
	"sync"
	"testing"
	"time"

	gocql "github.com/gocql/gocql"
	"github.com/gocql/gocql/lz4"
)

func setupSession(b *testing.B, compression bool, protoVersion int) *gocql.Session {
	cluster := gocql.NewCluster("127.0.0.1")
	if compression {
		cluster.Compressor = lz4.LZ4Compressor{}
	}
	cluster.ProtoVersion = protoVersion
	session, err := cluster.CreateSession()
	if err != nil {
		b.Fatal(err)
	}
	return session
}

func setupSelect(b *testing.B, short bool, compression bool, protoVersion int) *gocql.Session {
	buildInfo, ok := debug.ReadBuildInfo()
	if ok {
		for _, d := range buildInfo.Deps {
			if strings.Contains(d.Path, "gocql") {
				if d.Replace != nil {
					fmt.Printf("%v %v\n", d.Replace.Path, d.Replace.Version)
				} else {
					fmt.Printf("%v %v\n", d.Path, d.Version)
				}
				break
			}
		}
	} else {
		b.Fatal("build info not found")
	}
	session := setupSession(b, compression, protoVersion)
	err := session.Query(fmt.Sprintf("CREATE KEYSPACE IF NOT EXISTS %v WITH REPLICATION = {'class':'SimpleStrategy', 'replication_factor':1}", *keyspace)).Exec()
	if err != nil {
		b.Fatal(err)
	}
	err = session.Query(fmt.Sprintf("CREATE TABLE IF NOT EXISTS %v.%v (id bigint PRIMARY KEY, c1 UUID, c2 text, c3 bigint)", *keyspace, *table)).Exec()
	if err != nil {
		b.Fatal(err)
	}
	truncateTable(b, session)
	populateTable(b, short, session)
	return session
}

func truncateTable(b *testing.B, session *gocql.Session) {
	err := session.Query(fmt.Sprintf("TRUNCATE %v.%v", *keyspace, *table)).Exec()
	if err != nil {
		b.Fatal(err)
	}
	time.Sleep(1 * time.Second)
	err = session.Query(fmt.Sprintf("TRUNCATE %v.%v", *keyspace, *table)).Exec()
	if err != nil {
		b.Fatal(err)
	}
}

var dataRowsFlag = flag.Int("datarows", 1000000, "Number of rows to insert into db")

func populateTable(b *testing.B, short bool, session *gocql.Session) {
	finalCount := *dataRowsFlag
	j := 0
	wg := &sync.WaitGroup{}
	errCh := make(chan error, 128)
	for i := 0; i < 128; i++ {
		start := j
		var end int
		if i == 127 {
			end = finalCount
		} else {
			inc := finalCount / 128
			if inc <= 0 {
				inc = 1
			}
			end = start + inc
		}
		if end >= finalCount {
			end = finalCount
		}
		j = end + 1
		if end-start <= 0 {
			break
		}

		wg.Add(1)
		go func() {
			defer wg.Done()
			r := rand.New(rand.NewSource(int64(start)))
			for g := start; g <= end; g++ {
				uuidBytes := make([]byte, 16)
				r.Read(uuidBytes)
				uuid, err := gocql.UUIDFromBytes(uuidBytes)
				if err != nil {
					errCh <- err
					return
				}
				var strLen int
				if short {
					strLen = 16
				} else {
					strLen = 4096
				}
				err = session.Query(fmt.Sprintf("INSERT INTO %v.%v (id, c1, c2, c3) VALUES (?,?,?,?)", *keyspace, *table), g, uuid, randStringBytes(r, strLen), g*g).Exec()
				if err != nil {
					errCh <- err
					return
				}
			}
		}()

	}
	wg.Wait()
	done := false
	errs := make([]any, 0)
	for !done {
		select {
		case err := <-errCh:
			if err != nil {
				errs = append(errs, err)
			}
		default:
			done = true
		}
	}
	if len(errs) > 0 {
		b.Fatal(errs...)
	}
	fmt.Printf("Rows inserted before benchmark: %d\n", *dataRowsFlag)
}

func run(b *testing.B, session *gocql.Session, i int64) {
	iter := session.Query(fmt.Sprintf("SELECT * FROM %v.%v WHERE id = ?", *keyspace, *table), i%int64(*dataRowsFlag)).Iter()
	if iter.NumRows() != 1 {
		b.Fatal(fmt.Sprintf("num rows should be 1 but is %d", iter.NumRows()))
	}
	_, err := iter.RowData()
	if err != nil {
		b.Fatal(err)
	}
}

func warmup(b *testing.B, session *gocql.Session) {
	for i := 0; i < 24; i++ {
		run(b, session, int64(i))
	}
}

func BenchmarkSelectShortV4(b *testing.B) {
	session := setupSelect(b, true, false, 4)
	warmup(b, session)
	i := int64(0)
	for b.Loop() {
		run(b, session, i)
		i++
	}

	fmt.Printf("Queries run: %d\n", b.N)
}

func BenchmarkSelectLongV4(b *testing.B) {
	session := setupSelect(b, false, false, 4)
	warmup(b, session)
	i := int64(0)
	for b.Loop() {
		run(b, session, i)
		i++
	}

	fmt.Printf("Queries run: %d\n", b.N)
}

func BenchmarkSelectShortCompressionV4(b *testing.B) {
	session := setupSelect(b, true, true, 4)
	warmup(b, session)
	i := int64(0)
	for b.Loop() {
		run(b, session, i)
		i++
	}

	fmt.Printf("Queries run: %d\n", b.N)
}

func BenchmarkSelectLongCompressionV4(b *testing.B) {
	session := setupSelect(b, false, true, 4)
	warmup(b, session)
	i := int64(0)
	for b.Loop() {
		run(b, session, i)
		i++
	}

	fmt.Printf("Queries run: %d\n", b.N)
}

func BenchmarkSelectShortV5(b *testing.B) {
	session := setupSelect(b, true, false, 5)
	warmup(b, session)
	i := int64(0)
	for b.Loop() {
		run(b, session, i)
		i++
	}

	fmt.Printf("Queries run: %d\n", b.N)
}

func BenchmarkSelectLongV5(b *testing.B) {
	session := setupSelect(b, false, false, 5)
	warmup(b, session)
	i := int64(0)
	for b.Loop() {
		run(b, session, i)
		i++
	}

	fmt.Printf("Queries run: %d\n", b.N)
}

func BenchmarkSelectShortCompressionV5(b *testing.B) {
	session := setupSelect(b, true, true, 5)
	warmup(b, session)
	i := int64(0)
	for b.Loop() {
		run(b, session, i)
		i++
	}

	fmt.Printf("Queries run: %d\n", b.N)
}

func BenchmarkSelectLongCompressionV5(b *testing.B) {
	session := setupSelect(b, false, true, 5)
	warmup(b, session)
	i := int64(0)
	for b.Loop() {
		run(b, session, i)
		i++
	}

	fmt.Printf("Queries run: %d\n", b.N)
}
