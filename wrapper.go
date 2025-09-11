package main

import (
	"time"
)

type ClusterConfig struct {
	// addresses for the initial connections. It is recommended to use the value set in
	// the Cassandra config for broadcast_address or listen_address, an IP address not
	// a domain name. This is because events from Cassandra will use the configured IP
	// address, which is used to index connected hosts. If the domain name specified
	// resolves to more than 1 IP address then the driver may connect multiple times to
	// the same host, and will not mark the node being down or up from events.
	Hosts []string

	// CQL version (default: 3.0.0)
	CQLVersion string

	// ProtoVersion sets the version of the native protocol to use, this will
	// enable features in the driver for specific protocol versions, generally this
	// should be set to a known version (2,3,4) for the cluster being connected to.
	//
	// If it is 0 or unset (the default) then the driver will attempt to discover the
	// highest supported protocol for the cluster. In clusters with nodes of different
	// versions the protocol selected is not defined (ie, it can be any of the supported in the cluster)
	ProtoVersion int

	// Timeout limits the time spent on the client side while executing a query.
	// Specifically, query or batch execution will return an error if the client does not receive a response
	// from the server within the Timeout period.
	// Timeout is also used to configure the read timeout on the underlying network connection.
	// Client Timeout should always be higher than the request timeouts configured on the server,
	// so that retries don't overload the server.
	// Timeout has a default value of 11 seconds, which is higher than default server timeout for most query types.
	// Timeout is not applied to requests during initial connection setup, see ConnectTimeout.
	Timeout time.Duration

	// ConnectTimeout limits the time spent during connection setup.
	// During initial connection setup, internal queries, AUTH requests will return an error if the client
	// does not receive a response within the ConnectTimeout period.
	// ConnectTimeout is applied to the connection setup queries independently.
	// ConnectTimeout also limits the duration of dialing a new TCP connection
	// in case there is no Dialer nor HostDialer configured.
	// ConnectTimeout has a default value of 11 seconds.
	ConnectTimeout time.Duration

	// WriteTimeout limits the time the driver waits to write a request to a network connection.
	// WriteTimeout should be lower than or equal to Timeout.
	// WriteTimeout defaults to the value of Timeout.
	WriteTimeout time.Duration

	// Port used when dialing.
	// Default: 9042
	Port int

	// Initial keyspace. Optional.
	Keyspace string

	// Number of connections per host.
	// Default: 2
	NumConns int

	// The keepalive period to use, enabled if > 0 (default: 0)
	// SocketKeepalive is used to set up the default dialer and is ignored if Dialer or HostDialer is provided.
	SocketKeepalive time.Duration

	// Maximum cache size for prepared statements globally for gocql.
	// Default: 1000
	MaxPreparedStmts int

	// Maximum cache size for query info about statements for each session.
	// Default: 1000
	MaxRoutingKeyInfo int

	// Default page size to use for created sessions.
	// Default: 5000
	PageSize int

	// Sends a client side timestamp for all requests which overrides the timestamp at which it arrives at the server.
	// Default: true, only enabled for protocol 3 and above.
	DefaultTimestamp bool

	// If not zero, gocql attempt to reconnect known DOWN nodes in every ReconnectInterval.
	ReconnectInterval time.Duration

	// The maximum amount of time to wait for schema agreement in a cluster after
	// receiving a schema change frame. (default: 60s)
	MaxWaitSchemaAgreement time.Duration

	// If IgnorePeerAddr is true and the address in system.peers does not match
	// the supplied host by either initial hosts or discovered via events then the
	// host will be replaced with the supplied address.
	//
	// For example if an event comes in with host=10.0.0.1 but when looking up that
	// address in system.local or system.peers returns 127.0.0.1, the peer will be
	// set to 10.0.0.1 which is what will be used to connect to.
	IgnorePeerAddr bool

	// If DisableInitialHostLookup then the driver will not attempt to get host info
	// from the system.peers table, this will mean that the driver will connect to
	// hosts supplied and will not attempt to lookup the hosts information, this will
	// mean that data_centre, rack and token information will not be available and as
	// such host filtering and token aware query routing will not be available.
	DisableInitialHostLookup bool

	// Configure events the driver will register for
	Events struct {
		// disable registering for status events (node up/down)
		DisableNodeStatusEvents bool
		// disable registering for topology events (node added/removed/moved)
		DisableTopologyEvents bool
		// disable registering for schema events (keyspace/table/function removed/created/updated)
		DisableSchemaEvents bool
	}

	// DisableSkipMetadata will override the internal result metadata cache so that the driver does not
	// send skip_metadata for queries, this means that the result will always contain
	// the metadata to parse the rows and will not reuse the metadata from the prepared
	// statement.
	//
	// See https://issues.apache.org/jira/browse/CASSANDRA-10786
	DisableSkipMetadata bool

	// Default idempotence for queries
	DefaultIdempotence bool

	// The time to wait for frames before flushing the frames connection to Cassandra.
	// Can help reduce syscall overhead by making less calls to write. Set to 0 to
	// disable.
	//
	// (default: 200 microseconds)
	WriteCoalesceWaitTime time.Duration

	// internal config for testing
	disableControlConn bool

	Compression bool
}

type Session interface {
	Close()
	Exec(qry string, args ...interface{}) error
	Query(qry string, args []interface{}, dest ...interface{}) error
}
