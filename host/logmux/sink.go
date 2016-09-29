package logmux

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"sync"
	"text/template"
	"time"

	"github.com/boltdb/bolt"
	"github.com/flynn/flynn/discoverd/client"
	"github.com/flynn/flynn/logaggregator/client"
	"github.com/flynn/flynn/logaggregator/utils"
	"github.com/flynn/flynn/pkg/syslog/rfc6587"
	"gopkg.in/inconshreveable/log15.v2"
)

var SinkExistsError = errors.New("sink with that id already exists")
var SinkNotFoundError = errors.New("sink with that id couldn't be found")

type SinkManager struct {
	mtx    sync.RWMutex
	mux    *Mux
	logger log15.Logger
	sinks  map[string]Sink

	dbPath string
	db     *bolt.DB

	shutdownOnce sync.Once
	shutdownCh   chan struct{}
}

func NewSinkManager(dbPath string, mux *Mux, logger log15.Logger) *SinkManager {
	return &SinkManager{
		dbPath:     dbPath,
		mux:        mux,
		logger:     logger,
		sinks:      make(map[string]Sink),
		shutdownCh: make(chan struct{}),
	}
}

func (sm *SinkManager) OpenDB() error {
	sm.mtx.Lock()
	defer sm.mtx.Unlock()
	if sm.dbPath == "" {
		return nil
	}

	// open database file
	if err := os.MkdirAll(filepath.Dir(sm.dbPath), 0755); err != nil {
		return fmt.Errorf("could not mkdir for sink persistence db: %s", err)
	}
	db, err := bolt.Open(sm.dbPath, 0600, &bolt.Options{Timeout: 5 * time.Second})
	if err != nil {
		return fmt.Errorf("could not open sink persistence db: %s", err)
	}

	// create buckets if they don't already exist
	if err := db.Update(func(tx *bolt.Tx) error {
		tx.CreateBucketIfNotExists([]byte("sinks"))
		return nil
	}); err != nil {
		return fmt.Errorf("could not initialise sink persistence db: %s")
	}
	sm.db = db

	// restore previous state if any
	if err := sm.restore(); err != nil {
		return err
	}

	// start persistence routine
	go sm.persistSinks()
	return nil
}

func (sm *SinkManager) CloseDB() error {
	sm.mtx.Lock()
	defer sm.mtx.Unlock()

	sm.Shutdown()
	if sm.db == nil {
		return nil
	}
	if err := sm.db.Close(); err != nil {
		return err
	}
	sm.db = nil
	return nil
}

func (sm *SinkManager) Shutdown() {
	sm.shutdownOnce.Do(func() { close(sm.shutdownCh) })
}

type SinkInfo struct {
	ID     string            `json:"id"`
	Kind   string            `json:"kind"`
	Cursor *utils.HostCursor `json:"cursor,omitempty"`
	Config []byte            `json:"config"`
}

func (sm *SinkManager) restore() error {
	// read back from buckets into in-memory structure
	if err := sm.db.View(func(tx *bolt.Tx) error {
		sinkBucket := tx.Bucket([]byte("sinks"))
		if err := sinkBucket.ForEach(func(k, v []byte) error {
			sinkInfo := &SinkInfo{}
			if err := json.Unmarshal(v, sinkInfo); err != nil {
				return fmt.Errorf("failed to deserialize sink info: %s", err)
			}
			err := sm.addSink(string(k), sinkInfo, false)
			if err != nil {
				return err
			}
			return nil
		}); err != nil {
			return err
		}
		return nil
	}); err != nil {
		return err
	}
	return nil
}

func (sm *SinkManager) newSink(s *SinkInfo) (sink Sink, err error) {
	switch s.Kind {
	case "logaggregator":
		sink, err = NewLogAggregatorSink(s)
		if err != nil {
			return nil, err
		}
	case "tcp":
		sink, err = NewTCPSink(s)
		if err != nil {
			return nil, err
		}
	}
	return sink, nil
}

func (sm *SinkManager) persistSink(id string) error {
	if err := sm.db.Update(func(tx *bolt.Tx) error {
		sinkBucket := tx.Bucket([]byte("sinks"))
		k := []byte(id)

		// remove sink from database if not found in current sinks
		sink, sinkExists := sm.sinks[id]
		if !sinkExists {
			sinkBucket.Delete(k)
			return nil
		}

		// serialize sink info and persist to disk
		b, err := json.Marshal(sink.Info())
		if err != nil {
			return fmt.Errorf("failed to serialize sink info: %s", err)
		}
		err = sinkBucket.Put(k, b)
		if err != nil {
			return fmt.Errorf("failed to persist sink info to boltdb: %s", err)
		}
		return nil
	}); err != nil {
		return err
	}
	return nil
}

func (sm *SinkManager) persistSinks() {
	sm.logger.Info("starting sink persistence routine")
	ticker := time.NewTicker(60 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-sm.shutdownCh:
			return
		case <-ticker.C:
			sm.logger.Info("persisting sinks")
			sm.mtx.Lock()
			for id, _ := range sm.sinks {
				sm.persistSink(id)
			}
			sm.mtx.Unlock()
		}
	}
}

func (sm *SinkManager) AddSink(id string, s *SinkInfo) error {
	sm.mtx.Lock()
	defer sm.mtx.Unlock()

	return sm.addSink(id, s, true)
}

func (sm *SinkManager) addSink(id string, s *SinkInfo, persist bool) error {
	if _, ok := sm.sinks[id]; ok {
		return SinkExistsError
	}
	sink, err := sm.newSink(s)
	if err != nil {
		return err
	}
	sm.sinks[id] = sink
	if persist {
		if err := sm.persistSink(id); err != nil {
			return err
		}
	}
	go sm.mux.addSink(sink)
	return nil
}

func (sm *SinkManager) RemoveSink(id string) error {
	sm.mtx.Lock()
	defer sm.mtx.Unlock()

	return sm.removeSink(id)
}

func (sm *SinkManager) removeSink(id string) error {
	if s, ok := sm.sinks[id]; !ok {
		return SinkNotFoundError
	} else {
		s.Shutdown()
	}
	delete(sm.sinks, id)
	return sm.persistSink(id)
}

func (sm *SinkManager) StreamToAggregators(s discoverd.Service) error {
	log := sm.logger.New("fn", "StreamToAggregators")
	ch := make(chan *discoverd.Event)
	_, err := s.Watch(ch)
	if err != nil {
		log.Error("failed to connect to discoverd watch", "error", err)
		return err
	}
	log.Info("connected to discoverd watch")
	sm.mtx.RLock()
	initial := make(map[string]struct{})
	for id, sink := range sm.sinks {
		if sink.Info().Kind == "logaggregator" {
			initial[id] = struct{}{}
		}
	}
	sm.mtx.RUnlock()

	//tcpCfg, _ := json.Marshal(tcpConfig{
	//	Addr: "127.0.0.1:514",
	//})
	//tcpInfo := &SinkInfo{
	//	Kind:   "tcp",
	//	Config: tcpCfg,
	//}
	//sm.AddSink("tcptest", tcpInfo)
	go func() {
		for e := range ch {
			switch e.Kind {
			case discoverd.EventKindUp:
				if _, ok := initial[e.Instance.Addr]; ok {
					delete(initial, e.Instance.Addr)
					continue // skip adding as we already have this sink.
				}
				log.Info("connecting to new aggregator", "addr", e.Instance.Addr)
				cfg, _ := json.Marshal(logAggregatorConfig{Addr: e.Instance.Addr})
				info := &SinkInfo{Kind: "logaggregator", Config: cfg}
				sm.AddSink(e.Instance.Addr, info)
			case discoverd.EventKindDown:
				log.Info("disconnecting from aggregator", "addr", e.Instance.Addr)
				sm.RemoveSink(e.Instance.Addr)
			case discoverd.EventKindCurrent:
				for id := range initial {
					log.Info("removing stale aggregator", "addr", id)
					delete(initial, id)
					sm.RemoveSink(id)
				}
			}
		}
	}()
	return nil
}

type Sink interface {
	Info() *SinkInfo
	Connect() error
	Close()
	GetCursor(hostID string) (*utils.HostCursor, error)
	Write(m message) error
	Shutdown()
	ShutdownCh() chan struct{}
}

type LogAggregatorSink struct {
	id               string
	logger           log15.Logger
	addr             string
	conn             net.Conn
	aggregatorClient *client.Client

	shutdownOnce sync.Once
	shutdownCh   chan struct{}
}

type logAggregatorConfig struct {
	Addr string `json:"addr"`
}

func NewLogAggregatorSink(info *SinkInfo) (*LogAggregatorSink, error) {
	cfg := &logAggregatorConfig{}
	if err := json.Unmarshal(info.Config, cfg); err != nil {
		return nil, err
	}
	return &LogAggregatorSink{
		id:         info.ID,
		addr:       cfg.Addr,
		shutdownCh: make(chan struct{}),
	}, nil
}

func (s *LogAggregatorSink) Info() *SinkInfo {
	config, _ := json.Marshal(logAggregatorConfig{Addr: s.addr})
	return &SinkInfo{
		ID:     s.id,
		Kind:   "logaggregator",
		Config: config,
	}
}

func (s *LogAggregatorSink) Connect() error {
	// Connect TCP connection to aggregator
	// TODO(titanous): add dial timeout
	conn, err := net.Dial("tcp", s.addr)
	if err != nil {
		return err
	}
	// Connect to logaggregator HTTP endpoint
	host, _, _ := net.SplitHostPort(s.addr)
	c, err := client.New("http://" + host)
	if err != nil {
		if conn != nil {
			conn.Close()
		}
		return err
	}
	s.conn = conn
	s.aggregatorClient = c
	return nil
}

func (s *LogAggregatorSink) Close() {
	s.conn.Close()
}

func (s *LogAggregatorSink) GetCursor(hostID string) (*utils.HostCursor, error) {
	cursors, err := s.aggregatorClient.GetCursors()
	if err != nil {
		return nil, err
	}
	var aggCursor *utils.HostCursor
	if c, ok := cursors[hostID]; ok {
		aggCursor = &c
	}
	return aggCursor, nil
}

func (s *LogAggregatorSink) Write(m message) error {
	_, err := s.conn.Write(rfc6587.Bytes(m.Message))
	return err
}

func (s *LogAggregatorSink) Shutdown() {
	s.shutdownOnce.Do(func() { close(s.shutdownCh) })
}

func (s *LogAggregatorSink) ShutdownCh() chan struct{} {
	return s.shutdownCh
}

// TCPSink is a flexible sink that can connect to TCP/TLS endpoints and write log messages using
// a defined format string. It can be used to connect to rsyslog and similar services that accept TCP connections.
type TCPSink struct {
	id       string
	addr     string
	template string
	cursor   *utils.HostCursor

	conn         net.Conn
	shutdownOnce sync.Once
	shutdownCh   chan struct{}
}

type tcpConfig struct {
	Addr     string `json:"addr"`
	Template string `json:"template"`
}

func NewTCPSink(info *SinkInfo) (*TCPSink, error) {
	cfg := &tcpConfig{}
	if err := json.Unmarshal(info.Config, cfg); err != nil {
		return nil, err
	}
	return &TCPSink{
		id:         info.ID,
		addr:       cfg.Addr,
		template:   cfg.Template,
		cursor:     info.Cursor,
		shutdownCh: make(chan struct{}),
	}, nil
}

func (s *TCPSink) Info() *SinkInfo {
	config, _ := json.Marshal(tcpConfig{Addr: s.addr, Template: s.template})
	return &SinkInfo{
		ID:     s.id,
		Kind:   "tcp",
		Config: config,
	}
}

func (s *TCPSink) Connect() error {
	conn, err := net.Dial("tcp", s.addr)
	if err != nil {
		return err
	}
	s.conn = conn
	return nil
}

func (s *TCPSink) GetCursor(_ string) (*utils.HostCursor, error) {
	return s.cursor, nil
}

// TODO(jpg) work out what fields we want in this
// or if we just want to give it the message instead...
// we also need to do all the stuff to get the app name etc into the messages in the first place
type Data struct {
	AppName string
}

func (s *TCPSink) Write(m message) error {
	// format the message using template if provided
	if s.template != "" {
		tmpl, err := template.New("").Parse(s.template)
		if err != nil {
			return err
		}
		data := Data{
			AppName: "test",
		}
		var buf *bytes.Buffer
		tmpl.Execute(buf, data)
		m.Message.Msg = buf.Bytes()
	}
	_, err := s.conn.Write(rfc6587.Bytes(m.Message))
	if err != nil {
		return err
	}
	s.cursor = m.HostCursor
	return nil
}

func (s *TCPSink) Close() {
	if s.conn != nil {
		s.conn.Close()
	}
}

func (s *TCPSink) Shutdown() {
	s.shutdownOnce.Do(func() { close(s.shutdownCh) })
}

func (s *TCPSink) ShutdownCh() chan struct{} {
	return s.shutdownCh
}
