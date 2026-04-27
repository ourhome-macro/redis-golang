package database

import (
	"MiddlewareSelf/redis/aof"
	"MiddlewareSelf/redis/resp"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	defaultReplicationBacklogSize = 1 << 20
	replicaSendQueueSize          = 64
)

type ReplicaMetadata struct {
	ListeningPort int
	Capabilities  []string
	RemoteAddr    string
}

type ReplicaSession struct {
	repl *replicationState
	id   int64
}

func (s *ReplicaSession) Activate() error {
	if s == nil || s.repl == nil {
		return nil
	}
	return s.repl.activateReplica(s.id)
}

func (s *ReplicaSession) Ack(offset int64) {
	if s == nil || s.repl == nil {
		return
	}
	s.repl.updateReplicaAck(s.id, offset)
}

func (s *ReplicaSession) Close() {
	if s == nil || s.repl == nil {
		return
	}
	s.repl.removeReplica(s.id)
}

type ReplicationInfo struct {
	Role                   string
	ConnectedReplicas      int
	MasterReplID           string
	MasterReplOffset       int64
	BacklogActive          bool
	BacklogSize            int
	BacklogFirstByteOffset int64
	BacklogHistLen         int64
	Replicas               []ReplicaInfo
}

type ReplicaInfo struct {
	IP     string
	Port   int
	State  string
	Offset int64
	Lag    int64
}

type replicationState struct {
	mu          sync.Mutex
	replID      string
	offset      int64
	backlog     []byte
	firstOffset int64
	backlogSize int
	replicas    map[int64]*replicaConn
	nextID      int64
}

type replicaConn struct {
	id            int64
	conn          net.Conn
	sendCh        chan []byte
	remoteAddr    string
	listeningPort int
	capabilities  []string
	state         string
	catchingUp    bool
	pending       []byte
	ackOffset     int64
	lastAck       time.Time
	closeOnce     sync.Once
}

func newReplicationState(backlogSize int) *replicationState {
	if backlogSize <= 0 {
		backlogSize = defaultReplicationBacklogSize
	}
	return &replicationState{
		replID:      newReplicationID(),
		backlogSize: backlogSize,
		replicas:    make(map[int64]*replicaConn),
	}
}

func newReplicationID() string {
	buf := make([]byte, 20)
	if _, err := rand.Read(buf); err == nil {
		return hex.EncodeToString(buf)
	}
	return fmt.Sprintf("%040x", time.Now().UnixNano())
}

func newReplicaConn(id int64, conn net.Conn, meta ReplicaMetadata, offset int64) *replicaConn {
	replica := &replicaConn{
		id:            id,
		conn:          conn,
		sendCh:        make(chan []byte, replicaSendQueueSize),
		remoteAddr:    meta.RemoteAddr,
		listeningPort: meta.ListeningPort,
		capabilities:  append([]string(nil), meta.Capabilities...),
		state:         "sync",
		catchingUp:    true,
		ackOffset:     offset,
		lastAck:       time.Now(),
	}
	go replica.writeLoop()
	return replica
}

func (r *replicaConn) writeLoop() {
	for payload := range r.sendCh {
		if len(payload) == 0 {
			continue
		}
		_ = r.conn.SetWriteDeadline(time.Now().Add(5 * time.Second))
		if _, err := r.conn.Write(payload); err != nil {
			r.close()
			return
		}
	}
}

func (r *replicaConn) enqueue(payload []byte) error {
	if len(payload) == 0 {
		return nil
	}
	select {
	case r.sendCh <- payload:
		return nil
	default:
		return fmt.Errorf("replica send queue is full")
	}
}

func (r *replicaConn) close() {
	r.closeOnce.Do(func() {
		close(r.sendCh)
		_ = r.conn.Close()
	})
}

func (r *replicationState) Close() {
	r.mu.Lock()
	replicas := make([]*replicaConn, 0, len(r.replicas))
	for id, replica := range r.replicas {
		delete(r.replicas, id)
		replicas = append(replicas, replica)
	}
	r.mu.Unlock()

	for _, replica := range replicas {
		replica.close()
	}
}

func (r *replicationState) beginFullSync(conn net.Conn, meta ReplicaMetadata) (*ReplicaSession, string, int64) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.nextID++
	replica := newReplicaConn(r.nextID, conn, meta, r.offset)
	r.replicas[replica.id] = replica
	return &ReplicaSession{repl: r, id: replica.id}, r.replID, r.offset
}

func (r *replicationState) tryPartialSync(conn net.Conn, meta ReplicaMetadata, replID string, offset int64) (*ReplicaSession, []byte, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if replID != r.replID {
		return nil, nil, false
	}
	backlog, ok := r.backlogSinceOffsetLocked(offset)
	if !ok {
		return nil, nil, false
	}

	r.nextID++
	replica := newReplicaConn(r.nextID, conn, meta, offset)
	r.replicas[replica.id] = replica
	return &ReplicaSession{repl: r, id: replica.id}, backlog, true
}

func (r *replicationState) activateReplica(id int64) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	replica, ok := r.replicas[id]
	if !ok {
		return fmt.Errorf("replica session %d not found", id)
	}

	if len(replica.pending) > 0 {
		payload := append([]byte(nil), replica.pending...)
		if err := replica.enqueue(payload); err != nil {
			delete(r.replicas, id)
			replica.close()
			return err
		}
		replica.pending = nil
	}

	replica.catchingUp = false
	replica.state = "online"
	return nil
}

func (r *replicationState) updateReplicaAck(id int64, offset int64) {
	r.mu.Lock()
	defer r.mu.Unlock()

	replica, ok := r.replicas[id]
	if !ok {
		return
	}
	replica.ackOffset = offset
	replica.lastAck = time.Now()
}

func (r *replicationState) removeReplica(id int64) {
	r.mu.Lock()
	replica, ok := r.replicas[id]
	if ok {
		delete(r.replicas, id)
	}
	r.mu.Unlock()

	if ok {
		replica.close()
	}
}

func (r *replicationState) Propagate(dbIndex int, commands [][][]byte) {
	payload := encodeReplicationCommands(dbIndex, commands)
	if len(payload) == 0 {
		return
	}

	r.mu.Lock()
	r.appendBacklogLocked(payload)
	liveReplicas := make([]*replicaConn, 0, len(r.replicas))
	for _, replica := range r.replicas {
		if replica.catchingUp {
			replica.pending = append(replica.pending, payload...)
			continue
		}
		liveReplicas = append(liveReplicas, replica)
	}
	r.mu.Unlock()

	for _, replica := range liveReplicas {
		if err := replica.enqueue(payload); err != nil {
			r.removeReplica(replica.id)
		}
	}
}

func (r *replicationState) Info() ReplicationInfo {
	r.mu.Lock()
	defer r.mu.Unlock()

	info := ReplicationInfo{
		Role:              "master",
		ConnectedReplicas: len(r.replicas),
		MasterReplID:      r.replID,
		MasterReplOffset:  r.offset,
		BacklogActive:     true,
		BacklogSize:       r.backlogSize,
		BacklogHistLen:    int64(len(r.backlog)),
	}
	if len(r.backlog) > 0 {
		info.BacklogFirstByteOffset = r.firstOffset
	}

	info.Replicas = make([]ReplicaInfo, 0, len(r.replicas))
	now := time.Now()
	for _, replica := range r.replicas {
		host, _, err := net.SplitHostPort(replica.remoteAddr)
		if err != nil {
			host = replica.remoteAddr
		}
		lag := int64(now.Sub(replica.lastAck).Seconds())
		if lag < 0 {
			lag = 0
		}
		info.Replicas = append(info.Replicas, ReplicaInfo{
			IP:     host,
			Port:   replica.listeningPort,
			State:  replica.state,
			Offset: replica.ackOffset,
			Lag:    lag,
		})
	}

	return info
}

func (r *replicationState) appendBacklogLocked(payload []byte) {
	if len(payload) == 0 {
		return
	}

	prevOffset := r.offset
	r.offset += int64(len(payload))
	if len(r.backlog) == 0 {
		r.firstOffset = prevOffset + 1
	}
	r.backlog = append(r.backlog, payload...)

	if excess := len(r.backlog) - r.backlogSize; excess > 0 {
		trimmed := append([]byte(nil), r.backlog[excess:]...)
		r.backlog = trimmed
		r.firstOffset += int64(excess)
	}
}

func (r *replicationState) backlogSinceOffsetLocked(offset int64) ([]byte, bool) {
	if offset < 0 || offset > r.offset {
		return nil, false
	}
	if len(r.backlog) == 0 {
		return nil, offset == r.offset
	}

	start := offset + 1
	if start < r.firstOffset {
		return nil, false
	}
	if start > r.offset+1 {
		return nil, false
	}
	if start == r.offset+1 {
		return nil, true
	}

	index := int(start - r.firstOffset)
	if index < 0 || index > len(r.backlog) {
		return nil, false
	}
	return append([]byte(nil), r.backlog[index:]...), true
}

func encodeReplicationCommands(dbIndex int, commands [][][]byte) []byte {
	if len(commands) == 0 {
		return nil
	}

	selectBytes := resp.MakeArrayReply(makeCommand("SELECT", strconv.Itoa(dbIndex))).ToBytes()
	out := make([]byte, 0, len(selectBytes)*len(commands))
	for _, args := range commands {
		out = append(out, selectBytes...)
		out = append(out, resp.MakeArrayReply(args).ToBytes()...)
	}
	return out
}

func encodeRewriteCommandStream(commands []aof.RewriteCommand) []byte {
	out := make([]byte, 0)
	for _, cmd := range commands {
		out = append(out, resp.MakeArrayReply(cmd.Args).ToBytes()...)
	}
	return out
}

func (db *Db) BeginFullResync(conn net.Conn, meta ReplicaMetadata) (*ReplicaSession, string, int64, []byte, error) {
	db.mu.Lock()
	defer db.mu.Unlock()

	commands, err := db.snapshotForRewrite()
	if err != nil {
		return nil, "", 0, nil, err
	}
	session, replID, offset := db.replication.beginFullSync(conn, meta)
	return session, replID, offset, encodeRewriteCommandStream(commands), nil
}

func (db *Db) TryPartialResync(conn net.Conn, meta ReplicaMetadata, replID string, offset int64) (*ReplicaSession, []byte, bool) {
	return db.replication.tryPartialSync(conn, meta, replID, offset)
}

func (db *Db) replicationInfoBytes() []byte {
	info := db.replication.Info()

	lines := []string{
		"# Replication",
		fmt.Sprintf("role:%s", info.Role),
		fmt.Sprintf("connected_slaves:%d", info.ConnectedReplicas),
		fmt.Sprintf("master_replid:%s", info.MasterReplID),
		fmt.Sprintf("master_repl_offset:%d", info.MasterReplOffset),
		"second_repl_offset:-1",
		fmt.Sprintf("repl_backlog_active:%d", boolToInt(info.BacklogActive)),
		fmt.Sprintf("repl_backlog_size:%d", info.BacklogSize),
		fmt.Sprintf("repl_backlog_first_byte_offset:%d", info.BacklogFirstByteOffset),
		fmt.Sprintf("repl_backlog_histlen:%d", info.BacklogHistLen),
	}

	for i, replica := range info.Replicas {
		lines = append(lines, fmt.Sprintf(
			"slave%d:ip=%s,port=%d,state=%s,offset=%d,lag=%d",
			i,
			replica.IP,
			replica.Port,
			replica.State,
			replica.Offset,
			replica.Lag,
		))
	}

	return []byte(strings.Join(lines, "\r\n") + "\r\n")
}
