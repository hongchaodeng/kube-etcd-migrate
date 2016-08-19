package main

import (
	"encoding/binary"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path"
	"time"

	"github.com/coreos/etcd/client"
	etcdErr "github.com/coreos/etcd/error"
	"github.com/coreos/etcd/etcdserver"
	pb "github.com/coreos/etcd/etcdserver/etcdserverpb"
	"github.com/coreos/etcd/lease"
	"github.com/coreos/etcd/mvcc"
	"github.com/coreos/etcd/mvcc/backend"
	"github.com/coreos/etcd/mvcc/mvccpb"
	"github.com/coreos/etcd/pkg/pbutil"
	"github.com/coreos/etcd/raft/raftpb"
	"github.com/coreos/etcd/snap"
	"github.com/coreos/etcd/store"
	"github.com/coreos/etcd/wal"
	"github.com/coreos/etcd/wal/walpb"
	"github.com/gogo/protobuf/proto"
)

const ExitError = 1

var (
	migrateDatadir     string
	migrateWALdir      string
	migrateTransformer string
)

func init() {
	flag.StringVar(&migrateDatadir, "data-dir", "", "Path to the data directory")
	flag.StringVar(&migrateWALdir, "wal-dir", "", "Path to the WAL directory")
	flag.StringVar(&migrateTransformer, "transformer", "", "Path to the user-provided transformer program")
	flag.Parse()
}

func main() {
	var (
		writer io.WriteCloser
		reader io.ReadCloser
		errc   chan error
	)
	if migrateTransformer != "" {
		writer, reader, errc = startTransformer()
	} else {
		fmt.Println("using default transformer")
		writer, reader, errc = defaultTransformer()
	}

	st := rebuildStoreV2()
	be := prepareBackend()
	defer be.Close()

	ls := lease.NewLessor(be, timeout)
	l, err := ls.Grant(1, timeout)
	if err != nil {
		panic(err)
	}
	leaseID = l.ID

	maxIndexc := make(chan uint64, 1)
	go func() {
		maxIndexc <- writeStore(writer, st)
		writer.Close()
	}()

	readKeys(reader, be)
	mvcc.UpdateConsistentIndex(be, <-maxIndexc)
	if err := <-errc; err != nil {
		fmt.Println("failed to transform keys")
		ExitWithError(ExitError, err)
	}

	fmt.Println("finished transforming keys")
}

var leaseID lease.LeaseID

const timeout = 60 * 60

func prepareBackend() backend.Backend {
	dbpath := path.Join(migrateDatadir, "member", "snap", "db")
	be := backend.New(dbpath, time.Second, 10000)
	tx := be.BatchTx()
	tx.Lock()
	tx.UnsafeCreateBucket([]byte("key"))
	tx.UnsafeCreateBucket([]byte("meta"))
	tx.Unlock()
	return be
}

func rebuildStoreV2() store.Store {
	waldir := migrateWALdir
	if len(waldir) == 0 {
		waldir = path.Join(migrateDatadir, "member", "wal")
	}
	snapdir := path.Join(migrateDatadir, "member", "snap")

	ss := snap.New(snapdir)
	snapshot, err := ss.Load()
	if err != nil && err != snap.ErrNoSnapshot {
		ExitWithError(ExitError, err)
	}

	var walsnap walpb.Snapshot
	if snapshot != nil {
		walsnap.Index, walsnap.Term = snapshot.Metadata.Index, snapshot.Metadata.Term
	}

	w, err := wal.OpenForRead(waldir, walsnap)
	if err != nil {
		ExitWithError(ExitError, err)
	}
	defer w.Close()

	_, _, ents, err := w.ReadAll()
	if err != nil {
		ExitWithError(ExitError, err)
	}

	st := store.New()
	if snapshot != nil {
		err := st.Recovery(snapshot.Data)
		if err != nil {
			ExitWithError(ExitError, err)
		}
	}

	applier := etcdserver.NewApplierV2(st, nil)
	for _, ent := range ents {
		if ent.Type != raftpb.EntryNormal {
			continue
		}

		var raftReq pb.InternalRaftRequest
		if !pbutil.MaybeUnmarshal(&raftReq, ent.Data) { // backward compatible
			var r pb.Request
			pbutil.MustUnmarshal(&r, ent.Data)
			applyRequest(&r, applier)
		} else {
			if raftReq.V2 != nil {
				req := raftReq.V2
				applyRequest(req, applier)
			}
		}
	}

	return st
}

func applyRequest(r *pb.Request, applyV2 etcdserver.ApplierV2) {
	toTTLOptions(r)
	switch r.Method {
	case "POST":
		applyV2.Post(r)
	case "PUT":
		applyV2.Put(r)
	case "DELETE":
		applyV2.Delete(r)
	case "QGET":
		applyV2.QGet(r)
	case "SYNC":
		applyV2.Sync(r)
	default:
		panic("unknown command")
	}
}

func toTTLOptions(r *pb.Request) store.TTLOptionSet {
	refresh, _ := pbutil.GetBool(r.Refresh)
	ttlOptions := store.TTLOptionSet{Refresh: refresh}
	if r.Expiration != 0 {
		ttlOptions.ExpireTime = time.Unix(0, r.Expiration)
	}
	return ttlOptions
}

func writeStore(w io.Writer, st store.Store) uint64 {
	all, err := st.Get("/1", true, true)
	if err != nil {
		if eerr, ok := err.(*etcdErr.Error); ok && eerr.ErrorCode == etcdErr.EcodeKeyNotFound {
			fmt.Println("no v2 keys to migrate")
			os.Exit(0)
		}
		ExitWithError(ExitError, err)
	}
	return writeKeys(w, all.Node)
}

func writeKeys(w io.Writer, n *store.NodeExtern) uint64 {
	maxIndex := n.ModifiedIndex

	nodes := n.Nodes
	// remove store v2 bucket prefix
	n.Key = n.Key[2:]
	if n.Key == "" {
		n.Key = "/"
	}
	if n.Dir {
		n.Nodes = nil
	}
	b, err := json.Marshal(n)
	if err != nil {
		ExitWithError(ExitError, err)
	}
	fmt.Fprintf(w, string(b))
	for _, nn := range nodes {
		max := writeKeys(w, nn)
		if max > maxIndex {
			maxIndex = max
		}
	}
	return maxIndex
}

func readKeys(r io.Reader, be backend.Backend) error {
	for {
		length64, err := readInt64(r)
		if err != nil {
			if err == io.EOF {
				return nil
			}
			return err
		}

		buf := make([]byte, int(length64))
		if _, err = io.ReadFull(r, buf); err != nil {
			return err
		}

		var kv mvccpb.KeyValue
		err = proto.Unmarshal(buf, &kv)
		if err != nil {
			return err
		}

		mvcc.WriteKV(be, kv)
	}
}

func readInt64(r io.Reader) (int64, error) {
	var n int64
	err := binary.Read(r, binary.LittleEndian, &n)
	return n, err
}

func startTransformer() (io.WriteCloser, io.ReadCloser, chan error) {
	cmd := exec.Command(migrateTransformer)
	cmd.Stderr = os.Stderr

	writer, err := cmd.StdinPipe()
	if err != nil {
		ExitWithError(ExitError, err)
	}

	reader, rerr := cmd.StdoutPipe()
	if rerr != nil {
		ExitWithError(ExitError, rerr)
	}

	if err := cmd.Start(); err != nil {
		ExitWithError(ExitError, err)
	}

	errc := make(chan error, 1)

	go func() {
		errc <- cmd.Wait()
	}()

	return writer, reader, errc
}

func defaultTransformer() (io.WriteCloser, io.ReadCloser, chan error) {
	// transformer decodes v2 keys from sr
	sr, sw := io.Pipe()
	// transformer encodes v3 keys into dw
	dr, dw := io.Pipe()

	decoder := json.NewDecoder(sr)

	errc := make(chan error, 1)

	go func() {
		defer func() {
			sr.Close()
			dw.Close()
		}()

		for decoder.More() {
			node := &client.Node{}
			if err := decoder.Decode(node); err != nil {
				errc <- err
				return
			}

			kv := transform(node)
			if kv == nil {
				continue
			}

			data, err := proto.Marshal(kv)
			if err != nil {
				errc <- err
				return
			}
			buf := make([]byte, 8)
			binary.LittleEndian.PutUint64(buf, uint64(len(data)))
			if _, err := dw.Write(buf); err != nil {
				errc <- err
				return
			}
			if _, err := dw.Write(data); err != nil {
				errc <- err
				return
			}
		}

		errc <- nil
	}()

	return sw, dr, errc
}

func transform(n *client.Node) *mvccpb.KeyValue {
	const unKnownVersion = 1
	if n.Dir {
		return nil
	}
	kv := &mvccpb.KeyValue{
		Key:            []byte(n.Key),
		Value:          []byte(n.Value),
		CreateRevision: int64(n.CreatedIndex),
		ModRevision:    int64(n.ModifiedIndex),
		Version:        unKnownVersion,
		Lease:          int64(leaseID),
	}
	return kv
}

func ExitWithError(code int, err error) {
	fmt.Fprintln(os.Stderr, "Error: ", err)
	if cerr, ok := err.(*client.ClusterError); ok {
		fmt.Fprintln(os.Stderr, cerr.Detail())
	}
	os.Exit(code)
}
