package policy

// Request is the identity the sidecar matches against buckets.
type Request struct {
	Caller   string
	Callee   string
	Endpoint string
	Priority Priority
}

// Table is an immutable, precompiled lookup from request to bucket.
type Table struct {
	Version uint64
	byMatch map[Match]*Bucket
	byID    map[uint32]*Bucket
}

// Compile builds a Table from a validated policy file.
func Compile(pf *File, version uint64) *Table {
	t := &Table{
		Version: version,
		byMatch: make(map[Match]*Bucket, len(pf.Buckets)),
		byID:    make(map[uint32]*Bucket, len(pf.Buckets)),
	}
	for i := range pf.Buckets {
		b := &pf.Buckets[i]
		t.byMatch[b.Match] = b
		t.byID[b.ID] = b
	}
	return t
}

// Match returns the most specific bucket for req, or nil.
func (t *Table) Match(req Request) *Bucket {
	for _, m := range [...]Match{
		{Caller: req.Caller, Callee: req.Callee, Endpoint: req.Endpoint},
		{Caller: req.Caller, Callee: req.Callee},
		{Callee: req.Callee, Endpoint: req.Endpoint},
		{Callee: req.Callee},
	} {
		if b, ok := t.byMatch[m]; ok {
			return b
		}
	}
	return nil
}

// Bucket looks up a bucket by ID.
func (t *Table) Bucket(id uint32) *Bucket { return t.byID[id] }

// Buckets returns all buckets, in no particular order.
func (t *Table) Buckets() []*Bucket {
	out := make([]*Bucket, 0, len(t.byID))
	for _, b := range t.byID {
		out = append(out, b)
	}
	return out
}
