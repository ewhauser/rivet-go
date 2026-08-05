package sqlitesocket

import "testing"

// Seeds are hand-assembled valid frames per schema/v1.bare: a u16
// little-endian vbare version prefix, then BARE payload (varint uints,
// little-endian fixed-width integers, length-prefixed strings).
func helloOKSeed() []byte {
	return []byte{
		0x01, 0x00, // version 1
		0x00,                   // ServerHello tag: HelloOk
		0x00, 0x00, 0x00, 0x02, // maxFrameBytes = 32 MiB (LE)
	}
}

func execOKSeed() []byte {
	return []byte{
		0x01, 0x00, // version 1
		0x00,                   // ServerFrame tag: Response
		0x2a, 0x00, 0x00, 0x00, // request id 42
		0x00, // payload tag: SqliteExecOk
	}
}

func queryOKSeed() []byte {
	return []byte{
		0x01, 0x00, // version 1
		0x00,                   // ServerFrame tag: Response
		0x01, 0x00, 0x00, 0x00, // request id 1
		0x01,      // payload tag: SqliteQueryOk
		0x01,      // one column
		0x01, 'a', // column name "a"
		0x01,                                           // one row
		0x01,                                           // one value in row
		0x01,                                           // SqlValue tag: integer
		0x07, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // 7
		0x02, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // changes = 2
		0x00, // lastInsertRowId: none
	}
}

func sqlErrorSeed() []byte {
	frame := []byte{
		0x01, 0x00, // version 1
		0x00,                   // ServerFrame tag: Response
		0x05, 0x00, 0x00, 0x00, // request id 5
		0x05,                   // payload tag: SqlError
		0x13, 0x00, 0x00, 0x00, // sqlite code 19 (constraint)
		0x00, 0x00, 0x00, 0x00, // statement index 0
		0x06, // message length 6
	}
	return append(frame, []byte("failed")...)
}

// FuzzDecodeSocketFrames drives both BARE decode entry points the socket
// client uses on engine-supplied bytes. Invariant: error or result, never a
// panic or hang, for arbitrary input.
func FuzzDecodeSocketFrames(f *testing.F) {
	for _, seed := range [][]byte{
		helloOKSeed(),
		execOKSeed(),
		queryOKSeed(),
		sqlErrorSeed(),
		{},
		{0x01, 0x00},
	} {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, data []byte) {
		_, _ = decodeHello(data)
		_, _ = decodeResponse(data)
	})
}

// The seeds must decode successfully on the paths they target, so the fuzzer
// starts from the deep interior of the decoder rather than early rejections.
func TestFuzzSeedsAreValid(t *testing.T) {
	if _, err := decodeHello(helloOKSeed()); err != nil {
		t.Fatalf("hello seed rejected: %v", err)
	}
	for name, seed := range map[string][]byte{
		"exec ok":   execOKSeed(),
		"query ok":  queryOKSeed(),
		"sql error": sqlErrorSeed(),
	} {
		result, err := decodeResponse(seed)
		if name == "sql error" {
			if err != nil || result.err == nil {
				t.Fatalf("%s seed: decode err=%v structured=%v", name, err, result.err)
			}
			continue
		}
		if err != nil {
			t.Fatalf("%s seed rejected: %v", name, err)
		}
	}
}
