package wire

import (
	"encoding/binary"
	"fmt"
)

const (
	// The envelope schema needs depth 4; 64 leaves headroom without permitting
	// stack abuse.
	maxScanDepth = 64
	// A native poll returns at most 64 events. Capping every array at that same
	// value prevents a valid-sized hostile input from amplifying into a much
	// larger []Event allocation inside msgpack.Unmarshal.
	maxArrayEntries = 64
	// Event maps are small, but metadata is extensible. This bound is well above
	// the M1 schema while still preventing allocation amplification.
	maxMapEntries = 1_024
	// FFI-BOUNDARY.md fixes streaming chunks at 1 MiB. Strings, binary values,
	// and extension payloads may not bypass that boundary limit.
	maxBlobBytes = 1 << 20
)

// validateShape walks raw MessagePack and rejects inputs whose declared
// container or blob lengths cannot fit in the bytes that follow. Every
// element occupies at least one encoded byte, so an array of n elements
// (or map of n pairs, or blob of n bytes) claiming more than the remaining
// input is malformed. This runs before msgpack.Unmarshal so a hostile
// length prefix can never drive a large allocation.
func validateShape(data []byte) error {
	end, err := scanValue(data, 0, 0)
	if err != nil {
		return err
	}
	if end != len(data) {
		return fmt.Errorf("trailing bytes after envelope: %d unread", len(data)-end)
	}
	return nil
}

func scanValue(data []byte, pos, depth int) (int, error) {
	if depth > maxScanDepth {
		return 0, fmt.Errorf("nesting depth exceeds %d", maxScanDepth)
	}
	if pos >= len(data) {
		return 0, fmt.Errorf("truncated value at offset %d", pos)
	}
	b := data[pos]
	pos++

	switch {
	case b <= 0x7f || b >= 0xe0: // fixint
		return pos, nil
	case b >= 0x80 && b <= 0x8f: // fixmap
		return scanEntries(data, pos, depth, 2*int(b&0x0f))
	case b >= 0x90 && b <= 0x9f: // fixarray
		return scanEntries(data, pos, depth, int(b&0x0f))
	case b >= 0xa0 && b <= 0xbf: // fixstr
		return scanBlob(data, pos, int(b&0x1f))
	}

	switch b {
	case 0xc0, 0xc2, 0xc3: // nil, false, true
		return pos, nil
	case 0xc1:
		return 0, fmt.Errorf("reserved byte 0xc1 at offset %d", pos-1)
	case 0xca: // float32
		return scanBlob(data, pos, 4)
	case 0xcb: // float64
		return scanBlob(data, pos, 8)
	case 0xcc, 0xd0: // uint8, int8
		return scanBlob(data, pos, 1)
	case 0xcd, 0xd1: // uint16, int16
		return scanBlob(data, pos, 2)
	case 0xce, 0xd2: // uint32, int32
		return scanBlob(data, pos, 4)
	case 0xcf, 0xd3: // uint64, int64
		return scanBlob(data, pos, 8)
	case 0xd4, 0xd5, 0xd6, 0xd7, 0xd8: // fixext1/2/4/8/16
		return scanBlob(data, pos, 1+(1<<(b-0xd4)))
	case 0xc4, 0xd9: // bin8, str8
		n, pos, err := scanLen(data, pos, 1)
		if err != nil {
			return 0, err
		}
		return scanBlob(data, pos, n)
	case 0xc5, 0xda: // bin16, str16
		n, pos, err := scanLen(data, pos, 2)
		if err != nil {
			return 0, err
		}
		return scanBlob(data, pos, n)
	case 0xc6, 0xdb: // bin32, str32
		n, pos, err := scanLen(data, pos, 4)
		if err != nil {
			return 0, err
		}
		return scanBlob(data, pos, n)
	case 0xc7: // ext8
		n, pos, err := scanLen(data, pos, 1)
		if err != nil {
			return 0, err
		}
		return scanBlob(data, pos, n+1)
	case 0xc8: // ext16
		n, pos, err := scanLen(data, pos, 2)
		if err != nil {
			return 0, err
		}
		return scanBlob(data, pos, n+1)
	case 0xc9: // ext32
		n, pos, err := scanLen(data, pos, 4)
		if err != nil {
			return 0, err
		}
		return scanBlob(data, pos, n+1)
	case 0xdc: // array16
		n, pos, err := scanLen(data, pos, 2)
		if err != nil {
			return 0, err
		}
		if err := checkLimit("array", n, maxArrayEntries); err != nil {
			return 0, err
		}
		return scanEntries(data, pos, depth, n)
	case 0xdd: // array32
		n, pos, err := scanLen(data, pos, 4)
		if err != nil {
			return 0, err
		}
		if err := checkLimit("array", n, maxArrayEntries); err != nil {
			return 0, err
		}
		return scanEntries(data, pos, depth, n)
	case 0xde: // map16
		n, pos, err := scanLen(data, pos, 2)
		if err != nil {
			return 0, err
		}
		if err := checkLimit("map", n, maxMapEntries); err != nil {
			return 0, err
		}
		return scanEntries(data, pos, depth, 2*n)
	case 0xdf: // map32
		n, pos, err := scanLen(data, pos, 4)
		if err != nil {
			return 0, err
		}
		if err := checkLimit("map", n, maxMapEntries); err != nil {
			return 0, err
		}
		return scanEntries(data, pos, depth, 2*n)
	}
	return 0, fmt.Errorf("unrecognized type byte 0x%02x at offset %d", b, pos-1)
}

// scanLen reads a big-endian unsigned length of width 1, 2, or 4 bytes and
// rejects values that cannot fit in the remaining input.
func scanLen(data []byte, pos, width int) (int, int, error) {
	if pos+width > len(data) {
		return 0, 0, fmt.Errorf("truncated length at offset %d", pos)
	}
	var n uint64
	switch width {
	case 1:
		n = uint64(data[pos])
	case 2:
		n = uint64(binary.BigEndian.Uint16(data[pos:]))
	case 4:
		n = uint64(binary.BigEndian.Uint32(data[pos:]))
	}
	pos += width
	if n > uint64(len(data)-pos) {
		return 0, 0, fmt.Errorf("declared length %d exceeds %d remaining bytes", n, len(data)-pos)
	}
	return int(n), pos, nil
}

func scanBlob(data []byte, pos, n int) (int, error) {
	if err := checkLimit("blob", n, maxBlobBytes); err != nil {
		return 0, err
	}
	if n > len(data)-pos {
		return 0, fmt.Errorf("truncated payload: need %d bytes, have %d", n, len(data)-pos)
	}
	return pos + n, nil
}

func checkLimit(kind string, got, maximum int) error {
	if got > maximum {
		return fmt.Errorf("%s length %d exceeds limit %d", kind, got, maximum)
	}
	return nil
}

func scanEntries(data []byte, pos, depth, count int) (int, error) {
	if count > len(data)-pos {
		return 0, fmt.Errorf("container claims %d entries with %d bytes remaining", count, len(data)-pos)
	}
	var err error
	for i := 0; i < count; i++ {
		pos, err = scanValue(data, pos, depth+1)
		if err != nil {
			return 0, err
		}
	}
	return pos, nil
}
