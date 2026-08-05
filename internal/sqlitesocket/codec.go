// Package sqlitesocket implements the pinned experimental Actor Runtime
// Socket SQL protocol in pure Go. The schema is vendored from
// engine/sdks/rust/actor-runtime-socket-protocol/schemas/v1.bare at Rivet
// commit 957d4e482f404913ca1955d8ecc357533f6fd081.
package sqlitesocket

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"unicode/utf8"

	"github.com/ewhauser/rivet-go/internal/wire"
)

const (
	protocolVersion   = uint16(1)
	defaultMaxFrame   = 32 * 1024 * 1024
	maxValueBytes     = 1 << 20
	maxSupportedFrame = 32 * 1024 * 1024
	maxParameters     = 1_024
	maxColumns        = 1_024
)

type requestKind uint8

const (
	requestExec requestKind = iota
	requestQuery
	requestBegin
	requestCommit
	requestRollback
)

type request struct {
	id        uint32
	kind      requestKind
	sql       string
	args      []wire.SQLiteValue
	leaseKey  *string
	timeoutMS *uint64
}

type response struct {
	id           uint32
	columns      []string
	values       []wire.SQLiteValue
	changes      int64
	lastInsertID *int64
	err          error
}

type encoder struct{ data []byte }

func (e *encoder) uint(value uint64) {
	for value >= 0x80 {
		e.data = append(e.data, byte(value)|0x80)
		value >>= 7
	}
	e.data = append(e.data, byte(value))
}

func (e *encoder) u32(value uint32)  { e.data = binary.LittleEndian.AppendUint32(e.data, value) }
func (e *encoder) u64(value uint64)  { e.data = binary.LittleEndian.AppendUint64(e.data, value) }
func (e *encoder) i64(value int64)   { e.u64(uint64(value)) }
func (e *encoder) f64(value float64) { e.u64(math.Float64bits(value)) }

func (e *encoder) optionalString(value *string) {
	if value == nil {
		e.data = append(e.data, 0)
		return
	}
	e.data = append(e.data, 1)
	e.string(*value)
}

func (e *encoder) string(value string) {
	e.uint(uint64(len(value)))
	e.data = append(e.data, value...)
}

func (e *encoder) sqlValue(value wire.SQLiteValue) error {
	switch value.Kind {
	case "null":
		e.uint(0)
	case "integer":
		if value.Integer == nil {
			return errors.New("SQLite integer argument has no payload")
		}
		e.uint(1)
		e.i64(*value.Integer)
	case "real":
		if value.Bits == nil {
			return errors.New("SQLite real argument has no payload")
		}
		e.uint(2)
		e.u64(*value.Bits)
	case "text":
		if value.Text == nil || len(*value.Text) > maxValueBytes {
			return errors.New("SQLite text argument is missing or too large")
		}
		e.uint(3)
		e.string(*value.Text)
	case "blob":
		if value.Blob == nil || len(*value.Blob) > maxValueBytes {
			return errors.New("SQLite blob argument is missing or too large")
		}
		e.uint(4)
		e.uint(uint64(len(*value.Blob)))
		e.data = append(e.data, (*value.Blob)...)
	default:
		return fmt.Errorf("unknown SQLite argument kind %q", value.Kind)
	}
	return nil
}

func encodeHello() []byte { return []byte{byte(protocolVersion), byte(protocolVersion >> 8)} }

func encodeRequest(value request) ([]byte, error) {
	if value.id == 0 {
		return nil, errors.New("SQLite socket request ID is zero")
	}
	if len(value.args) > maxParameters {
		return nil, fmt.Errorf("SQLite arguments exceed limit %d", maxParameters)
	}
	e := encoder{data: []byte{byte(protocolVersion), byte(protocolVersion >> 8)}}
	e.uint(0) // ClientFrame.Request
	e.u32(value.id)
	if value.kind == requestBegin || value.kind == requestCommit || value.kind == requestRollback {
		e.optionalString(nil)
	} else {
		e.optionalString(value.leaseKey)
	}
	e.uint(uint64(value.kind))
	switch value.kind {
	case requestExec:
		e.string(value.sql)
	case requestQuery:
		e.string(value.sql)
		e.uint(uint64(len(value.args)))
		for _, arg := range value.args {
			if err := e.sqlValue(arg); err != nil {
				return nil, err
			}
		}
	case requestBegin:
		if value.leaseKey == nil {
			return nil, errors.New("SQLite begin has no lease key")
		}
		e.string(*value.leaseKey)
		if value.timeoutMS == nil {
			e.data = append(e.data, 0)
		} else {
			e.data = append(e.data, 1)
			e.u64(*value.timeoutMS)
		}
	case requestCommit, requestRollback:
		if value.leaseKey == nil {
			return nil, errors.New("SQLite transaction finish has no lease key")
		}
		e.string(*value.leaseKey)
	default:
		return nil, errors.New("unknown SQLite socket request kind")
	}
	return e.data, nil
}

type decoder struct {
	data []byte
	pos  int
}

func (d *decoder) remaining() int { return len(d.data) - d.pos }

func (d *decoder) take(count int) ([]byte, error) {
	if count < 0 || count > d.remaining() {
		return nil, fmt.Errorf("BARE value needs %d bytes with %d remaining", count, d.remaining())
	}
	value := d.data[d.pos : d.pos+count]
	d.pos += count
	return value, nil
}

func (d *decoder) uint() (uint64, error) {
	var value uint64
	for shift := uint(0); shift < 70; shift += 7 {
		data, err := d.take(1)
		if err != nil {
			return 0, err
		}
		part := data[0]
		if shift == 63 && part > 1 {
			return 0, errors.New("BARE uint overflows u64")
		}
		value |= uint64(part&0x7f) << shift
		if part < 0x80 {
			return value, nil
		}
	}
	return 0, errors.New("BARE uint is too long")
}

func (d *decoder) u32() (uint32, error) {
	data, err := d.take(4)
	if err != nil {
		return 0, err
	}
	return binary.LittleEndian.Uint32(data), nil
}

func (d *decoder) u64() (uint64, error) {
	data, err := d.take(8)
	if err != nil {
		return 0, err
	}
	return binary.LittleEndian.Uint64(data), nil
}

func (d *decoder) i32() (int32, error) {
	data, err := d.take(4)
	if err != nil {
		return 0, err
	}
	return int32(binary.LittleEndian.Uint32(data)), nil
}

func (d *decoder) i64() (int64, error) {
	value, err := d.u64()
	return int64(value), err
}

func (d *decoder) boolean() (bool, error) {
	data, err := d.take(1)
	if err != nil {
		return false, err
	}
	if data[0] > 1 {
		return false, fmt.Errorf("BARE bool has invalid value %d", data[0])
	}
	return data[0] == 1, nil
}

func (d *decoder) length(maximum int) (int, error) {
	length, err := d.uint()
	if err != nil {
		return 0, err
	}
	if length > uint64(maximum) || length > uint64(d.remaining()) {
		return 0, fmt.Errorf("BARE length %d exceeds limit or %d remaining bytes", length, d.remaining())
	}
	return int(length), nil
}

func (d *decoder) string() (string, error) {
	length, err := d.length(maxValueBytes)
	if err != nil {
		return "", err
	}
	data, err := d.take(length)
	if err != nil {
		return "", err
	}
	if !utf8.Valid(data) {
		return "", errors.New("BARE string is not UTF-8")
	}
	return string(data), nil
}

func (d *decoder) sqlValue() (wire.SQLiteValue, error) {
	tag, err := d.uint()
	if err != nil {
		return wire.SQLiteValue{}, err
	}
	switch tag {
	case 0:
		return wire.SQLiteValue{Kind: "null"}, nil
	case 1:
		value, err := d.i64()
		return wire.SQLiteValue{Kind: "integer", Integer: &value}, err
	case 2:
		bits, err := d.u64()
		return wire.SQLiteValue{Kind: "real", Bits: &bits}, err
	case 3:
		value, err := d.string()
		return wire.SQLiteValue{Kind: "text", Text: &value}, err
	case 4:
		length, err := d.length(maxValueBytes)
		if err != nil {
			return wire.SQLiteValue{}, err
		}
		data, err := d.take(length)
		value := append([]byte(nil), data...)
		if value == nil {
			value = []byte{}
		}
		return wire.SQLiteValue{Kind: "blob", Blob: &value}, err
	default:
		return wire.SQLiteValue{}, fmt.Errorf("unknown BARE SqlValue tag %d", tag)
	}
}

func decodeHello(payload []byte) (uint32, error) {
	d, err := versionedDecoder(payload)
	if err != nil {
		return 0, err
	}
	tag, err := d.uint()
	if err != nil {
		return 0, err
	}
	if tag == 1 {
		return 0, errors.New("Actor Runtime Socket rejected protocol version 1")
	}
	if tag != 0 {
		return 0, fmt.Errorf("unknown ServerHello tag %d", tag)
	}
	maxFrame, err := d.u32()
	if err != nil {
		return 0, err
	}
	if d.remaining() != 0 {
		return 0, errors.New("ServerHello has remaining bytes")
	}
	if maxFrame < 1024 {
		return 0, fmt.Errorf("ServerHello maxFrameBytes %d is too small", maxFrame)
	}
	if maxFrame > maxSupportedFrame {
		return 0, fmt.Errorf("ServerHello maxFrameBytes %d exceeds the Go SDK limit %d", maxFrame, maxSupportedFrame)
	}
	return maxFrame, nil
}

func decodeResponse(payload []byte) (response, error) {
	d, err := versionedDecoder(payload)
	if err != nil {
		return response{}, err
	}
	frameTag, err := d.uint()
	if err != nil {
		return response{}, err
	}
	if frameTag == 1 {
		reason, reasonErr := d.uint()
		if reasonErr != nil {
			return response{}, reasonErr
		}
		return response{}, fmt.Errorf("Actor Runtime Socket sent GoAway reason %d", reason)
	}
	if frameTag != 0 {
		return response{}, fmt.Errorf("unknown ServerFrame tag %d", frameTag)
	}
	id, err := d.u32()
	if err != nil {
		return response{}, err
	}
	result := response{id: id}
	payloadTag, err := d.uint()
	if err != nil {
		return response{}, err
	}
	switch payloadTag {
	case 0, 2, 3, 4:
	case 1:
		if err := decodeQueryResult(d, &result); err != nil {
			return response{}, err
		}
	case 5:
		code, err := d.i32()
		if err != nil {
			return response{}, err
		}
		statement, err := d.u32()
		if err != nil {
			return response{}, err
		}
		message, err := d.string()
		if err != nil {
			return response{}, err
		}
		result.err = &wire.WireError{Code: "sqlite_error", Message: message, SQLiteCode: &code, StatementIndex: &statement}
	case 6:
		result.err = &wire.WireError{Code: "sqlite_endpoint_closed", Message: "Actor Runtime Socket endpoint is closed"}
	case 7:
		limit, err := d.string()
		if err != nil {
			return response{}, err
		}
		capacity, err := d.u32()
		if err != nil {
			return response{}, err
		}
		result.err = &wire.WireError{Code: "sqlite_queue_full", Message: fmt.Sprintf("%s queue is full (capacity %d)", limit, capacity)}
	case 8:
		message, err := d.string()
		if err != nil {
			return response{}, err
		}
		result.err = &wire.WireError{Code: "invalid_lease_key", Message: message}
	case 9:
		timeout, err := d.u64()
		if err != nil {
			return response{}, err
		}
		message, err := d.string()
		if err != nil {
			return response{}, err
		}
		result.err = &wire.WireError{Code: "transaction_expired", Message: fmt.Sprintf("%s (timeout %d ms)", message, timeout)}
	case 10:
		result.err = &wire.WireError{Code: "sqlite_result_too_large", Message: "SQLite result exceeds Actor Runtime Socket maxFrameBytes"}
	default:
		return response{}, fmt.Errorf("unknown ResponsePayload tag %d", payloadTag)
	}
	if d.remaining() != 0 {
		return response{}, errors.New("ServerFrame has remaining bytes")
	}
	return result, nil
}

func decodeQueryResult(d *decoder, result *response) error {
	columnCount, err := d.length(maxColumns)
	if err != nil {
		return err
	}
	result.columns = make([]string, columnCount)
	for index := range result.columns {
		result.columns[index], err = d.string()
		if err != nil {
			return err
		}
	}
	rowCount, err := d.length(d.remaining())
	if err != nil {
		return err
	}
	result.values = make([]wire.SQLiteValue, 0, min(rowCount*max(1, columnCount), 4_096))
	for row := 0; row < rowCount; row++ {
		valueCount, err := d.length(maxColumns)
		if err != nil {
			return err
		}
		if valueCount != columnCount {
			return fmt.Errorf("SQLite row has %d values for %d columns", valueCount, columnCount)
		}
		for range valueCount {
			value, err := d.sqlValue()
			if err != nil {
				return err
			}
			result.values = append(result.values, value)
		}
	}
	result.changes, err = d.i64()
	if err != nil {
		return err
	}
	present, err := d.boolean()
	if err != nil {
		return err
	}
	if present {
		value, err := d.i64()
		if err != nil {
			return err
		}
		result.lastInsertID = &value
	}
	return nil
}

func versionedDecoder(payload []byte) (*decoder, error) {
	if len(payload) < 2 {
		return nil, errors.New("vbare payload has no embedded version")
	}
	if version := binary.LittleEndian.Uint16(payload); version != protocolVersion {
		return nil, fmt.Errorf("unsupported Actor Runtime Socket protocol version %d", version)
	}
	return &decoder{data: payload[2:]}, nil
}
