package vmon

import (
	"bytes"
	"compress/gzip"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"time"

	"github.com/fxamacker/cbor/v2"
	"github.com/klauspost/compress/zstd"
	pb "github.com/stencil-hq/vibemon/sdk/go/internal/pb"
)

// ValueCodec is a portable value serialization format.
type ValueCodec uint8

const (
	// ValueJSON is deterministic RFC 8259 JSON.
	ValueJSON ValueCodec = iota + 1
	// ValueCBOR is deterministic RFC 8949 CBOR.
	ValueCBOR
)

// ValueCompression controls envelope payload compression.
type ValueCompression uint8

const (
	// CompressionNone stores serialized bytes directly.
	CompressionNone ValueCompression = iota
	// CompressionGZIP stores a deterministic gzip stream.
	CompressionGZIP
	// CompressionZSTD stores a deterministic Zstandard frame.
	CompressionZSTD
)

// ArtifactValueLoader retrieves compressed bytes for an artifact-backed envelope.
type ArtifactValueLoader func(*ArtifactReference) ([]byte, error)

// ArtifactReference identifies immutable content by SHA-256 digest.
type ArtifactReference struct{ Digest []byte }

// ValueEnvelope is a portable, checksummed serialized value.
type ValueEnvelope struct{ wire *pb.ValueEnvelope }

// EncodeValue serializes a value and computes its uncompressed SHA-256 checksum.
func EncodeValue(value any, codec ValueCodec, compression ValueCompression) (*ValueEnvelope, error) {
	var raw []byte
	var err error
	var serializer pb.ValueSerializer
	switch codec {
	case ValueJSON:
		raw, err = json.Marshal(value)
		if err == nil {
			err = validateIJSON(raw)
		}
		serializer = pb.ValueSerializer_VALUE_SERIALIZER_JSON
	case ValueCBOR:
		mode, modeErr := cbor.CanonicalEncOptions().EncMode()
		if modeErr != nil {
			return nil, modeErr
		}
		raw, err = mode.Marshal(value)
		serializer = pb.ValueSerializer_VALUE_SERIALIZER_CBOR
	default:
		return nil, fmt.Errorf("vmon: unsupported value codec %d", codec)
	}
	if err != nil {
		return nil, fmt.Errorf("vmon: encode value: %w", err)
	}
	stored := raw
	wireCompression := pb.ValueCompression_VALUE_COMPRESSION_NONE
	switch compression {
	case CompressionNone:
	case CompressionGZIP:
		var buffer bytes.Buffer
		writer, _ := gzip.NewWriterLevel(&buffer, gzip.BestSpeed)
		writer.Header.ModTime = zeroTime
		if _, err = writer.Write(raw); err == nil {
			err = writer.Close()
		}
		if err != nil {
			return nil, fmt.Errorf("vmon: compress value: %w", err)
		}
		stored = buffer.Bytes()
		wireCompression = pb.ValueCompression_VALUE_COMPRESSION_GZIP
	case CompressionZSTD:
		writer, createErr := zstd.NewWriter(nil, zstd.WithEncoderConcurrency(1))
		if createErr != nil {
			return nil, fmt.Errorf("vmon: create zstd encoder: %w", createErr)
		}
		stored = writer.EncodeAll(raw, nil)
		writer.Close()
		wireCompression = pb.ValueCompression_VALUE_COMPRESSION_ZSTD
	default:
		return nil, fmt.Errorf("vmon: unsupported value compression %d", compression)
	}
	digest := sha256.Sum256(raw)
	return &ValueEnvelope{wire: &pb.ValueEnvelope{SchemaVersion: 1, Serializer: serializer, Compression: wireCompression, Checksum: &pb.Digest{Algorithm: pb.DigestAlgorithm_DIGEST_ALGORITHM_SHA256, Value: digest[:]}, UncompressedSizeBytes: uint64(len(raw)), Storage: &pb.ValueEnvelope_InlineData{InlineData: stored}}}, nil
}

var zeroTime = func() (t time.Time) { return }()

// Decode decodes and validates the envelope into destination. Cloudpickle is always rejected.
func (value *ValueEnvelope) Decode(destination any, loader ArtifactValueLoader) error {
	if value == nil || value.wire == nil {
		return errors.New("vmon: nil value envelope")
	}
	wire := value.wire
	if wire.Serializer == pb.ValueSerializer_VALUE_SERIALIZER_CLOUDPICKLE {
		return errors.New("vmon: cloudpickle values are trusted Python-only and unsupported by Go")
	}
	stored := wire.GetInlineData()
	if stored == nil {
		ref := wire.GetArtifact()
		if ref == nil || ref.Digest == nil {
			return errors.New("vmon: value envelope has no storage")
		}
		if loader == nil {
			return errors.New("vmon: artifact-backed value requires a loader")
		}
		var err error
		stored, err = loader(&ArtifactReference{Digest: append([]byte(nil), ref.Digest.Value...)})
		if err != nil {
			return fmt.Errorf("vmon: load value artifact: %w", err)
		}
		artifactDigest := sha256.Sum256(stored)
		if ref.Digest.Algorithm != pb.DigestAlgorithm_DIGEST_ALGORITHM_SHA256 || !bytes.Equal(artifactDigest[:], ref.Digest.Value) {
			return errors.New("vmon: artifact digest mismatch")
		}
	}
	raw := stored
	switch wire.Compression {
	case pb.ValueCompression_VALUE_COMPRESSION_NONE:
	case pb.ValueCompression_VALUE_COMPRESSION_GZIP:
		reader, err := gzip.NewReader(bytes.NewReader(stored))
		if err != nil {
			return fmt.Errorf("vmon: decompress value: %w", err)
		}
		raw, err = io.ReadAll(reader)
		closeErr := reader.Close()
		if err == nil {
			err = closeErr
		}
		if err != nil {
			return fmt.Errorf("vmon: decompress value: %w", err)
		}
	case pb.ValueCompression_VALUE_COMPRESSION_ZSTD:
		reader, err := zstd.NewReader(nil, zstd.WithDecoderConcurrency(1))
		if err != nil {
			return fmt.Errorf("vmon: create zstd decoder: %w", err)
		}
		raw, err = reader.DecodeAll(stored, nil)
		reader.Close()
		if err != nil {
			return fmt.Errorf("vmon: decompress value: %w", err)
		}
	default:
		return errors.New("vmon: unsupported value compression")
	}
	if uint64(len(raw)) != wire.UncompressedSizeBytes {
		return errors.New("vmon: value size mismatch")
	}
	digest := sha256.Sum256(raw)
	if wire.Checksum == nil || wire.Checksum.Algorithm != pb.DigestAlgorithm_DIGEST_ALGORITHM_SHA256 || !bytes.Equal(digest[:], wire.Checksum.Value) {
		return errors.New("vmon: value checksum mismatch")
	}
	switch wire.Serializer {
	case pb.ValueSerializer_VALUE_SERIALIZER_JSON:
		if err := validateIJSON(raw); err != nil {
			return err
		}
		return json.Unmarshal(raw, destination)
	case pb.ValueSerializer_VALUE_SERIALIZER_CBOR:
		return cbor.Unmarshal(raw, destination)
	default:
		return errors.New("vmon: unsupported value serializer")
	}
}

func validateIJSON(raw []byte) error {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	var value any
	if err := decoder.Decode(&value); err != nil {
		return fmt.Errorf("vmon: invalid JSON value: %w", err)
	}
	var visit func(any) error
	visit = func(current any) error {
		switch typed := current.(type) {
		case json.Number:
			number, err := typed.Float64()
			if err != nil || math.IsInf(number, 0) || math.IsNaN(number) {
				return fmt.Errorf("vmon: JSON number %q is outside IEEE-754", typed)
			}
			if math.Trunc(number) == number && math.Abs(number) > 9007199254740991 {
				return fmt.Errorf("vmon: JSON integer %q exceeds the I-JSON safe range; use CBOR", typed)
			}
		case []any:
			for _, item := range typed {
				if err := visit(item); err != nil {
					return err
				}
			}
		case map[string]any:
			for _, item := range typed {
				if err := visit(item); err != nil {
					return err
				}
			}
		}
		return nil
	}
	return visit(value)
}

func envelopeFromWire(wire *pb.ValueEnvelope) *ValueEnvelope { return &ValueEnvelope{wire: wire} }
