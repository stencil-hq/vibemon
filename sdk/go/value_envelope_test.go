package vmon

import (
	"bytes"
	"crypto/sha256"
	"testing"

	pb "github.com/stencil-hq/vibemon/sdk/go/internal/pb"
)

func TestPortableValueEnvelopeRoundTrips(t *testing.T) {
	input := map[string]any{"answer": float64(42), "ok": true}
	for _, codec := range []ValueCodec{ValueJSON, ValueCBOR} {
		for _, compression := range []ValueCompression{CompressionNone, CompressionGZIP, CompressionZSTD} {
			first, err := EncodeValue(input, codec, compression)
			if err != nil {
				t.Fatalf("encode %d/%d: %v", codec, compression, err)
			}
			second, err := EncodeValue(input, codec, compression)
			if err != nil {
				t.Fatalf("encode second: %v", err)
			}
			if !bytes.Equal(first.wire.GetInlineData(), second.wire.GetInlineData()) {
				t.Fatal("encoding is not deterministic")
			}
			var output map[string]any
			if err := first.Decode(&output, nil); err != nil {
				t.Fatalf("decode %d/%d: %v", codec, compression, err)
			}
			if output["ok"] != true {
				t.Fatalf("decoded value = %#v", output)
			}
		}
	}
}

func TestValueEnvelopeRejectsCloudpickle(t *testing.T) {
	value := &ValueEnvelope{wire: &pb.ValueEnvelope{Serializer: pb.ValueSerializer_VALUE_SERIALIZER_CLOUDPICKLE}}
	if err := value.Decode(new(any), nil); err == nil {
		t.Fatal("cloudpickle was accepted")
	}
}

func TestValueEnvelopeArtifactAndChecksum(t *testing.T) {
	value, err := EncodeValue([]string{"a", "b"}, ValueJSON, CompressionGZIP)
	if err != nil {
		t.Fatal(err)
	}
	stored := append([]byte(nil), value.wire.GetInlineData()...)
	artifactDigest := sha256.Sum256(stored)
	value.wire.Storage = &pb.ValueEnvelope_Artifact{Artifact: &pb.ArtifactRef{Digest: &pb.Digest{Algorithm: pb.DigestAlgorithm_DIGEST_ALGORITHM_SHA256, Value: artifactDigest[:]}}}
	var output []string
	if err := value.Decode(&output, func(ref *ArtifactReference) ([]byte, error) { return stored, nil }); err != nil {
		t.Fatal(err)
	}
	if len(output) != 2 || output[1] != "b" {
		t.Fatalf("decoded = %#v", output)
	}
	value.wire.Checksum.Value[0] ^= 0xff
	if err := value.Decode(&output, func(ref *ArtifactReference) ([]byte, error) { return stored, nil }); err == nil {
		t.Fatal("corrupt checksum accepted")
	}
}

func TestJSONSafeIntegerBoundary(t *testing.T) {
	if _, err := EncodeValue(map[string]any{"nested": []any{int64(9007199254740991)}}, ValueJSON, CompressionNone); err != nil {
		t.Fatalf("safe boundary rejected: %v", err)
	}
	if _, err := EncodeValue(map[string]any{"nested": []any{int64(9007199254740992)}}, ValueJSON, CompressionNone); err == nil {
		t.Fatal("unsafe nested JSON integer accepted")
	}
	value, err := EncodeValue(uint64(9007199254740992), ValueCBOR, CompressionNone)
	if err != nil {
		t.Fatalf("wide CBOR integer rejected: %v", err)
	}
	var decoded uint64
	if err := value.Decode(&decoded, nil); err != nil || decoded != 9007199254740992 {
		t.Fatalf("wide CBOR round trip = %d, %v", decoded, err)
	}
}
