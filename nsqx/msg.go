package nsqx

import (
	"encoding/json"
	"github.com/google/uuid"
	"reflect"
)

// SetFnName defines the constant string used for method invocation.
var SetFnName string = "Set"

// Encoder struct implements encoding functionality.
type Encoder struct {
}

// Encode converts an object into a JSON byte sequence.
// It first attempts to call the "Set" method on the self object, then uses json.Marshal for serialization.
func (t Encoder) Encode(self any) ([]byte, error) {
	val := reflect.ValueOf(self)
	if val.IsValid() {
		method := val.MethodByName(SetFnName)
		if method.IsValid() {
			method.Call(nil)
		}
	}
	return json.Marshal(self)
}

// Decoder struct implements decoding functionality.
type Decoder struct {
}

// Decode converts a JSON byte sequence into an object.
func (t Decoder) Decode(data []byte, self any) error {
	return json.Unmarshal(data, self)
}

// UniqueId struct defines the logic for generating unique IDs.
type UniqueId struct {
	UniqueId string `json:"_uuid"`
}

// Set method generates a new unique ID for the UniqueId struct.
func (u *UniqueId) Set() {
	u.UniqueId = uuid.NewString()
	return
}

// Msg interface defines encoding and decoding methods.
// Any type that implements the Encode and Decode methods satisfies this interface.
type Msg interface {
	Encode(self any) ([]byte, error)
	Decode(data []byte, self any) error
}
