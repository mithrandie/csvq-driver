package csvq

import (
	"database/sql/driver"
	"reflect"
	"testing"
	"time"
)

var valueConverterConvertValueTests = []struct {
	Value  interface{}
	Expect driver.Value
	Error  string
}{
	{
		Value:  String{value: "abc"},
		Expect: String{value: "abc"},
	},
	{
		Value:  nil,
		Expect: Null{},
	},
	{
		Value:  "abc",
		Expect: String{value: "abc"},
	},
	{
		Value:  int(123),
		Expect: Integer{value: 123},
	},
	{
		Value:  int8(123),
		Expect: Integer{value: 123},
	},
	{
		Value:  int16(123),
		Expect: Integer{value: 123},
	},
	{
		Value:  int32(123),
		Expect: Integer{value: 123},
	},
	{
		Value:  int64(123),
		Expect: Integer{value: 123},
	},
	{
		Value:  uint(123),
		Expect: Integer{value: 123},
	},
	{
		Value:  uint8(123),
		Expect: Integer{value: 123},
	},
	{
		Value:  uint16(123),
		Expect: Integer{value: 123},
	},
	{
		Value:  uint32(123),
		Expect: Integer{value: 123},
	},
	{
		Value:  uint64(123),
		Expect: Integer{value: 123},
	},
	{
		Value: uint64(10000000000000000000),
		Error: "uint64 values with high bit set are not supported",
	},
	{
		Value:  float32(1234),
		Expect: Float{value: 1234},
	},
	{
		Value:  float64(1234),
		Expect: Float{value: 1234},
	},
	{
		Value:  true,
		Expect: Boolean{value: true},
	},
	{
		Value:  time.Date(2012, 2, 1, 12, 35, 43, 0, time.UTC),
		Expect: Datetime{value: time.Date(2012, 2, 1, 12, 35, 43, 0, time.UTC)},
	},
	{
		Value: []string{"a", "b", "c"},
		Error: "unsupported type: []string",
	},
}

func TestValueConverter_ConvertValue(t *testing.T) {
	c := ValueConverter{}

	for _, v := range valueConverterConvertValueTests {
		result, err := c.ConvertValue(v.Value)
		if err != nil {
			if len(v.Error) < 1 {
				t.Errorf("%v: unexpected error %q", v.Value, err)
			} else if v.Error != "environment-dependent" && err.Error() != v.Error {
				t.Errorf("%v: error %q, want error %q", v.Value, err.Error(), v.Error)
			}
			continue
		}
		if 0 < len(v.Error) {
			t.Errorf("%v: no error, want error %q", v.Value, v.Error)
			continue
		}
		if !reflect.DeepEqual(result, v.Expect) {
			t.Errorf("%v: result = %v, want %v", v.Value, result, v.Expect)
		}
	}
}
