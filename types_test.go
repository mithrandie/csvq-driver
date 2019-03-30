package csvq

import (
	"database/sql/driver"
	"reflect"
	"testing"
	"time"

	"github.com/mithrandie/csvq/lib/parser"
	"github.com/mithrandie/ternary"
)

var valuePrimitiveTypesTests = []struct {
	Value               Value
	ExpectValue         driver.Value
	ExpectPrimitiveType parser.PrimitiveType
}{
	{
		Value:               String{value: "abc"},
		ExpectValue:         "abc",
		ExpectPrimitiveType: parser.NewStringValue("abc"),
	},
	{
		Value:               Integer{value: 123},
		ExpectValue:         int64(123),
		ExpectPrimitiveType: parser.NewIntegerValue(123),
	},
	{
		Value:               Float{value: 1.234},
		ExpectValue:         float64(1.234),
		ExpectPrimitiveType: parser.NewFloatValue(1.234),
	},
	{
		Value:               Boolean{value: true},
		ExpectValue:         true,
		ExpectPrimitiveType: parser.NewTernaryValue(ternary.TRUE),
	},
	{
		Value:               Datetime{value: time.Date(2012, 2, 1, 12, 35, 43, 0, time.UTC)},
		ExpectValue:         time.Date(2012, 2, 1, 12, 35, 43, 0, time.UTC),
		ExpectPrimitiveType: parser.NewDatetimeValue(time.Date(2012, 2, 1, 12, 35, 43, 0, time.UTC)),
	},
	{
		Value:               Null{},
		ExpectValue:         nil,
		ExpectPrimitiveType: parser.NewNullValue(),
	},
}

func TestValue_PrimitiveType(t *testing.T) {
	for _, v := range valuePrimitiveTypesTests {
		dv, _ := v.Value.Value()
		if !reflect.DeepEqual(dv, v.ExpectValue) {
			t.Errorf("%v: result = %v, want %v", v.Value, dv, v.ExpectValue)
		}
		p := v.Value.PrimitiveType()
		if !reflect.DeepEqual(p, v.ExpectPrimitiveType) {
			t.Errorf("%v: result = %v, want %v", v.Value, p, v.ExpectPrimitiveType)
		}
	}
}
