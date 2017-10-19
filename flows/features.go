package flows

import (
	"bytes"
	"fmt"
	"reflect"

	"pm.cn.tuwien.ac.at/ipfix/go-ipfix"
)

type constantFeature struct {
	value interface{}
}

func (f *constantFeature) Event(interface{}, EventContext, interface{})    {}
func (f *constantFeature) FinishEvent()                                    {}
func (f *constantFeature) Value() interface{}                              { return f.value }
func (f *constantFeature) SetValue(interface{}, EventContext, interface{}) {}
func (f *constantFeature) Start(EventContext)                              {}
func (f *constantFeature) Stop(FlowEndReason, EventContext)                {}
func (f *constantFeature) Variant() int                                    { return NoVariant }
func (f *constantFeature) Emit(interface{}, EventContext, interface{})     {}
func (f *constantFeature) setDependent([]Feature)                          {}
func (f *constantFeature) SetArguments([]Feature)                          {}
func (f *constantFeature) IsConstant() bool                                { return true }
func (f *constantFeature) setRecord(*record)                               {}

var _ Feature = (*constantFeature)(nil)

func newConstantMetaFeature(value interface{}) featureMaker {
	var t ipfix.Type
	var f interface{}
	switch cf := value.(type) {
	case bool:
		t = ipfix.Boolean
		f = cf
	case float64:
		t = ipfix.Float64
		f = Float64(cf)
	case int64:
		t = ipfix.Signed64
		f = Signed64(cf)
	case int:
		t = ipfix.Signed64
		f = Signed64(cf)
	case uint64:
		t = ipfix.Unsigned64
		f = Unsigned64(cf)
	default:
		panic(fmt.Sprint("Can't create constant of type ", reflect.TypeOf(value)))
	}
	feature := &constantFeature{f}
	return featureMaker{
		ret:  Const,
		make: func() Feature { return feature },
		ie:   ipfix.NewInformationElement(fmt.Sprintf("_const{%v}", value), 0, 0, t, 0),
	}
}

////////////////////////////////////////////////////////////////////////////////

type selectF struct {
	EmptyBaseFeature
	sel bool
}

func (f *selectF) Start(EventContext) { f.sel = false }

func (f *selectF) Event(new interface{}, context EventContext, src interface{}) {
	/* If src is not nil we got an event from the argument -> Store the boolean value (This always happens before events from the flow)
	   otherwise we have an event from the flow -> forward it in case we should and reset sel
	*/
	if src != nil {
		f.sel = new.(bool)
	} else {
		if f.sel {
			for _, v := range f.dependent {
				v.Event(new, context, nil) // is it ok to use nil as source? (we are faking flow source here)
			}
			f.sel = false
		}
	}
}

type selectS struct {
	EmptyBaseFeature
	start, stop, current int
}

func (f *selectS) SetArguments(arguments []Feature) {
	f.start = int(arguments[0].Value().(Number).ToInt())
	f.stop = int(arguments[1].Value().(Number).ToInt())
}
func (f *selectS) Start(EventContext) { f.current = 0 }

func (f *selectS) Event(new interface{}, context EventContext, src interface{}) {
	if f.current >= f.start && f.current < f.stop {
		for _, v := range f.dependent {
			v.Event(new, context, nil) // is it ok to use nil as source? (we are faking flow source here)
		}
	}
	f.current++
}

func init() {
	RegisterFunction("select", Selection, func() Feature { return &selectF{} }, PacketFeature)
	RegisterFunction("select_slice", Selection, func() Feature { return &selectS{} }, Const, Const)
	RegisterFunction("select_slice", Selection, func() Feature { return &selectS{} }, Const, Const, Selection)
}

////////////////////////////////////////////////////////////////////////////////

//apply and map pseudofeatures
func init() {
	RegisterFunction("apply", FlowFeature, nil, FlowFeature, Selection)
	RegisterFunction("map", PacketFeature, nil, PacketFeature, Selection)
}

////////////////////////////////////////////////////////////////////////////////

type count struct {
	BaseFeature
	count int
}

func (f *count) Start(context EventContext) {
	f.count = 0
}

func (f *count) Event(new interface{}, context EventContext, src interface{}) {
	f.count++
}

func (f *count) Stop(reason FlowEndReason, context EventContext) {
	f.SetValue(f.count, context, f)
}

func init() {
	RegisterTemporaryFeature("count", ipfix.Unsigned64, 0, FlowFeature, func() Feature { return &count{} }, Selection)
}

////////////////////////////////////////////////////////////////////////////////

type mean struct {
	BaseFeature
	total Number
	count int
}

func (f *mean) Start(context EventContext) {
	f.total = nil
	f.count = 0
}

func (f *mean) Event(new interface{}, context EventContext, src interface{}) {
	num := new.(Number)
	if f.total == nil {
		f.total = num
	} else {
		f.total = f.total.Add(num)
	}
	f.count++
}

func (f *mean) Stop(reason FlowEndReason, context EventContext) {
	f.SetValue(f.total.ToFloat()/float64(f.count), context, f)
}

func init() {
	RegisterFunction("mean", FlowFeature, func() Feature { return &mean{} }, PacketFeature)
}

////////////////////////////////////////////////////////////////////////////////

type min struct {
	BaseFeature
}

func (f *min) Event(new interface{}, context EventContext, src interface{}) {
	if f.value == nil || new.(Number).Less(f.value.(Number)) {
		f.value = new
	}
}

func (f *min) Stop(reason FlowEndReason, context EventContext) {
	f.SetValue(f.value, context, f)
}

func init() {
	RegisterFunction("min", FlowFeature, func() Feature { return &min{} }, PacketFeature)
	RegisterFunction("minimum", FlowFeature, func() Feature { return &min{} }, PacketFeature)
}

////////////////////////////////////////////////////////////////////////////////

type max struct {
	BaseFeature
}

func (f *max) Event(new interface{}, context EventContext, src interface{}) {
	if f.value == nil || new.(Number).Greater(f.value.(Number)) {
		f.value = new
	}
}

func (f *max) Stop(reason FlowEndReason, context EventContext) {
	f.SetValue(f.value, context, f)
}

func init() {
	RegisterFunction("max", FlowFeature, func() Feature { return &max{} }, PacketFeature)
	RegisterFunction("maximum", FlowFeature, func() Feature { return &max{} }, PacketFeature)
}

////////////////////////////////////////////////////////////////////////////////

type less struct {
	MultiBaseFeature
}

func (f *less) Event(new interface{}, context EventContext, src interface{}) {
	values := f.EventResult(new, src)
	if values == nil {
		return
	}
	a, b := UpConvert(values[0], values[1])
	if a.Less(b) {
		f.SetValue(true, context, f)
	} else {
		f.SetValue(false, context, f)
	}
}

func init() {
	RegisterTemporaryFeature("less", ipfix.Boolean, 0, MatchType, func() Feature { return &less{} }, MatchType, MatchType)
}

////////////////////////////////////////////////////////////////////////////////

type geq struct {
	MultiBaseFeature
}

func (f *geq) Event(new interface{}, context EventContext, src interface{}) {
	values := f.EventResult(new, src)
	if values == nil {
		return
	}
	a, b := UpConvert(values[0], values[1])
	if !a.Less(b) {
		f.SetValue(true, context, f)
	} else {
		f.SetValue(false, context, f)
	}
}

func init() {
	RegisterTemporaryFeature("geq", ipfix.Boolean, 0, MatchType, func() Feature { return &geq{} }, MatchType, MatchType)
}

////////////////////////////////////////////////////////////////////////////////

type accumulate struct {
	MultiBaseFeature
	vector []interface{}
}

func (f *accumulate) Start(context EventContext) {
	f.vector = make([]interface{}, 0)
}

func (f *accumulate) Stop(reason FlowEndReason, context EventContext) {
	if len(f.vector) != 0 {
		f.SetValue(f.vector, context, f)
	}
}

func (f *accumulate) Event(new interface{}, context EventContext, src interface{}) {
	f.vector = append(f.vector, new)
}

//FIXME: this has a bad name
func init() {
	RegisterFunction("accumulate", MatchType, func() Feature { return &accumulate{} }, PacketFeature)
}

////////////////////////////////////////////////////////////////////////////////

type concatenate struct {
	BaseFeature
	buffer *bytes.Buffer
}

func (f *concatenate) Start(context EventContext) {
	f.buffer = new(bytes.Buffer)
}

func (f *concatenate) Event(new interface{}, context EventContext, src interface{}) {
	fmt.Fprint(f.buffer, new)
}

func (f *concatenate) Stop(reason FlowEndReason, context EventContext) {
	f.SetValue(f.buffer.String(), context, f)
}

func init() {
	RegisterTemporaryFeature("concatenate", ipfix.OctetArray, 0, FlowFeature, func() Feature { return &concatenate{} }, PacketFeature)
}

////////////////////////////////////////////////////////////////////////////////

type logF struct {
	BaseFeature
}

func (f *logF) Event(new interface{}, context EventContext, src interface{}) {
	num := new.(Number)
	f.value = num.Log()
}

func (f *logF) Stop(reason FlowEndReason, context EventContext) {
	f.SetValue(f.value, context, f)
}

func init() {
	RegisterFunction("log", MatchType, func() Feature { return &logF{} }, MatchType)
}

////////////////////////////////////////////////////////////////////////////////

type divide struct {
	MultiBaseFeature
}

func (f *divide) Event(new interface{}, context EventContext, src interface{}) {
	values := f.EventResult(new, src)
	if values == nil {
		return
	}
	a, b := UpConvert(values[0], values[1])
	f.value = a.Divide(b)
}

func (f *divide) Stop(reason FlowEndReason, context EventContext) {
	f.SetValue(f.value, context, f)
}

func init() {
	RegisterFunction("divide", MatchType, func() Feature { return &divide{} }, MatchType, MatchType)
}

////////////////////////////////////////////////////////////////////////////////

type multiply struct {
	MultiBaseFeature
}

func (f *multiply) Event(new interface{}, context EventContext, src interface{}) {
	values := f.EventResult(new, src)
	if values == nil {
		return
	}
	a, b := UpConvert(values[0], values[1])
	f.value = a.Multiply(b)
}

func (f *multiply) Stop(reason FlowEndReason, context EventContext) {
	f.SetValue(f.value, context, f)
}

func init() {
	RegisterFunction("multiply", MatchType, func() Feature { return &multiply{} }, MatchType, MatchType)
}

////////////////////////////////////////////////////////////////////////////////

type packetTotalCount struct {
	BaseFeature
	count Unsigned64
}

func (f *packetTotalCount) Start(context EventContext) {
	f.count = 0
}

func (f *packetTotalCount) Event(new interface{}, context EventContext, src interface{}) {
	f.count++
}

func (f *packetTotalCount) Stop(reason FlowEndReason, context EventContext) {
	f.SetValue(f.count, context, f)
}

func init() {
	RegisterStandardFeature("packetTotalCount", FlowFeature, func() Feature { return &packetTotalCount{} }, RawPacket)
}
