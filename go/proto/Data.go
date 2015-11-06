// automatically generated, do not modify

package 

import (
	flatbuffers "github.com/google/flatbuffers/go"
)
type Data struct {
	_tab flatbuffers.Table
}

func (rcv *Data) Init(buf []byte, i flatbuffers.UOffsetT) {
	rcv._tab.Bytes = buf
	rcv._tab.Pos = i
}

func (rcv *Data) I32Val() uint32 {
	o := flatbuffers.UOffsetT(rcv._tab.Offset(4))
	if o != 0 {
		return rcv._tab.GetUint32(o + rcv._tab.Pos)
	}
	return 0
}

func (rcv *Data) I64Val() uint64 {
	o := flatbuffers.UOffsetT(rcv._tab.Offset(6))
	if o != 0 {
		return rcv._tab.GetUint64(o + rcv._tab.Pos)
	}
	return 0
}

func (rcv *Data) DoubleVal() float64 {
	o := flatbuffers.UOffsetT(rcv._tab.Offset(8))
	if o != 0 {
		return rcv._tab.GetFloat64(o + rcv._tab.Pos)
	}
	return 0
}

func (rcv *Data) StrVal(j int) byte {
	o := flatbuffers.UOffsetT(rcv._tab.Offset(10))
	if o != 0 {
		a := rcv._tab.Vector(o)
		return rcv._tab.GetByte(a + flatbuffers.UOffsetT(j * 1))
	}
	return 0
}

func (rcv *Data) StrValLength() int {
	o := flatbuffers.UOffsetT(rcv._tab.Offset(10))
	if o != 0 {
		return rcv._tab.VectorLen(o)
	}
	return 0
}

func (rcv *Data) StrValBytes() []byte {
	o := flatbuffers.UOffsetT(rcv._tab.Offset(10))
	if o != 0 {
		return rcv._tab.ByteVector(o + rcv._tab.Pos)
	}
	return nil
}

func DataStart(builder *flatbuffers.Builder) { builder.StartObject(4) }
func DataAddI32Val(builder *flatbuffers.Builder, i32Val uint32) { builder.PrependUint32Slot(0, i32Val, 0) }
func DataAddI64Val(builder *flatbuffers.Builder, i64Val uint64) { builder.PrependUint64Slot(1, i64Val, 0) }
func DataAddDoubleVal(builder *flatbuffers.Builder, doubleVal float64) { builder.PrependFloat64Slot(2, doubleVal, 0) }
func DataAddStrVal(builder *flatbuffers.Builder, strVal flatbuffers.UOffsetT) { builder.PrependUOffsetTSlot(3, flatbuffers.UOffsetT(strVal), 0) }
func DataStartStrValVector(builder *flatbuffers.Builder, numElems int) flatbuffers.UOffsetT { return builder.StartVector(1, numElems, 1)
}
func DataEnd(builder *flatbuffers.Builder) flatbuffers.UOffsetT { return builder.EndObject() }
