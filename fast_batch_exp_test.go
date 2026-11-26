package aerospike

import (
	"testing"
)

type expShape struct {
	name  string
	bin   string
	flags ExpWriteFlags
	build func(p *Expression) *Expression
}

func expTestShapes() []expShape {
	mp := NewMapPolicy(MapOrder.UNORDERED, MapWriteMode.UPDATE)
	halve := func(bin string) func(key *Expression) *Expression {
		return func(key *Expression) *Expression {
			rc := ExpMapGetByKey(MapReturnType.VALUE, ExpTypeINT, key, ExpMapBin("rc"))
			val := ExpMapGetByKey(MapReturnType.VALUE, ExpTypeINT, key, ExpMapBin(bin))
			return ExpCond(
				ExpGreater(rc, ExpIntVal(254)),
				ExpMapPut(mp, key, ExpNumDiv(val, ExpIntVal(2)), ExpMapBin(bin)),
				ExpUnknown(),
			)
		}
	}
	return []expShape{
		{"halve bc", "bc", ExpWriteFlagUpdateOnly | ExpWriteFlagEvalNoFail | ExpWriteFlagPolicyNoFail, halve("bc")},
		{"halve rc", "rc", ExpWriteFlagUpdateOnly | ExpWriteFlagEvalNoFail | ExpWriteFlagPolicyNoFail, halve("rc")},
		{"max bid", "o", ExpWriteFlagDefault, func(fp *Expression) *Expression {
			return ExpCond(ExpBinExists("o"), ExpMax(ExpIntBin("o"), fp), fp)
		}},
		{"scalar la", "la", ExpWriteFlagDefault, func(bits *Expression) *Expression {
			newLA := ExpIntOr(
				ExpIntAnd(ExpIntLShift(ExpIntBin("la"), ExpIntVal(2)), ExpIntVal(0xFF)),
				bits,
			)
			return ExpCond(ExpBinExists("la"), newLA, bits)
		}},
	}
}

func TestExpTemplateGolden(t *testing.T) {
	for _, s := range expTestShapes() {
		tmpl, err := CompileExpTemplate(s.build(ExpParam(0)), 1)
		if err != nil {
			t.Fatalf("%s: %v", s.name, err)
		}
		for _, v := range []int64{0, 1, 3, 254, 255, 1 << 31, 1<<32 - 1, 1 << 40} {
			want := officialOpBytes(t, ExpWriteOp(s.bin, s.build(ExpIntVal(v)), s.flags))
			b, w := newGoldenWrite()
			w.ExpWrite(s.bin, tmpl, s.flags, []int64{v})
			assertFastBytes(t, s.name, want, fastWriteOpBytes(b))
			if b.entries[0].opCount != 1 {
				t.Fatalf("%s: opCount = %d, want 1", s.name, b.entries[0].opCount)
			}
		}
	}
}

func TestExpTemplateTwoParams(t *testing.T) {
	build := func(a, b *Expression) *Expression {
		return ExpCond(ExpBinExists("x"), ExpNumAdd(ExpIntBin("x"), a, b), a)
	}
	tmpl, err := CompileExpTemplate(build(ExpParam(0), ExpParam(1)), 2)
	if err != nil {
		t.Fatal(err)
	}
	want := officialOpBytes(t, ExpWriteOp("x", build(ExpIntVal(-7), ExpIntVal(1<<33)), ExpWriteFlagDefault))
	b, w := newGoldenWrite()
	w.ExpWrite("x", tmpl, ExpWriteFlagDefault, []int64{-7, 1 << 33})
	assertFastBytes(t, "two params", want, fastWriteOpBytes(b))
}

func TestExpTemplateRejectsWrongParamCount(t *testing.T) {
	if _, err := CompileExpTemplate(ExpNumAdd(ExpParam(0), ExpParam(1)), 1); err == nil {
		t.Fatal("param index beyond nParams: want error")
	}
	if _, err := CompileExpTemplate(ExpNumAdd(ExpIntBin("x"), ExpParam(0)), 2); err == nil {
		t.Fatal("unused param: want error")
	}

	tmpl, err := CompileExpTemplate(ExpNumAdd(ExpIntBin("x"), ExpParam(0)), 1)
	if err != nil {
		t.Fatal(err)
	}
	for _, params := range [][]int64{nil, {1, 2}} {
		func() {
			defer func() {
				if recover() == nil {
					t.Fatalf("ExpWrite with %d params: want panic", len(params))
				}
			}()
			_, w := newGoldenWrite()
			w.ExpWrite("x", tmpl, ExpWriteFlagDefault, params)
		}()
	}
}

func TestExpTemplateFailsClosedOnCollision(t *testing.T) {
	e := ExpCond(ExpBinExists("x"), ExpParam(0), ExpIntVal(expSentinel(0)))
	if _, err := CompileExpTemplate(e, 1); err == nil {
		t.Fatal("literal equal to a sentinel: want error")
	}
}
