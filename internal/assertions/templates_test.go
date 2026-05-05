package assertions

import (
	"strings"
	"testing"

	"github.com/joyautomation/sparkplug-tck-go/internal/runner"
	"github.com/joyautomation/sparkplug-tck-go/internal/spb"
	"github.com/joyautomation/sparkplug-tck-go/internal/spbpb"
)

func boolp(b bool) *bool { return &b }

func metricWithTemplate(t *spbpb.Payload_Template) spb.Message {
	m := nbirth("G", "N", 0, 1, 1, 0, false)
	m.Payload.Metrics = append(m.Payload.Metrics, &spbpb.Payload_Metric{
		Name:     str("MyTemplate"),
		Datatype: u32(uint32(spbpb.DataType_Template)),
		Value:    &spbpb.Payload_Metric_TemplateValue{TemplateValue: t},
	})
	return m
}

func TestTemplate_AllRegistered(t *testing.T) {
	want := []string{
		"tck-id-payloads-template-is-definition",
		"tck-id-payloads-template-is-definition-definition",
		"tck-id-payloads-template-is-definition-instance",
		"tck-id-payloads-template-instance-is-definition",
		"tck-id-payloads-template-ref-definition",
		"tck-id-payloads-template-ref-instance",
		"tck-id-payloads-template-instance-ref",
		"tck-id-payloads-template-parameter-name-required",
		"tck-id-payloads-template-parameter-name-type",
		"tck-id-payloads-template-parameter-type-req",
		"tck-id-payloads-template-parameter-type-value",
		"tck-id-payloads-template-parameter-value",
		"tck-id-payloads-template-parameter-value-type",
	}
	got := map[string]bool{}
	for _, a := range runner.All() {
		got[a.ID] = true
	}
	for _, id := range want {
		if !got[id] {
			t.Errorf("assertion %q missing from registry", id)
		}
	}
}

func TestTemplate_DefinitionHappyPath(t *testing.T) {
	def := &spbpb.Payload_Template{
		IsDefinition: boolp(true),
		Parameters: []*spbpb.Payload_Template_Parameter{
			{
				Name:  str("setpoint"),
				Type:  u32(uint32(spbpb.DataType_Float)),
				Value: &spbpb.Payload_Template_Parameter_FloatValue{FloatValue: 1.5},
			},
		},
	}
	res := runner.RunAll(runner.NewCapture([]spb.Message{metricWithTemplate(def)}))
	for _, id := range []string{
		"tck-id-payloads-template-is-definition",
		"tck-id-payloads-template-is-definition-definition",
		"tck-id-payloads-template-ref-definition",
		"tck-id-payloads-template-parameter-name-required",
		"tck-id-payloads-template-parameter-type-req",
		"tck-id-payloads-template-parameter-type-value",
		"tck-id-payloads-template-parameter-value",
	} {
		if r := resultByID(t, res, id); r.Status != runner.StatusPass {
			t.Errorf("%s: expected pass, got %+v", id, r)
		}
	}
}

func TestTemplate_InstanceHappyPath(t *testing.T) {
	inst := &spbpb.Payload_Template{
		IsDefinition: boolp(false),
		TemplateRef:  str("MyTemplate"),
	}
	res := runner.RunAll(runner.NewCapture([]spb.Message{metricWithTemplate(inst)}))
	for _, id := range []string{
		"tck-id-payloads-template-is-definition",
		"tck-id-payloads-template-is-definition-instance",
		"tck-id-payloads-template-instance-is-definition",
		"tck-id-payloads-template-ref-instance",
		"tck-id-payloads-template-instance-ref",
	} {
		if r := resultByID(t, res, id); r.Status != runner.StatusPass {
			t.Errorf("%s: expected pass, got %+v", id, r)
		}
	}
}

func TestTemplate_IsDefinitionMissing_Fails(t *testing.T) {
	tpl := &spbpb.Payload_Template{} // IsDefinition unset
	res := runner.RunAll(runner.NewCapture([]spb.Message{metricWithTemplate(tpl)}))
	r := resultByID(t, res, "tck-id-payloads-template-is-definition")
	if r.Status != runner.StatusFail {
		t.Errorf("expected fail, got %+v", r)
	}
}

func TestTemplate_DefinitionWithRef_Fails(t *testing.T) {
	tpl := &spbpb.Payload_Template{
		IsDefinition: boolp(true),
		TemplateRef:  str("Other"),
	}
	res := runner.RunAll(runner.NewCapture([]spb.Message{metricWithTemplate(tpl)}))
	r := resultByID(t, res, "tck-id-payloads-template-ref-definition")
	if r.Status != runner.StatusFail || !strings.Contains(r.Detail, "template_ref") {
		t.Errorf("expected ref-definition fail, got %+v", r)
	}
}

func TestTemplate_InstanceMissingRef_Fails(t *testing.T) {
	tpl := &spbpb.Payload_Template{IsDefinition: boolp(false)}
	res := runner.RunAll(runner.NewCapture([]spb.Message{metricWithTemplate(tpl)}))
	r := resultByID(t, res, "tck-id-payloads-template-ref-instance")
	if r.Status != runner.StatusFail {
		t.Errorf("expected ref-instance fail, got %+v", r)
	}
}

func TestTemplateParameter_MissingName_Fails(t *testing.T) {
	def := &spbpb.Payload_Template{
		IsDefinition: boolp(true),
		Parameters: []*spbpb.Payload_Template_Parameter{
			{Type: u32(uint32(spbpb.DataType_Int32))}, // no Name
		},
	}
	res := runner.RunAll(runner.NewCapture([]spb.Message{metricWithTemplate(def)}))
	r := resultByID(t, res, "tck-id-payloads-template-parameter-name-required")
	if r.Status != runner.StatusFail {
		t.Errorf("expected fail, got %+v", r)
	}
}

func TestTemplateParameter_MissingTypeInBirth_Fails(t *testing.T) {
	def := &spbpb.Payload_Template{
		IsDefinition: boolp(true),
		Parameters: []*spbpb.Payload_Template_Parameter{
			{Name: str("p")}, // no Type
		},
	}
	res := runner.RunAll(runner.NewCapture([]spb.Message{metricWithTemplate(def)}))
	r := resultByID(t, res, "tck-id-payloads-template-parameter-type-req")
	if r.Status != runner.StatusFail {
		t.Errorf("expected fail, got %+v", r)
	}
}

func TestTemplateParameter_InvalidType_Fails(t *testing.T) {
	def := &spbpb.Payload_Template{
		IsDefinition: boolp(true),
		Parameters: []*spbpb.Payload_Template_Parameter{
			{Name: str("p"), Type: u32(99)},
		},
	}
	res := runner.RunAll(runner.NewCapture([]spb.Message{metricWithTemplate(def)}))
	r := resultByID(t, res, "tck-id-payloads-template-parameter-type-value")
	if r.Status != runner.StatusFail {
		t.Errorf("expected fail, got %+v", r)
	}
}

func TestTemplate_NoMessages_NA(t *testing.T) {
	res := runner.RunAll(runner.NewCapture([]spb.Message{nbirth("G", "N", 0, 1, 1, 0, false)}))
	r := resultByID(t, res, "tck-id-payloads-template-is-definition")
	if r.Status != runner.StatusNotApplicable {
		t.Errorf("expected NA, got %+v", r)
	}
}
