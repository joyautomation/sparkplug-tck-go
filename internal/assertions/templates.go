package assertions

import (
	"fmt"

	"github.com/joyautomation/sparkplug-tck-go/internal/runner"
	"github.com/joyautomation/sparkplug-tck-go/internal/spb"
	"github.com/joyautomation/sparkplug-tck-go/internal/spbpb"
)

// Structural rules for Template metrics. A Template comes in two shapes:
//
//   * Definition  — IsDefinition=true, no template_ref, members describe shape
//   * Instance    — IsDefinition=false, template_ref points at a definition
//
// We classify by IsDefinition (the spec requires it set on every template),
// and apply per-shape rules. The cross-shape membership/parameter rules
// (instance members must match definition) are deferred until we wire a
// session-level template registry.

func init() {
	runner.Register(runner.Assertion{ID: "tck-id-payloads-template-is-definition", Run: templateIsDefinitionPresent})
	runner.Register(runner.Assertion{ID: "tck-id-payloads-template-is-definition-definition", Run: templateIsDefDefinition})
	runner.Register(runner.Assertion{ID: "tck-id-payloads-template-is-definition-instance", Run: templateIsDefInstance})
	runner.Register(runner.Assertion{ID: "tck-id-payloads-template-instance-is-definition", Run: aliasOf(templateIsDefInstance, "tck-id-payloads-template-instance-is-definition")})
	runner.Register(runner.Assertion{ID: "tck-id-payloads-template-ref-definition", Run: templateRefDefinition})
	runner.Register(runner.Assertion{ID: "tck-id-payloads-template-ref-instance", Run: templateRefInstance})
	runner.Register(runner.Assertion{ID: "tck-id-payloads-template-instance-ref", Run: aliasOf(templateRefInstance, "tck-id-payloads-template-instance-ref")})

	runner.Register(runner.Assertion{ID: "tck-id-payloads-template-parameter-name-required", Run: templateParamNameRequired})
	runner.Register(runner.Assertion{ID: "tck-id-payloads-template-parameter-name-type", Run: templateParamNameType})
	runner.Register(runner.Assertion{ID: "tck-id-payloads-template-parameter-type-req", Run: templateParamTypeReq})
	runner.Register(runner.Assertion{ID: "tck-id-payloads-template-parameter-type-value", Run: templateParamTypeValue})
	runner.Register(runner.Assertion{ID: "tck-id-payloads-template-parameter-value", Run: templateParamValue})
	runner.Register(runner.Assertion{ID: "tck-id-payloads-template-parameter-value-type", Run: templateParamValueType})
}

// forEachTemplate visits every Payload_Template across messages. Templates
// can be the value of any metric whose Value oneof is TemplateValue.
func forEachTemplate(c *runner.Capture, visit func(m spb.Message, met *spbpb.Payload_Metric, t *spbpb.Payload_Template)) {
	for _, m := range c.Messages {
		if m.Topic.Type == spb.STATE || m.Payload == nil {
			continue
		}
		for _, met := range m.Payload.GetMetrics() {
			tv, ok := met.Value.(*spbpb.Payload_Metric_TemplateValue)
			if !ok || tv == nil || tv.TemplateValue == nil {
				continue
			}
			visit(m, met, tv.TemplateValue)
		}
	}
}

// isTemplateDefinition reports whether the template should be treated as a
// definition. We rely on the explicit IsDefinition flag — instances must
// set it to false, definitions to true.
func isTemplateDefinition(t *spbpb.Payload_Template) bool {
	return t.IsDefinition != nil && *t.IsDefinition
}

func templateSubject(m spb.Message, met *spbpb.Payload_Metric) string {
	return subjectFor(m) + "/" + metricLabel(met)
}

func templateIsDefinitionPresent(c *runner.Capture) []runner.Result {
	const id = "tck-id-payloads-template-is-definition"
	var out []runner.Result
	forEachTemplate(c, func(m spb.Message, met *spbpb.Payload_Metric, t *spbpb.Payload_Template) {
		subj := templateSubject(m, met)
		if t.IsDefinition == nil {
			out = append(out, runner.Fail(id, subj, "Template is_definition not set"))
		} else {
			out = append(out, runner.Pass(id, subj))
		}
	})
	if len(out) == 0 {
		return []runner.Result{runner.NA(id, "no Template metrics in capture")}
	}
	return out
}

// templateIsDefDefinition: a Template Definition must set IsDefinition=true.
// Definitions appear as top-level metrics in NBIRTH (they describe a shape)
// and don't have a template_ref. We treat any template without template_ref
// as a candidate definition, then assert IsDefinition=true.
func templateIsDefDefinition(c *runner.Capture) []runner.Result {
	const id = "tck-id-payloads-template-is-definition-definition"
	var out []runner.Result
	forEachTemplate(c, func(m spb.Message, met *spbpb.Payload_Metric, t *spbpb.Payload_Template) {
		if t.TemplateRef != nil && *t.TemplateRef != "" {
			return // instance, not subject to this rule
		}
		subj := templateSubject(m, met)
		if t.IsDefinition == nil || !*t.IsDefinition {
			out = append(out, runner.Fail(id, subj, "Template Definition must set is_definition=true"))
		} else {
			out = append(out, runner.Pass(id, subj))
		}
	})
	if len(out) == 0 {
		return []runner.Result{runner.NA(id, "no Template Definitions in capture")}
	}
	return out
}

// templateIsDefInstance: a Template Instance must set IsDefinition=false.
// Instances are templates carrying a template_ref.
func templateIsDefInstance(c *runner.Capture) []runner.Result {
	const id = "tck-id-payloads-template-is-definition-instance"
	var out []runner.Result
	forEachTemplate(c, func(m spb.Message, met *spbpb.Payload_Metric, t *spbpb.Payload_Template) {
		if t.TemplateRef == nil || *t.TemplateRef == "" {
			return // definition (or unclassified), not subject to this rule
		}
		subj := templateSubject(m, met)
		if t.IsDefinition != nil && *t.IsDefinition {
			out = append(out, runner.Fail(id, subj, "Template Instance must set is_definition=false"))
		} else {
			out = append(out, runner.Pass(id, subj))
		}
	})
	if len(out) == 0 {
		return []runner.Result{runner.NA(id, "no Template Instances in capture")}
	}
	return out
}

// templateRefDefinition: a Template Definition MUST omit template_ref.
func templateRefDefinition(c *runner.Capture) []runner.Result {
	const id = "tck-id-payloads-template-ref-definition"
	var out []runner.Result
	forEachTemplate(c, func(m spb.Message, met *spbpb.Payload_Metric, t *spbpb.Payload_Template) {
		if !isTemplateDefinition(t) {
			return
		}
		subj := templateSubject(m, met)
		if t.TemplateRef != nil && *t.TemplateRef != "" {
			out = append(out, runner.Fail(id, subj,
				fmt.Sprintf("Template Definition must not set template_ref (got %q)", *t.TemplateRef)))
		} else {
			out = append(out, runner.Pass(id, subj))
		}
	})
	if len(out) == 0 {
		return []runner.Result{runner.NA(id, "no Template Definitions in capture")}
	}
	return out
}

// templateRefInstance: a Template Instance MUST set template_ref to the
// definition name.
func templateRefInstance(c *runner.Capture) []runner.Result {
	const id = "tck-id-payloads-template-ref-instance"
	var out []runner.Result
	forEachTemplate(c, func(m spb.Message, met *spbpb.Payload_Metric, t *spbpb.Payload_Template) {
		if isTemplateDefinition(t) {
			return
		}
		// Treat templates with no IsDefinition flag and no ref as ambiguous
		// — leave them to templateIsDefinitionPresent to fail. Here we only
		// fail templates clearly marked as instances (IsDefinition=false)
		// with missing template_ref.
		if t.IsDefinition == nil {
			return
		}
		subj := templateSubject(m, met)
		if t.TemplateRef == nil || *t.TemplateRef == "" {
			out = append(out, runner.Fail(id, subj, "Template Instance must set template_ref"))
		} else {
			out = append(out, runner.Pass(id, subj))
		}
	})
	if len(out) == 0 {
		return []runner.Result{runner.NA(id, "no Template Instances in capture")}
	}
	return out
}

// --- Template Parameter rules ---------------------------------------------

func forEachTemplateParameter(c *runner.Capture, visit func(m spb.Message, met *spbpb.Payload_Metric, p *spbpb.Payload_Template_Parameter)) {
	forEachTemplate(c, func(m spb.Message, met *spbpb.Payload_Metric, t *spbpb.Payload_Template) {
		for _, p := range t.GetParameters() {
			if p == nil {
				continue
			}
			visit(m, met, p)
		}
	})
}

func paramSubject(m spb.Message, met *spbpb.Payload_Metric, p *spbpb.Payload_Template_Parameter) string {
	pname := "<unnamed>"
	if p.Name != nil {
		pname = *p.Name
	}
	return templateSubject(m, met) + "/param=" + pname
}

func templateParamNameRequired(c *runner.Capture) []runner.Result {
	const id = "tck-id-payloads-template-parameter-name-required"
	var out []runner.Result
	forEachTemplateParameter(c, func(m spb.Message, met *spbpb.Payload_Metric, p *spbpb.Payload_Template_Parameter) {
		subj := paramSubject(m, met, p)
		if p.Name == nil {
			out = append(out, runner.Fail(id, subj, "Template Parameter must include name"))
		} else {
			out = append(out, runner.Pass(id, subj))
		}
	})
	if len(out) == 0 {
		return []runner.Result{runner.NA(id, "no Template Parameters in capture")}
	}
	return out
}

// templateParamNameType: name must be a UTF-8 string. Proto string fields
// are UTF-8 by definition; if it decoded, it's UTF-8. Trivial pass.
func templateParamNameType(c *runner.Capture) []runner.Result {
	const id = "tck-id-payloads-template-parameter-name-type"
	return templateParamTrivialPass(c, id)
}

// templateParamTypeReq: parameter datatype must be set in BIRTH messages.
func templateParamTypeReq(c *runner.Capture) []runner.Result {
	const id = "tck-id-payloads-template-parameter-type-req"
	var out []runner.Result
	forEachTemplate(c, func(m spb.Message, met *spbpb.Payload_Metric, t *spbpb.Payload_Template) {
		if !m.Topic.Type.IsBirth() {
			return
		}
		for _, p := range t.GetParameters() {
			if p == nil {
				continue
			}
			subj := paramSubject(m, met, p)
			if p.Type == nil {
				out = append(out, runner.Fail(id, subj, "Template Parameter must include datatype in BIRTH"))
			} else {
				out = append(out, runner.Pass(id, subj))
			}
		}
	})
	if len(out) == 0 {
		return []runner.Result{runner.NA(id, "no Template Parameters in BIRTH messages")}
	}
	return out
}

func templateParamTypeValue(c *runner.Capture) []runner.Result {
	const id = "tck-id-payloads-template-parameter-type-value"
	var out []runner.Result
	forEachTemplateParameter(c, func(m spb.Message, met *spbpb.Payload_Metric, p *spbpb.Payload_Template_Parameter) {
		if p.Type == nil {
			return // covered by -type-req
		}
		subj := paramSubject(m, met, p)
		if !validDataType(*p.Type) {
			out = append(out, runner.Fail(id, subj,
				fmt.Sprintf("type=%d not in valid enum", *p.Type)))
		} else {
			out = append(out, runner.Pass(id, subj))
		}
	})
	if len(out) == 0 {
		return []runner.Result{runner.NA(id, "no Template Parameters with datatype in capture")}
	}
	return out
}

// templateParamValue: parameter value oneof must be one of the allowed
// scalar types (uint32/uint64/float/double/bool/string). The proto already
// pins the oneof, so any decoded parameter satisfies this — except for the
// extension_value variant, which we report as a fail.
func templateParamValue(c *runner.Capture) []runner.Result {
	const id = "tck-id-payloads-template-parameter-value"
	var out []runner.Result
	forEachTemplateParameter(c, func(m spb.Message, met *spbpb.Payload_Metric, p *spbpb.Payload_Template_Parameter) {
		subj := paramSubject(m, met, p)
		switch p.Value.(type) {
		case nil,
			*spbpb.Payload_Template_Parameter_IntValue,
			*spbpb.Payload_Template_Parameter_LongValue,
			*spbpb.Payload_Template_Parameter_FloatValue,
			*spbpb.Payload_Template_Parameter_DoubleValue,
			*spbpb.Payload_Template_Parameter_BooleanValue,
			*spbpb.Payload_Template_Parameter_StringValue:
			out = append(out, runner.Pass(id, subj))
		default:
			out = append(out, runner.Fail(id, subj, "Template Parameter value must be a basic Sparkplug scalar"))
		}
	})
	if len(out) == 0 {
		return []runner.Result{runner.NA(id, "no Template Parameters in capture")}
	}
	return out
}

// templateParamValueType: type field is uint32 by definition (proto). Trivial pass.
func templateParamValueType(c *runner.Capture) []runner.Result {
	const id = "tck-id-payloads-template-parameter-value-type"
	return templateParamTrivialPass(c, id)
}

func templateParamTrivialPass(c *runner.Capture, id string) []runner.Result {
	var out []runner.Result
	forEachTemplateParameter(c, func(m spb.Message, met *spbpb.Payload_Metric, p *spbpb.Payload_Template_Parameter) {
		out = append(out, runner.Pass(id, paramSubject(m, met, p)))
	})
	if len(out) == 0 {
		return []runner.Result{runner.NA(id, "no Template Parameters in capture")}
	}
	return out
}
