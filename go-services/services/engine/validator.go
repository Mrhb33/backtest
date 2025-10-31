package engine

// IR Validator (scaffold)

type ValidationError struct{ Msg string }

func (e ValidationError) Error() string { return e.Msg }

type Validator struct{}

func (v *Validator) Validate(p Program) error {
	// No look-ahead & warmup checks would be implemented here
	// For now, just ensure program nodes are bounded
	if len(p.Nodes) > 100000 { // arbitrary guard
		return ValidationError{Msg: "program too large"}
	}
	return nil
}
