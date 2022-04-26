package ddbmodel

type DdbModelEmptyError struct{}

func (e *DdbModelEmptyError) Error() string {
	return "Empty DdbModel"
}

