package domain

import (
	"errors"
	"fmt"
)

type AppError struct {
	Reason  Reason
	Message string
	Details any
	Err     error
}

func (e *AppError) Error() string {
	if e.Err != nil {
		return fmt.Sprintf("%s: %v", e.Message, e.Err)
	}
	return e.Message
}

func (e *AppError) Unwrap() error { return e.Err }

func NewAppError(reason Reason, message string, details any) *AppError {
	return &AppError{Reason: reason, Message: message, Details: details}
}

func AsAppError(err error) (*AppError, bool) {
	var ae *AppError
	if errors.As(err, &ae) {
		return ae, true
	}
	return nil, false
}
