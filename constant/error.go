package constant

import "errors"

var (
	ErrWriteOperationOnReadDB = errors.New("write operation attempted on read-only database")
)
