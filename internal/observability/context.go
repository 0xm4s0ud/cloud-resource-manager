package observability

import "context"

type ctxKey int

const requestIDKey ctxKey = 1

func withRequestID(ctx context.Context, id string) context.Context {
	return context.WithValue(ctx, requestIDKey, id)
}

func RequestIDFromContext(ctx context.Context) string {
	v, _ := ctx.Value(requestIDKey).(string)
	return v
}
