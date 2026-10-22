package interceptors

import (
	"context"
	"strconv"
	"strings"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// PlayerIDHeader — metadata-ключ с id игрока. Упрощённая замена JWT.
const PlayerIDHeader = "x-player-id"

type playerIDKey struct{}

// PlayerIDFromContext возвращает id игрока, который положил auth interceptor.
func PlayerIDFromContext(ctx context.Context) (int64, bool) {
	id, ok := ctx.Value(playerIDKey{}).(int64)
	return id, ok
}

// Auth проверяет x-player-id. Всё закрыто по умолчанию: без авторизации работают только
// сервисы из списка публичных, для остальных заголовок обязателен.
type Auth struct {
	public map[string]struct{}
}

// NewAuth принимает полные имена публичных сервисов (например, "grpc.health.v1.Health").
func NewAuth(publicServices ...string) *Auth {
	public := make(map[string]struct{}, len(publicServices))
	for _, s := range publicServices {
		public[s] = struct{}{}
	}
	return &Auth{public: public}
}

func (a *Auth) Unary() grpc.UnaryServerInterceptor {
	return func(
		ctx context.Context,
		req any,
		info *grpc.UnaryServerInfo,
		handler grpc.UnaryHandler,
	) (any, error) {
		ctx, err := a.authenticate(ctx, info.FullMethod)
		if err != nil {
			return nil, err
		}
		return handler(ctx, req)
	}
}

func (a *Auth) Stream() grpc.StreamServerInterceptor {
	return func(
		srv any,
		ss grpc.ServerStream,
		info *grpc.StreamServerInfo,
		handler grpc.StreamHandler,
	) error {
		ctx, err := a.authenticate(ss.Context(), info.FullMethod)
		if err != nil {
			return err
		}
		return handler(srv, &ctxStream{ServerStream: ss, ctx: ctx})
	}
}

// authenticate кладёт id игрока в контекст. Публичные сервисы проходят без проверки
// и не читают заголовок.
func (a *Auth) authenticate(ctx context.Context, fullMethod string) (context.Context, error) {
	if _, ok := a.public[serviceName(fullMethod)]; ok {
		return ctx, nil
	}

	var raw string
	if md, ok := metadata.FromIncomingContext(ctx); ok {
		if vals := md.Get(PlayerIDHeader); len(vals) > 0 {
			raw = vals[0]
		}
	}
	if raw == "" {
		return ctx, status.Error(codes.Unauthenticated, PlayerIDHeader+" metadata is required")
	}

	id, err := strconv.ParseInt(raw, 10, 64)
	if err != nil || id <= 0 {
		return ctx, status.Error(codes.Unauthenticated, PlayerIDHeader+" must be a positive integer")
	}
	return context.WithValue(ctx, playerIDKey{}, id), nil
}

// serviceName вынимает "pkg.Service" из "/pkg.Service/Method".
func serviceName(fullMethod string) string {
	name, _, _ := strings.Cut(strings.TrimPrefix(fullMethod, "/"), "/")
	return name
}

type ctxStream struct {
	grpc.ServerStream
	ctx context.Context //nolint:containedctx // обёртка стрима обязана переопределять Context()
}

func (s *ctxStream) Context() context.Context { return s.ctx }
