package api

import (
	"github.com/aws/aws-lambda-go/events"
)

type UserId string

func AuthorizedUser(event *events.APIGatewayV2HTTPRequest) (*UserId, error) {
	context := event.RequestContext
	auth := context.Authorizer
	if auth == nil {
		return nil, UserIdMissing
	}
	jwt := auth.JWT
	if jwt == nil {
		return nil, UserIdMissing
	}
	sub, found := jwt.Claims["sub"]
	if !found {
		return nil, UserIdMissing
	}
	userId := UserId(sub)
	return &userId, nil
}

func WithUserProvided(userId UserId, event *events.APIGatewayV2HTTPRequest) *events.APIGatewayV2HTTPRequest {
	auth := &events.APIGatewayV2HTTPRequestContextAuthorizerDescription{
		JWT: &events.APIGatewayV2HTTPRequestContextAuthorizerJWTDescription{
			Claims: map[string]string{
				"sub": string(userId),
			},
		},
	}
	event.RequestContext.Authorizer = auth
	return event
}
