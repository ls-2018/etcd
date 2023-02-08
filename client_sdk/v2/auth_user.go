// Copyright 2015 The etcd Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package client

import (
	"context"
	"net/url"
	"path"
)

var defaultV2AuthPrefix = "/v2/auth"

type User struct {
	User     string   `json:"user"`
	Password string   `json:"password,omitempty"`
	Roles    []string `json:"roles"`
	Grant    []string `json:"grant,omitempty"`
	Revoke   []string `json:"revoke,omitempty"`
}

type UserRoles struct {
	User  string `json:"user"`
	Roles []Role `json:"roles"`
}

func v2AuthURL(ep url.URL, action string, name string) *url.URL {
	if name != "" {
		ep.Path = path.Join(ep.Path, defaultV2AuthPrefix, action, name)
		return &ep
	}
	ep.Path = path.Join(ep.Path, defaultV2AuthPrefix, action)
	return &ep
}

type AuthAPI interface {
	// Enable auth.
	Enable(ctx context.Context) error

	// Disable auth.
	Disable(ctx context.Context) error
}

type authError struct {
	Message string `json:"message"`
	Code    int    `json:"-"`
}

func (e authError) Error() string {
	return e.Message
}

// NewAuthUserAPI constructs a new AuthUserAPI that uses HTTP to
// interact with etcd's user creation and modification features.

type AuthUserAPI interface {
	// AddUser adds a user.
	AddUser(ctx context.Context, username string, password string) error

	// RemoveUser removes a user.
	RemoveUser(ctx context.Context, username string) error

	// GetUser retrieves user details.
	GetUser(ctx context.Context, username string) (*User, error)

	// GrantUser grants a user some permission roles.
	GrantUser(ctx context.Context, username string, roles []string) (*User, error)

	// RevokeUser revokes some permission roles from a user.
	RevokeUser(ctx context.Context, username string, roles []string) (*User, error)

	// ChangePassword changes the user's password.
	ChangePassword(ctx context.Context, username string, password string) (*User, error)

	// ListUsers lists the users.
	ListUsers(ctx context.Context) ([]string, error)
}
