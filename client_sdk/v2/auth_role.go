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
)

type Role struct {
	Role        string       `json:"role"`
	Permissions Permissions  `json:"permissions"`
	Grant       *Permissions `json:"grant,omitempty"`
	Revoke      *Permissions `json:"revoke,omitempty"`
}

type Permissions struct {
	KV rwPermission `json:"kv"`
}

type rwPermission struct {
	Read  []string `json:"read"`
	Write []string `json:"write"`
}

type PermissionType int

const (
	ReadPermission PermissionType = iota
	WritePermission
	ReadWritePermission
)

// NewAuthRoleAPI constructs a new AuthRoleAPI that uses HTTP to
// interact with etcd's role creation and modification features.

type AuthRoleAPI interface {
	// AddRole adds a role.
	AddRole(ctx context.Context, role string) error

	// RemoveRole removes a role.
	RemoveRole(ctx context.Context, role string) error

	// GetRole retrieves role details.
	GetRole(ctx context.Context, role string) (*Role, error)

	// GrantRoleKV grants a role some permission prefixes for the KV store.
	GrantRoleKV(ctx context.Context, role string, prefixes []string, permType PermissionType) (*Role, error)

	// RevokeRoleKV revokes some permission prefixes for a role on the KV store.
	RevokeRoleKV(ctx context.Context, role string, prefixes []string, permType PermissionType) (*Role, error)

	// ListRoles lists roles.
	ListRoles(ctx context.Context) ([]string, error)
}
