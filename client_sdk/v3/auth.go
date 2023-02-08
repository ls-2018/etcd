// Copyright 2016 The etcd Authors
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

package clientv3

import (
	"context"
	"fmt"
	"strings"

	"github.com/ls-2018/etcd_cn/offical/api/v3/authpb"
	pb "github.com/ls-2018/etcd_cn/offical/etcdserverpb"
	"google.golang.org/grpc"
)

type (
	AuthEnableResponse               pb.AuthEnableResponse
	AuthDisableResponse              pb.AuthDisableResponse
	AuthStatusResponse               pb.AuthStatusResponse
	AuthenticateResponse             pb.AuthenticateResponse
	AuthUserAddResponse              pb.AuthUserAddResponse
	AuthUserDeleteResponse           pb.AuthUserDeleteResponse
	AuthUserChangePasswordResponse   pb.AuthUserChangePasswordResponse
	AuthUserGrantRoleResponse        pb.AuthUserGrantRoleResponse
	AuthUserGetResponse              pb.AuthUserGetResponse
	AuthUserRevokeRoleResponse       pb.AuthUserRevokeRoleResponse
	AuthRoleAddResponse              pb.AuthRoleAddResponse
	AuthRoleGrantPermissionResponse  pb.AuthRoleGrantPermissionResponse
	AuthRoleGetResponse              pb.AuthRoleGetResponse
	AuthRoleRevokePermissionResponse pb.AuthRoleRevokePermissionResponse
	AuthRoleDeleteResponse           pb.AuthRoleDeleteResponse
	AuthUserListResponse             pb.AuthUserListResponse
	AuthRoleListResponse             pb.AuthRoleListResponse

	PermissionType authpb.Permission_Type
	Permission     authpb.Permission
)

const (
	PermRead      = authpb.READ
	PermWrite     = authpb.WRITE
	PermReadWrite = authpb.READWRITE
)

type UserAddOptions authpb.UserAddOptions

type Auth interface {
	Authenticate(ctx context.Context, name string, password string) (*AuthenticateResponse, error)
	AuthEnable(ctx context.Context) (*AuthEnableResponse, error)
	AuthDisable(ctx context.Context) (*AuthDisableResponse, error)
	AuthStatus(ctx context.Context) (*AuthStatusResponse, error)
	UserAdd(ctx context.Context, name string, password string) (*AuthUserAddResponse, error)
	UserAddWithOptions(ctx context.Context, name string, password string, opt *UserAddOptions) (*AuthUserAddResponse, error)
	UserDelete(ctx context.Context, name string) (*AuthUserDeleteResponse, error)
	UserChangePassword(ctx context.Context, name string, password string) (*AuthUserChangePasswordResponse, error)
	UserGrantRole(ctx context.Context, user string, role string) (*AuthUserGrantRoleResponse, error)
	UserGet(ctx context.Context, name string) (*AuthUserGetResponse, error)
	UserList(ctx context.Context) (*AuthUserListResponse, error)
	UserRevokeRole(ctx context.Context, name string, role string) (*AuthUserRevokeRoleResponse, error)
	RoleAdd(ctx context.Context, name string) (*AuthRoleAddResponse, error)
	RoleGrantPermission(ctx context.Context, name string, key, rangeEnd string, permType PermissionType) (*AuthRoleGrantPermissionResponse, error)
	RoleGet(ctx context.Context, role string) (*AuthRoleGetResponse, error)
	RoleList(ctx context.Context) (*AuthRoleListResponse, error)
	RoleRevokePermission(ctx context.Context, role string, key, rangeEnd string) (*AuthRoleRevokePermissionResponse, error)
	RoleDelete(ctx context.Context, role string) (*AuthRoleDeleteResponse, error)
}

type authClient struct {
	remote   pb.AuthClient
	callOpts []grpc.CallOption
}

func NewAuth(c *Client) Auth {
	api := &authClient{remote: RetryAuthClient(c)}
	if c != nil {
		api.callOpts = c.callOpts
	}
	return api
}

func NewAuthFromAuthClient(remote pb.AuthClient, c *Client) Auth {
	api := &authClient{remote: remote}
	if c != nil {
		api.callOpts = c.callOpts
	}
	return api
}

func (auth *authClient) Authenticate(ctx context.Context, name string, password string) (*AuthenticateResponse, error) {
	resp, err := auth.remote.Authenticate(ctx, &pb.AuthenticateRequest{Name: name, Password: password}, auth.callOpts...)
	return (*AuthenticateResponse)(resp), toErr(ctx, err)
}

func (auth *authClient) AuthEnable(ctx context.Context) (*AuthEnableResponse, error) {
	resp, err := auth.remote.AuthEnable(ctx, &pb.AuthEnableRequest{}, auth.callOpts...)
	return (*AuthEnableResponse)(resp), toErr(ctx, err)
}

func (auth *authClient) AuthDisable(ctx context.Context) (*AuthDisableResponse, error) {
	resp, err := auth.remote.AuthDisable(ctx, &pb.AuthDisableRequest{}, auth.callOpts...)
	return (*AuthDisableResponse)(resp), toErr(ctx, err)
}

func (auth *authClient) AuthStatus(ctx context.Context) (*AuthStatusResponse, error) {
	resp, err := auth.remote.AuthStatus(ctx, &pb.AuthStatusRequest{}, auth.callOpts...)
	return (*AuthStatusResponse)(resp), toErr(ctx, err)
}

func (auth *authClient) UserAdd(ctx context.Context, name string, password string) (*AuthUserAddResponse, error) {
	resp, err := auth.remote.UserAdd(ctx, &pb.AuthUserAddRequest{Name: name, Password: password, Options: &authpb.UserAddOptions{NoPassword: false}}, auth.callOpts...)
	return (*AuthUserAddResponse)(resp), toErr(ctx, err)
}

func (auth *authClient) UserAddWithOptions(ctx context.Context, name string, password string, options *UserAddOptions) (*AuthUserAddResponse, error) {
	resp, err := auth.remote.UserAdd(ctx, &pb.AuthUserAddRequest{Name: name, Password: password, Options: (*authpb.UserAddOptions)(options)}, auth.callOpts...)
	return (*AuthUserAddResponse)(resp), toErr(ctx, err)
}

func (auth *authClient) UserDelete(ctx context.Context, name string) (*AuthUserDeleteResponse, error) {
	resp, err := auth.remote.UserDelete(ctx, &pb.AuthUserDeleteRequest{Name: name}, auth.callOpts...)
	return (*AuthUserDeleteResponse)(resp), toErr(ctx, err)
}

func (auth *authClient) UserChangePassword(ctx context.Context, name string, password string) (*AuthUserChangePasswordResponse, error) {
	resp, err := auth.remote.UserChangePassword(ctx, &pb.AuthUserChangePasswordRequest{Name: name, Password: password}, auth.callOpts...)
	return (*AuthUserChangePasswordResponse)(resp), toErr(ctx, err)
}

func (auth *authClient) UserGrantRole(ctx context.Context, user string, role string) (*AuthUserGrantRoleResponse, error) {
	resp, err := auth.remote.UserGrantRole(ctx, &pb.AuthUserGrantRoleRequest{User: user, Role: role}, auth.callOpts...)
	return (*AuthUserGrantRoleResponse)(resp), toErr(ctx, err)
}

func (auth *authClient) UserGet(ctx context.Context, name string) (*AuthUserGetResponse, error) {
	resp, err := auth.remote.UserGet(ctx, &pb.AuthUserGetRequest{Name: name}, auth.callOpts...)
	return (*AuthUserGetResponse)(resp), toErr(ctx, err)
}

func (auth *authClient) UserList(ctx context.Context) (*AuthUserListResponse, error) {
	resp, err := auth.remote.UserList(ctx, &pb.AuthUserListRequest{}, auth.callOpts...)
	return (*AuthUserListResponse)(resp), toErr(ctx, err)
}

func (auth *authClient) UserRevokeRole(ctx context.Context, name string, role string) (*AuthUserRevokeRoleResponse, error) {
	resp, err := auth.remote.UserRevokeRole(ctx, &pb.AuthUserRevokeRoleRequest{Name: name, Role: role}, auth.callOpts...)
	return (*AuthUserRevokeRoleResponse)(resp), toErr(ctx, err)
}

// RoleAdd ok
func (auth *authClient) RoleAdd(ctx context.Context, name string) (*AuthRoleAddResponse, error) {
	resp, err := auth.remote.RoleAdd(ctx, &pb.AuthRoleAddRequest{Name: name}, auth.callOpts...)
	return (*AuthRoleAddResponse)(resp), toErr(ctx, err)
}

// RoleGrantPermission ok
func (auth *authClient) RoleGrantPermission(ctx context.Context, name string, key, rangeEnd string, permType PermissionType) (*AuthRoleGrantPermissionResponse, error) {
	perm := &authpb.Permission{
		Key:      key,
		RangeEnd: rangeEnd,
		PermType: authpb.Permission_Type(permType),
	}
	resp, err := auth.remote.RoleGrantPermission(ctx, &pb.AuthRoleGrantPermissionRequest{Name: name, Perm: perm}, auth.callOpts...)
	return (*AuthRoleGrantPermissionResponse)(resp), toErr(ctx, err)
}

// RoleGet ok
func (auth *authClient) RoleGet(ctx context.Context, role string) (*AuthRoleGetResponse, error) {
	resp, err := auth.remote.RoleGet(ctx, &pb.AuthRoleGetRequest{Role: role}, auth.callOpts...)
	return (*AuthRoleGetResponse)(resp), toErr(ctx, err)
}

// RoleList ok
func (auth *authClient) RoleList(ctx context.Context) (*AuthRoleListResponse, error) {
	resp, err := auth.remote.RoleList(ctx, &pb.AuthRoleListRequest{}, auth.callOpts...)
	return (*AuthRoleListResponse)(resp), toErr(ctx, err)
}

func (auth *authClient) RoleRevokePermission(ctx context.Context, role string, key, rangeEnd string) (*AuthRoleRevokePermissionResponse, error) {
	resp, err := auth.remote.RoleRevokePermission(ctx, &pb.AuthRoleRevokePermissionRequest{Role: role, Key: string(key), RangeEnd: string(rangeEnd)}, auth.callOpts...)
	return (*AuthRoleRevokePermissionResponse)(resp), toErr(ctx, err)
}

func (auth *authClient) RoleDelete(ctx context.Context, role string) (*AuthRoleDeleteResponse, error) {
	resp, err := auth.remote.RoleDelete(ctx, &pb.AuthRoleDeleteRequest{Role: role}, auth.callOpts...)
	return (*AuthRoleDeleteResponse)(resp), toErr(ctx, err)
}

func StrToPermissionType(s string) (PermissionType, error) {
	val, ok := authpb.PermissionTypeValue[strings.ToUpper(s)]
	if ok {
		return PermissionType(val), nil
	}
	return PermissionType(-1), fmt.Errorf("无效的权限类型: %s", s)
}
