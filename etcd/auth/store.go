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

package auth

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/binary"
	"errors"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ls-2018/etcd_cn/etcd/mvcc/backend"
	"github.com/ls-2018/etcd_cn/etcd/mvcc/buckets"
	"github.com/ls-2018/etcd_cn/offical/api/v3/authpb"
	"github.com/ls-2018/etcd_cn/offical/api/v3/v3rpc/rpctypes"
	pb "github.com/ls-2018/etcd_cn/offical/etcdserverpb"

	"go.uber.org/zap"
	"golang.org/x/crypto/bcrypt"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/peer"
)

var (
	enableFlagKey = []byte("authEnabled")
	authEnabled   = []byte{1}
	authDisabled  = []byte{0}

	revisionKey = []byte("authRevision") // 鉴权版本号

	ErrRootUserNotExist     = errors.New("auth: root用户不存在")
	ErrRootRoleNotExist     = errors.New("auth: root用户没有root角色")
	ErrUserAlreadyExist     = errors.New("auth: 用户已存在")
	ErrUserEmpty            = errors.New("auth: 用户名是空的")
	ErrUserNotFound         = errors.New("auth: 没有找到该用户")
	ErrRoleAlreadyExist     = errors.New("auth: 角色已存在")
	ErrRoleNotFound         = errors.New("auth: 角色不存在")
	ErrRoleEmpty            = errors.New("auth: 角色名不能为空")
	ErrPermissionNotGiven   = errors.New("auth: permission not given")
	ErrAuthFailed           = errors.New("auth: authentication failed, invalid user ID or password")
	ErrNoPasswordUser       = errors.New("auth: authentication failed, password was given for no password user")
	ErrPermissionDenied     = errors.New("auth: permission denied")
	ErrRoleNotGranted       = errors.New("auth: role is not granted to the user")
	ErrPermissionNotGranted = errors.New("auth: 角色没有权限")
	ErrAuthNotEnabled       = errors.New("auth: authentication is not enabled")
	ErrAuthOldRevision      = errors.New("auth: 请求头里的修订版本是旧的")
	ErrInvalidAuthToken     = errors.New("auth: invalid auth token")
	ErrInvalidAuthOpts      = errors.New("auth: invalid auth options")
	ErrInvalidAuthMgmt      = errors.New("auth: invalid auth management")
	ErrInvalidAuthMethod    = errors.New("auth: invalid auth signature method")
	ErrMissingKey           = errors.New("auth: missing key data")
	ErrKeyMismatch          = errors.New("auth: public and private keys don't match")
	ErrVerifyOnly           = errors.New("auth: token signing attempted with verify-only key")
)

const (
	rootUser        = "root"
	rootRole        = "root"
	tokenTypeSimple = "simple"
	tokenTypeJWT    = "jwt"
	revBytesLen     = 8
)

type AuthInfo struct {
	Username string
	Revision uint64
}

// AuthenticateParamIndex is used for a key of context in the parameters of Authenticate()
type AuthenticateParamIndex struct{}

// AuthenticateParamSimpleTokenPrefix is used for a key of context in the parameters of Authenticate()
type AuthenticateParamSimpleTokenPrefix struct{}

type AuthStore interface {
	AuthEnable() error
	AuthDisable()
	IsAuthEnabled() bool
	Authenticate(ctx context.Context, username, password string) (*pb.AuthenticateResponse, error)
	// Recover recovers the state of auth store from the given backend
	Recover(b backend.Backend)
	UserAdd(r *pb.AuthUserAddRequest) (*pb.AuthUserAddResponse, error)
	UserDelete(r *pb.AuthUserDeleteRequest) (*pb.AuthUserDeleteResponse, error)
	UserChangePassword(r *pb.AuthUserChangePasswordRequest) (*pb.AuthUserChangePasswordResponse, error)
	UserGrantRole(r *pb.AuthUserGrantRoleRequest) (*pb.AuthUserGrantRoleResponse, error)
	UserGet(r *pb.AuthUserGetRequest) (*pb.AuthUserGetResponse, error)
	UserRevokeRole(r *pb.AuthUserRevokeRoleRequest) (*pb.AuthUserRevokeRoleResponse, error)

	RoleAdd(r *pb.AuthRoleAddRequest) (*pb.AuthRoleAddResponse, error)
	RoleGrantPermission(r *pb.AuthRoleGrantPermissionRequest) (*pb.AuthRoleGrantPermissionResponse, error)
	RoleGet(r *pb.AuthRoleGetRequest) (*pb.AuthRoleGetResponse, error)

	RoleRevokePermission(r *pb.AuthRoleRevokePermissionRequest) (*pb.AuthRoleRevokePermissionResponse, error)
	RoleDelete(r *pb.AuthRoleDeleteRequest) (*pb.AuthRoleDeleteResponse, error)
	UserList(r *pb.AuthUserListRequest) (*pb.AuthUserListResponse, error)
	RoleList(r *pb.AuthRoleListRequest) (*pb.AuthRoleListResponse, error)
	IsPutPermitted(authInfo *AuthInfo, key []byte) error
	IsRangePermitted(authInfo *AuthInfo, key, rangeEnd []byte) error       // 检查用户的范围权限
	IsDeleteRangePermitted(authInfo *AuthInfo, key, rangeEnd []byte) error //
	IsAdminPermitted(authInfo *AuthInfo) error                             //
	GenTokenPrefix() (string, error)                                       // 在简单令牌的情况下生成一个随机字符串,在JWT的情况下,它生成一个空字符串
	Revision() uint64                                                      //
	CheckPassword(username, password string) (uint64, error)               // 检查给定的一对用户名和密码是否正确
	Close() error                                                          // 清理AuthStore
	AuthInfoFromCtx(ctx context.Context) (*AuthInfo, error)                // 从grpc上下文获取认证信息
	AuthInfoFromTLS(ctx context.Context) *AuthInfo                         // 从grpc证书上下文获取认证信息
	WithRoot(ctx context.Context) context.Context                          // 生成并安装可作为根凭据使用的令牌
	UserHasRole(user, role string) bool                                    // 检查用户是否有该角色
	BcryptCost() int                                                       // 获取加密认证密码的散列强度
}

type TokenProvider interface {
	info(ctx context.Context, token string, revision uint64) (*AuthInfo, bool)
	assign(ctx context.Context, username string, revision uint64) (string, error)
	enable()
	disable()
	invalidateUser(string)
	genTokenPrefix() (string, error)
}

type authStore struct {
	revision       uint64                              // 鉴权版本号
	lg             *zap.Logger                         //
	be             backend.Backend                     //
	enabled        bool                                // 是否开启认证
	enabledMu      sync.RWMutex                        //
	rangePermCache map[string]*unifiedRangePermissions // username -> unifiedRangePermissions
	tokenProvider  TokenProvider                       // TODO
	bcryptCost     int                                 // the algorithm cost / strength for hashing auth passwords
}

func (as *authStore) AuthEnable() error {
	as.enabledMu.Lock()
	defer as.enabledMu.Unlock()
	if as.enabled {
		as.lg.Info("authentication is already enabled; ignored auth enable request")
		return nil
	}
	b := as.be
	tx := b.BatchTx()
	tx.Lock()
	defer func() {
		tx.Unlock()
		b.ForceCommit()
	}()

	u := getUser(as.lg, tx, rootUser)
	if u == nil {
		return ErrRootUserNotExist
	}

	if !hasRootRole(u) {
		return ErrRootRoleNotExist
	}

	tx.UnsafePut(buckets.Auth, enableFlagKey, authEnabled)

	as.enabled = true
	as.tokenProvider.enable()

	as.rangePermCache = make(map[string]*unifiedRangePermissions)

	as.setRevision(getRevision(tx))

	as.lg.Info("enabled authentication")
	return nil
}

func (as *authStore) AuthDisable() {
	as.enabledMu.Lock()
	defer as.enabledMu.Unlock()
	if !as.enabled {
		return
	}
	b := as.be
	tx := b.BatchTx()
	tx.Lock()
	tx.UnsafePut(buckets.Auth, enableFlagKey, authDisabled)
	as.commitRevision(tx)
	tx.Unlock()
	b.ForceCommit()

	as.enabled = false
	as.tokenProvider.disable()

	as.lg.Info("disabled authentication")
}

func (as *authStore) Close() error {
	as.enabledMu.Lock()
	defer as.enabledMu.Unlock()
	if !as.enabled {
		return nil
	}
	as.tokenProvider.disable()
	return nil
}

func (as *authStore) Authenticate(ctx context.Context, username, password string) (*pb.AuthenticateResponse, error) {
	if !as.IsAuthEnabled() {
		return nil, ErrAuthNotEnabled
	}

	tx := as.be.BatchTx()
	tx.Lock()
	defer tx.Unlock()

	user := getUser(as.lg, tx, username)
	if user == nil {
		return nil, ErrAuthFailed
	}

	if user.Options != nil && user.Options.NoPassword {
		return nil, ErrAuthFailed
	}

	// 密码在API已经校验了,因此在这不用再校验
	token, err := as.tokenProvider.assign(ctx, username, as.Revision())
	if err != nil {
		return nil, err
	}

	as.lg.Debug("用户认证", zap.String("user-name", username), zap.String("token", token))
	return &pb.AuthenticateResponse{Token: token}, nil
}

func (as *authStore) Recover(be backend.Backend) {
	enabled := false
	as.be = be
	tx := be.BatchTx()
	tx.Lock()
	_, vs := tx.UnsafeRange(buckets.Auth, enableFlagKey, nil, 0)
	if len(vs) == 1 {
		if bytes.Equal(vs[0], authEnabled) {
			enabled = true
		}
	}

	as.setRevision(getRevision(tx))

	tx.Unlock()

	as.enabledMu.Lock()
	as.enabled = enabled
	as.enabledMu.Unlock()
}

func (as *authStore) authInfoFromToken(ctx context.Context, token string) (*AuthInfo, bool) {
	return as.tokenProvider.info(ctx, token, as.Revision())
}

func (as *authStore) isOpPermitted(userName string, revision uint64, key, rangeEnd []byte, permTyp authpb.Permission_Type) error {
	// 这个函数的开销很大,所以我们需要一个缓存机制
	if !as.IsAuthEnabled() {
		return nil
	}

	// only gets rev == 0 when passed AuthInfo{}; no user given
	if revision == 0 {
		return ErrUserEmpty
	}
	rev := as.Revision()
	if revision < rev {
		as.lg.Warn("请求认证的版本小于当前节点认证的版本",
			zap.Uint64("current node auth revision", rev),
			zap.Uint64("request auth revision", revision),
			zap.ByteString("request key", key),
			zap.Error(ErrAuthOldRevision))
		return ErrAuthOldRevision
	}

	tx := as.be.BatchTx()
	tx.Lock()
	defer tx.Unlock()

	user := getUser(as.lg, tx, userName)
	if user == nil {
		as.lg.Error("cannot find a user for permission check", zap.String("user-name", userName))
		return ErrPermissionDenied
	}

	// root role should have permission on all ranges
	if hasRootRole(user) {
		return nil
	}

	if as.isRangeOpPermitted(tx, userName, key, rangeEnd, permTyp) {
		return nil
	}

	return ErrPermissionDenied
}

func (as *authStore) IsPutPermitted(authInfo *AuthInfo, key []byte) error {
	return as.isOpPermitted(authInfo.Username, authInfo.Revision, key, nil, authpb.WRITE)
}

func (as *authStore) IsRangePermitted(authInfo *AuthInfo, key, rangeEnd []byte) error {
	return as.isOpPermitted(authInfo.Username, authInfo.Revision, key, rangeEnd, authpb.READ) // '' ,0 ,health,nil
}

func (as *authStore) IsDeleteRangePermitted(authInfo *AuthInfo, key, rangeEnd []byte) error {
	return as.isOpPermitted(authInfo.Username, authInfo.Revision, key, rangeEnd, authpb.WRITE)
}

func (as *authStore) IsAdminPermitted(authInfo *AuthInfo) error {
	if !as.IsAuthEnabled() {
		return nil
	}
	if authInfo == nil || authInfo.Username == "" {
		return ErrUserEmpty
	}

	tx := as.be.BatchTx()
	tx.Lock()
	u := getUser(as.lg, tx, authInfo.Username)
	tx.Unlock()

	if u == nil {
		return ErrUserNotFound
	}

	if !hasRootRole(u) {
		return ErrPermissionDenied
	}

	return nil
}

// IsAuthEnabled 是否启用认证
func (as *authStore) IsAuthEnabled() bool {
	as.enabledMu.RLock()
	defer as.enabledMu.RUnlock()
	return as.enabled
}

// NewAuthStore creates a new AuthStore.
func NewAuthStore(lg *zap.Logger, be backend.Backend, tp TokenProvider, bcryptCost int) *authStore {
	if lg == nil {
		lg = zap.NewNop()
	}

	if bcryptCost < bcrypt.MinCost || bcryptCost > bcrypt.MaxCost {
		lg.Warn(
			"使用默认的加密强度替换提供的加密强度",
			zap.Int("min-cost", bcrypt.MinCost),
			zap.Int("max-cost", bcrypt.MaxCost),
			zap.Int("default-cost", bcrypt.DefaultCost),
			zap.Int("given-cost", bcryptCost),
		)
		bcryptCost = bcrypt.DefaultCost
	}

	tx := be.BatchTx()
	tx.Lock()

	tx.UnsafeCreateBucket(buckets.Auth)
	tx.UnsafeCreateBucket(buckets.AuthUsers)
	tx.UnsafeCreateBucket(buckets.AuthRoles)

	enabled := false
	_, vs := tx.UnsafeRange(buckets.Auth, enableFlagKey, nil, 0)
	if len(vs) == 1 {
		if bytes.Equal(vs[0], authEnabled) {
			enabled = true
		}
	}

	as := &authStore{
		revision:       getRevision(tx),
		lg:             lg,
		be:             be,
		enabled:        enabled,
		rangePermCache: make(map[string]*unifiedRangePermissions),
		tokenProvider:  tp,
		bcryptCost:     bcryptCost,
	}

	if enabled {
		as.tokenProvider.enable()
	}

	if as.Revision() == 0 {
		as.commitRevision(tx)
	}

	tx.Unlock()
	be.ForceCommit()

	return as
}

func hasRootRole(u *authpb.User) bool {
	// u.Roles is sorted in UserGrantRole(), so we can use binary search.
	idx := sort.SearchStrings(u.Roles, rootRole)
	return idx != len(u.Roles) && u.Roles[idx] == rootRole
}

// ok 持久化,鉴权版本号
func (as *authStore) commitRevision(tx backend.BatchTx) {
	atomic.AddUint64(&as.revision, 1)
	revBytes := make([]byte, revBytesLen)
	binary.BigEndian.PutUint64(revBytes, as.Revision())
	tx.UnsafePut(buckets.Auth, revisionKey, revBytes)
}

// ok
func getRevision(tx backend.BatchTx) uint64 {
	_, vs := tx.UnsafeRange(buckets.Auth, revisionKey, nil, 0)
	if len(vs) != 1 {
		return 0
	}
	return binary.BigEndian.Uint64(vs[0])
}

// ok

func (as *authStore) setRevision(rev uint64) {
	atomic.StoreUint64(&as.revision, rev)
}

// Revision 返回鉴权版本号
func (as *authStore) Revision() uint64 {
	return atomic.LoadUint64(&as.revision)
}

func (as *authStore) AuthInfoFromTLS(ctx context.Context) (ai *AuthInfo) {
	peer, ok := peer.FromContext(ctx)
	if !ok || peer == nil || peer.AuthInfo == nil {
		return nil
	}

	tlsInfo := peer.AuthInfo.(credentials.TLSInfo)
	for _, chains := range tlsInfo.State.VerifiedChains {
		if len(chains) < 1 {
			continue
		}
		ai = &AuthInfo{
			Username: chains[0].Subject.CommonName,
			Revision: as.Revision(),
		}
		md, ok := metadata.FromIncomingContext(ctx)
		if !ok {
			return nil
		}

		// gRPC-gateway proxy request to etcd etcd includes Grpcgateway-Accept
		// header. The proxy uses etcd client etcd certificate. If the certificate
		// has a CommonName we should never use this for authentication.
		if gw := md["grpcgateway-accept"]; len(gw) > 0 {
			as.lg.Warn(
				"ignoring common name in gRPC-gateway proxy request",
				zap.String("common-name", ai.Username),
				zap.String("user-name", ai.Username),
				zap.Uint64("revision", ai.Revision),
			)
			return nil
		}
		as.lg.Debug(
			"found command name",
			zap.String("common-name", ai.Username),
			zap.String("user-name", ai.Username),
			zap.Uint64("revision", ai.Revision),
		)
		break
	}
	return ai
}

func (as *authStore) AuthInfoFromCtx(ctx context.Context) (*AuthInfo, error) {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return nil, nil
	}

	// TODO(mitake|hexfusion) review unifying key names
	ts, ok := md[rpctypes.TokenFieldNameGRPC]
	if !ok {
		ts, ok = md[rpctypes.TokenFieldNameSwagger]
	}
	if !ok {
		return nil, nil
	}

	token := ts[0]
	authInfo, uok := as.authInfoFromToken(ctx, token)
	if !uok {
		as.lg.Warn("invalid auth token", zap.String("token", token))
		return nil, ErrInvalidAuthToken
	}

	return authInfo, nil
}

func (as *authStore) GenTokenPrefix() (string, error) {
	return as.tokenProvider.genTokenPrefix()
}

func decomposeOpts(lg *zap.Logger, optstr string) (string, map[string]string, error) {
	opts := strings.Split(optstr, ",")
	tokenType := opts[0]

	typeSpecificOpts := make(map[string]string)
	for i := 1; i < len(opts); i++ {
		pair := strings.Split(opts[i], "=")

		if len(pair) != 2 {
			if lg != nil {
				lg.Error("invalid token option", zap.String("option", optstr))
			}
			return "", nil, ErrInvalidAuthOpts
		}

		if _, ok := typeSpecificOpts[pair[0]]; ok {
			if lg != nil {
				lg.Error(
					"invalid token option",
					zap.String("option", optstr),
					zap.String("duplicate-parameter", pair[0]),
				)
			}
			return "", nil, ErrInvalidAuthOpts
		}

		typeSpecificOpts[pair[0]] = pair[1]
	}

	return tokenType, typeSpecificOpts, nil
}

func NewTokenProvider(lg *zap.Logger, tokenOpts string, indexWaiter func(uint64) <-chan struct{}, TokenTTL time.Duration) (TokenProvider, error) { // token提供商
	tokenType, typeSpecificOpts, err := decomposeOpts(lg, tokenOpts) // 认证格式  simple、jwt
	if err != nil {
		return nil, ErrInvalidAuthOpts
	}

	switch tokenType {
	case tokenTypeSimple:
		if lg != nil {
			lg.Warn("简单令牌没有经过加密签名")
		}
		return newTokenProviderSimple(lg, indexWaiter, TokenTTL), nil

	case tokenTypeJWT:
		return newTokenProviderJWT(lg, typeSpecificOpts)

	case "":
		return newTokenProviderNop()

	default:
		if lg != nil {
			lg.Warn(
				"unknown token type",
				zap.String("type", tokenType),
				zap.Error(ErrInvalidAuthOpts),
			)
		}
		return nil, ErrInvalidAuthOpts
	}
}

func (as *authStore) WithRoot(ctx context.Context) context.Context {
	if !as.IsAuthEnabled() {
		return ctx
	}

	var ctxForAssign context.Context
	if ts, ok := as.tokenProvider.(*tokenSimple); ok && ts != nil {
		ctx1 := context.WithValue(ctx, AuthenticateParamIndex{}, uint64(0))
		prefix, err := ts.genTokenPrefix()
		if err != nil {
			as.lg.Error(
				"failed to generate prefix of internally used token",
				zap.Error(err),
			)
			return ctx
		}
		ctxForAssign = context.WithValue(ctx1, AuthenticateParamSimpleTokenPrefix{}, prefix)
	} else {
		ctxForAssign = ctx
	}

	token, err := as.tokenProvider.assign(ctxForAssign, "root", as.Revision())
	if err != nil {
		// this must not happen
		as.lg.Error(
			"failed to assign token for lease revoking",
			zap.Error(err),
		)
		return ctx
	}

	mdMap := map[string]string{
		rpctypes.TokenFieldNameGRPC: token,
	}
	tokenMD := metadata.New(mdMap)

	// use "mdIncomingKey{}" since it's called from local etcdserver
	return metadata.NewIncomingContext(ctx, tokenMD)
}

func (as *authStore) BcryptCost() int {
	return as.bcryptCost
}

// ---------------------------------------------------------------------------------------------------v

// RoleRevokePermission ok
func (as *authStore) RoleRevokePermission(r *pb.AuthRoleRevokePermissionRequest) (*pb.AuthRoleRevokePermissionResponse, error) {
	tx := as.be.BatchTx()
	tx.Lock()
	defer tx.Unlock()

	role := getRole(as.lg, tx, r.Role)
	if role == nil {
		return nil, ErrRoleNotFound
	}

	updatedRole := &authpb.Role{
		Name: role.Name,
	}

	for _, perm := range role.KeyPermission {
		if !strings.EqualFold(perm.Key, r.Key) || !strings.EqualFold(perm.RangeEnd, r.RangeEnd) {
			updatedRole.KeyPermission = append(updatedRole.KeyPermission, perm)
		}
	}

	if len(role.KeyPermission) == len(updatedRole.KeyPermission) {
		return nil, ErrPermissionNotGranted
	}

	putRole(as.lg, tx, updatedRole)

	as.clearCachedPerm()
	as.commitRevision(tx)

	as.lg.Info("撤销对range的权限", zap.String("role-name", r.Role), zap.String("key", r.Key), zap.String("range-end", r.RangeEnd))
	return &pb.AuthRoleRevokePermissionResponse{}, nil
}

// RoleGrantPermission ok
func (as *authStore) RoleGrantPermission(r *pb.AuthRoleGrantPermissionRequest) (*pb.AuthRoleGrantPermissionResponse, error) {
	if r.Perm == nil {
		return nil, ErrPermissionNotGiven
	}

	tx := as.be.BatchTx()
	tx.Lock()
	defer tx.Unlock()

	role := getRole(as.lg, tx, r.Name)
	if role == nil {
		return nil, ErrRoleNotFound
	}
	// 在已有的权限中, 寻找第一个与key相等的,没找到的话 idx =len(role.KeyPermission)
	idx := sort.Search(len(role.KeyPermission), func(i int) bool {
		// a,a 0
		// a b -1
		// b a 1
		// a,b,c,d,e
		// c
		return strings.Compare(role.KeyPermission[i].Key, r.Perm.Key) >= 0
	})

	if idx < len(role.KeyPermission) && strings.EqualFold(role.KeyPermission[idx].Key, r.Perm.Key) && strings.EqualFold(role.KeyPermission[idx].RangeEnd, r.Perm.RangeEnd) {
		// 更新存在的权限
		role.KeyPermission[idx].PermType = r.Perm.PermType
	} else {
		newPerm := &authpb.Permission{
			Key:      r.Perm.Key,      // /
			RangeEnd: r.Perm.RangeEnd, // ""
			PermType: r.Perm.PermType, // readwrite
		}

		role.KeyPermission = append(role.KeyPermission, newPerm)
		// 按照key 升序排列
		sort.Sort(permSlice(role.KeyPermission))
	}

	putRole(as.lg, tx, role)
	// 目前,单个角色更新会使每个缓存失效,应该进行优化.
	as.clearCachedPerm()

	as.commitRevision(tx)

	as.lg.Info("授予/更新用户权限", zap.String("user-name", r.Name), zap.String("permission-name", authpb.PermissionTypeName[int32(r.Perm.PermType)]))
	return &pb.AuthRoleGrantPermissionResponse{}, nil
}

// RoleList ok
func (as *authStore) RoleList(r *pb.AuthRoleListRequest) (*pb.AuthRoleListResponse, error) {
	tx := as.be.BatchTx()
	tx.Lock()
	roles := getAllRoles(as.lg, tx)
	tx.Unlock()

	resp := &pb.AuthRoleListResponse{Roles: make([]string, len(roles))}
	for i := range roles {
		resp.Roles[i] = string(roles[i].Name)
	}
	return resp, nil
}

// RoleDelete OK
func (as *authStore) RoleDelete(r *pb.AuthRoleDeleteRequest) (*pb.AuthRoleDeleteResponse, error) {
	if as.enabled && r.Role == rootRole {
		as.lg.Error("不能删除 'root' 角色", zap.String("role-name", r.Role))
		return nil, ErrInvalidAuthMgmt
	}

	tx := as.be.BatchTx()
	tx.Lock()
	defer tx.Unlock()

	role := getRole(as.lg, tx, r.Role)
	if role == nil {
		return nil, ErrRoleNotFound
	}

	delRole(tx, r.Role)

	users := getAllUsers(as.lg, tx) // 获取所有用户
	for _, user := range users {
		updatedUser := &authpb.User{
			Name:     user.Name,
			Password: user.Password,
			Options:  user.Options,
		}
		for _, role := range user.Roles {
			if role != r.Role {
				updatedUser.Roles = append(updatedUser.Roles, role)
			}
		}
		if len(updatedUser.Roles) == len(user.Roles) {
			continue
		}
		putUser(as.lg, tx, updatedUser)
		as.invalidateCachedPerm(user.Name)
	}

	as.commitRevision(tx)

	as.lg.Info("删除了一个角色", zap.String("role-name", r.Role))
	return &pb.AuthRoleDeleteResponse{}, nil
}

// RoleAdd OK
func (as *authStore) RoleAdd(r *pb.AuthRoleAddRequest) (*pb.AuthRoleAddResponse, error) {
	if len(r.Name) == 0 {
		return nil, ErrRoleEmpty
	}

	tx := as.be.BatchTx()
	tx.Lock()
	defer tx.Unlock()

	role := getRole(as.lg, tx, r.Name)
	if role != nil {
		return nil, ErrRoleAlreadyExist
	}

	newRole := &authpb.Role{
		Name: r.Name,
	}

	putRole(as.lg, tx, newRole)

	as.commitRevision(tx)

	as.lg.Info("创建了一个角色", zap.String("role-name", r.Name))
	return &pb.AuthRoleAddResponse{}, nil
}

// RoleGet ok
func (as *authStore) RoleGet(r *pb.AuthRoleGetRequest) (*pb.AuthRoleGetResponse, error) {
	tx := as.be.BatchTx()
	tx.Lock()
	defer tx.Unlock()

	var resp pb.AuthRoleGetResponse

	role := getRole(as.lg, tx, r.Role)
	if role == nil {
		return nil, ErrRoleNotFound
	}
	resp.Perm = append(resp.Perm, role.KeyPermission...)
	return &resp, nil
}

func (as *authStore) UserHasRole(user, role string) bool {
	tx := as.be.BatchTx()
	tx.Lock()
	u := getUser(as.lg, tx, user)
	tx.Unlock()

	if u == nil {
		as.lg.Warn("'has-role'请求不存在的用户", zap.String("user-name", user), zap.String("role-name", role))
		return false
	}

	for _, r := range u.Roles {
		if role == r {
			return true
		}
	}
	return false
}

func getRole(lg *zap.Logger, tx backend.BatchTx, rolename string) *authpb.Role {
	_, vs := tx.UnsafeRange(buckets.AuthRoles, []byte(rolename), nil, 0)
	if len(vs) == 0 {
		return nil
	}

	role := &authpb.Role{}
	err := role.Unmarshal(vs[0])
	if err != nil {
		lg.Panic("反序列化失败 'authpb.Role'", zap.Error(err))
	}
	return role
}

func getAllRoles(lg *zap.Logger, tx backend.BatchTx) []*authpb.Role {
	_, vs := tx.UnsafeRange(buckets.AuthRoles, []byte{0}, []byte{0xff}, -1)
	if len(vs) == 0 {
		return nil
	}

	roles := make([]*authpb.Role, len(vs))
	for i := range vs {
		role := &authpb.Role{}
		err := role.Unmarshal(vs[i])
		if err != nil {
			lg.Panic("failed to unmarshal 'authpb.Role'", zap.Error(err))
		}
		roles[i] = role
	}
	return roles
}

// ok
func putRole(lg *zap.Logger, tx backend.BatchTx, role *authpb.Role) {
	b, err := role.Marshal()
	if err != nil {
		lg.Panic("序列化失败'authpb.Role'", zap.String("role-name", role.Name), zap.Error(err))
	}

	tx.UnsafePut(buckets.AuthRoles, []byte(role.Name), b)
}

// ok
func delRole(tx backend.BatchTx, rolename string) {
	tx.UnsafeDelete(buckets.AuthRoles, []byte(rolename))
}

type permSlice []*authpb.Permission

func (perms permSlice) Len() int {
	return len(perms)
}

func (perms permSlice) Less(i, j int) bool {
	// a,a 0
	// a b -1
	// b a 1

	return strings.Compare(perms[i].Key, perms[j].Key) < 0
}

func (perms permSlice) Swap(i, j int) {
	perms[i], perms[j] = perms[j], perms[i]
}

// ---------------------------------------------------------------------------------------------------v

// 生成密码
func (as *authStore) selectPassword(password string, hashedPassword string) ([]byte, error) {
	if password != "" && hashedPassword == "" {
		// 此路径用于处理由版本大于3.5的etcd创建的日志条目
		return bcrypt.GenerateFromPassword([]byte(password), as.bcryptCost)
	}
	return base64.StdEncoding.DecodeString(hashedPassword)
}

func (as *authStore) CheckPassword(username, password string) (uint64, error) {
	if !as.IsAuthEnabled() {
		return 0, ErrAuthNotEnabled
	}

	var user *authpb.User
	// CompareHashAndPassword is very expensive, so we use closures
	// to avoid putting it in the critical section of the tx lock.
	revision, err := func() (uint64, error) {
		tx := as.be.BatchTx()
		tx.Lock()
		defer tx.Unlock()

		user = getUser(as.lg, tx, username)
		if user == nil {
			return 0, ErrAuthFailed
		}

		if user.Options != nil && user.Options.NoPassword {
			return 0, ErrNoPasswordUser
		}

		return getRevision(tx), nil
	}()
	if err != nil {
		return 0, err
	}

	if bcrypt.CompareHashAndPassword([]byte(user.Password), []byte(password)) != nil {
		as.lg.Info("invalid password", zap.String("user-name", username))
		return 0, ErrAuthFailed
	}
	return revision, nil
}

func (as *authStore) UserAdd(r *pb.AuthUserAddRequest) (*pb.AuthUserAddResponse, error) {
	if len(r.Name) == 0 {
		return nil, ErrUserEmpty
	}

	tx := as.be.BatchTx()
	tx.Lock()
	defer tx.Unlock()

	user := getUser(as.lg, tx, r.Name)
	if user != nil {
		return nil, ErrUserAlreadyExist
	}

	options := r.Options
	if options == nil {
		options = &authpb.UserAddOptions{
			NoPassword: false,
		}
	}

	var password []byte
	var err error

	if !options.NoPassword {
		password, err = as.selectPassword(r.Password, r.HashedPassword)
		if err != nil {
			return nil, ErrNoPasswordUser
		}
	}

	newUser := &authpb.User{
		Name:     r.Name,
		Password: string(password),
		Options:  options,
	}

	putUser(as.lg, tx, newUser)

	as.commitRevision(tx)

	as.lg.Info("添加一个用户", zap.String("user-name", r.Name))
	return &pb.AuthUserAddResponse{}, nil
}

func (as *authStore) UserDelete(r *pb.AuthUserDeleteRequest) (*pb.AuthUserDeleteResponse, error) {
	if as.enabled && r.Name == rootUser {
		as.lg.Error("不能删除 'root' 用户", zap.String("user-name", r.Name))
		return nil, ErrInvalidAuthMgmt
	}

	tx := as.be.BatchTx()
	tx.Lock()
	defer tx.Unlock()

	user := getUser(as.lg, tx, r.Name)
	if user == nil {
		return nil, ErrUserNotFound
	}

	delUser(tx, r.Name)

	as.commitRevision(tx)

	as.invalidateCachedPerm(r.Name)
	as.tokenProvider.invalidateUser(r.Name)

	as.lg.Info(
		"删除了一个用户",
		zap.String("user-name", r.Name),
		zap.Strings("user-roles", user.Roles),
	)
	return &pb.AuthUserDeleteResponse{}, nil
}

func (as *authStore) UserChangePassword(r *pb.AuthUserChangePasswordRequest) (*pb.AuthUserChangePasswordResponse, error) {
	tx := as.be.BatchTx()
	tx.Lock()
	defer tx.Unlock()

	user := getUser(as.lg, tx, r.Name)
	if user == nil {
		return nil, ErrUserNotFound
	}

	var password []byte
	var err error

	if !user.Options.NoPassword {
		password, err = as.selectPassword(r.Password, r.HashedPassword)
		if err != nil {
			return nil, ErrNoPasswordUser
		}
	}

	updatedUser := &authpb.User{
		Name:     r.Name,
		Roles:    user.Roles,
		Password: string(password),
		Options:  user.Options,
	}

	putUser(as.lg, tx, updatedUser)

	as.commitRevision(tx)

	as.invalidateCachedPerm(r.Name)
	as.tokenProvider.invalidateUser(r.Name)

	as.lg.Info(
		"更该用户密码",
		zap.String("user-name", r.Name),
		zap.Strings("user-roles", user.Roles),
	)
	return &pb.AuthUserChangePasswordResponse{}, nil
}

func (as *authStore) UserGrantRole(r *pb.AuthUserGrantRoleRequest) (*pb.AuthUserGrantRoleResponse, error) {
	tx := as.be.BatchTx()
	tx.Lock()
	defer tx.Unlock()

	user := getUser(as.lg, tx, r.User)
	if user == nil {
		return nil, ErrUserNotFound
	}

	if r.Role != rootRole {
		role := getRole(as.lg, tx, r.Role)
		if role == nil {
			return nil, ErrRoleNotFound
		}
	}

	idx := sort.SearchStrings(user.Roles, r.Role)
	if idx < len(user.Roles) && user.Roles[idx] == r.Role {
		as.lg.Warn(
			"ignored grant role request to a user",
			zap.String("user-name", r.User),
			zap.Strings("user-roles", user.Roles),
			zap.String("duplicate-role-name", r.Role),
		)
		return &pb.AuthUserGrantRoleResponse{}, nil
	}

	user.Roles = append(user.Roles, r.Role)
	sort.Strings(user.Roles)

	putUser(as.lg, tx, user)

	as.invalidateCachedPerm(r.User)

	as.commitRevision(tx)

	as.lg.Info(
		"granted a role to a user",
		zap.String("user-name", r.User),
		zap.Strings("user-roles", user.Roles),
		zap.String("added-role-name", r.Role),
	)
	return &pb.AuthUserGrantRoleResponse{}, nil
}

func (as *authStore) UserGet(r *pb.AuthUserGetRequest) (*pb.AuthUserGetResponse, error) {
	tx := as.be.BatchTx()
	tx.Lock()
	user := getUser(as.lg, tx, r.Name)
	tx.Unlock()

	if user == nil {
		return nil, ErrUserNotFound
	}

	var resp pb.AuthUserGetResponse
	resp.Roles = append(resp.Roles, user.Roles...)
	return &resp, nil
}

func (as *authStore) UserList(r *pb.AuthUserListRequest) (*pb.AuthUserListResponse, error) {
	tx := as.be.BatchTx()
	tx.Lock()
	users := getAllUsers(as.lg, tx)
	tx.Unlock()

	resp := &pb.AuthUserListResponse{Users: make([]string, len(users))}
	for i := range users {
		resp.Users[i] = users[i].Name
	}
	return resp, nil
}

func (as *authStore) UserRevokeRole(r *pb.AuthUserRevokeRoleRequest) (*pb.AuthUserRevokeRoleResponse, error) {
	if as.enabled && r.Name == rootUser && r.Role == rootRole {
		as.lg.Error(
			"'root'用户 不能移除 'root' 角色",
			zap.String("user-name", r.Name),
			zap.String("role-name", r.Role),
		)
		return nil, ErrInvalidAuthMgmt
	}

	tx := as.be.BatchTx()
	tx.Lock()
	defer tx.Unlock()

	user := getUser(as.lg, tx, r.Name)
	if user == nil {
		return nil, ErrUserNotFound
	}

	updatedUser := &authpb.User{
		Name:     user.Name,
		Password: user.Password,
		Options:  user.Options,
	}

	for _, role := range user.Roles {
		if role != r.Role {
			updatedUser.Roles = append(updatedUser.Roles, role)
		}
	}

	if len(updatedUser.Roles) == len(user.Roles) {
		return nil, ErrRoleNotGranted
	}

	putUser(as.lg, tx, updatedUser)

	as.invalidateCachedPerm(r.Name)

	as.commitRevision(tx)

	as.lg.Info(
		"移除用户角色",
		zap.String("user-name", r.Name),
		zap.Strings("old-user-roles", user.Roles),
		zap.Strings("new-user-roles", updatedUser.Roles),
		zap.String("revoked-role-name", r.Role),
	)
	return &pb.AuthUserRevokeRoleResponse{}, nil
}

func getUser(lg *zap.Logger, tx backend.BatchTx, username string) *authpb.User {
	_, vs := tx.UnsafeRange(buckets.AuthUsers, []byte(username), nil, 0)
	if len(vs) == 0 {
		return nil
	}

	user := &authpb.User{}
	err := user.Unmarshal(vs[0])
	if err != nil {
		lg.Panic(
			"failed to unmarshal 'authpb.User'",
			zap.String("user-name", username),
			zap.Error(err),
		)
	}
	return user
}

// 获取所有用户
func getAllUsers(lg *zap.Logger, tx backend.BatchTx) []*authpb.User {
	_, vs := tx.UnsafeRange(buckets.AuthUsers, []byte{0}, []byte{0xff}, -1)
	if len(vs) == 0 {
		return nil
	}

	users := make([]*authpb.User, len(vs))
	for i := range vs {
		user := &authpb.User{}
		err := user.Unmarshal(vs[i])
		if err != nil {
			lg.Panic("不能反序列化 'authpb.User'", zap.Error(err))
		}
		users[i] = user
	}
	return users
}

// OK
func putUser(lg *zap.Logger, tx backend.BatchTx, user *authpb.User) {
	b, err := user.Marshal()
	if err != nil {
		lg.Panic("序列化失败 'authpb.User'", zap.Error(err))
	}
	tx.UnsafePut(buckets.AuthUsers, []byte(user.Name), b)
}

func delUser(tx backend.BatchTx, username string) {
	tx.UnsafeDelete(buckets.AuthUsers, []byte(username))
}
