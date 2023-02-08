package etcdserverpb

import (
	"encoding/json"
	"errors"
	"strings"

	"github.com/ls-2018/etcd_cn/offical/api/v3/membershippb"
)

type xx struct {
	Key         string
	Value       string
	Lease       int64
	PrevKv      bool
	IgnoreValue bool
	IgnoreLease bool
}
type ASD struct {
	Put                      *xx
	Header                   *RequestHeader                            `protobuf:"bytes,100,opt,name=header,proto3" json:"header,omitempty"`
	ID                       uint64                                    `protobuf:"varint,1,opt,name=ID,proto3" json:"ID,omitempty"`
	V2                       *Request                                  `protobuf:"bytes,2,opt,name=v2,proto3" json:"v2,omitempty"`
	Range                    *RangeRequest                             `protobuf:"bytes,3,opt,name=range,proto3" json:"range,omitempty"`
	DeleteRange              *DeleteRangeRequest                       `protobuf:"bytes,5,opt,name=delete_range,json=deleteRange,proto3" json:"delete_range,omitempty"`
	Txn                      *TxnRequest                               `protobuf:"bytes,6,opt,name=txn,proto3" json:"txn,omitempty"`
	Compaction               *CompactionRequest                        `protobuf:"bytes,7,opt,name=compaction,proto3" json:"compaction,omitempty"`
	LeaseGrant               *LeaseGrantRequest                        `protobuf:"bytes,8,opt,name=lease_grant,json=leaseGrant,proto3" json:"lease_grant,omitempty"`
	LeaseRevoke              *LeaseRevokeRequest                       `protobuf:"bytes,9,opt,name=lease_revoke,json=leaseRevoke,proto3" json:"lease_revoke,omitempty"`
	Alarm                    *AlarmRequest                             `protobuf:"bytes,10,opt,name=alarm,proto3" json:"alarm,omitempty"`
	LeaseCheckpoint          *LeaseCheckpointRequest                   `protobuf:"bytes,11,opt,name=lease_checkpoint,json=leaseCheckpoint,proto3" json:"lease_checkpoint,omitempty"`
	AuthEnable               *AuthEnableRequest                        `protobuf:"bytes,1000,opt,name=auth_enable,json=authEnable,proto3" json:"auth_enable,omitempty"`
	AuthDisable              *AuthDisableRequest                       `protobuf:"bytes,1011,opt,name=auth_disable,json=authDisable,proto3" json:"auth_disable,omitempty"`
	AuthStatus               *AuthStatusRequest                        `protobuf:"bytes,1013,opt,name=auth_status,json=authStatus,proto3" json:"auth_status,omitempty"`
	Authenticate             *InternalAuthenticateRequest              `protobuf:"bytes,1012,opt,name=authenticate,proto3" json:"authenticate,omitempty"`
	AuthUserAdd              *AuthUserAddRequest                       `protobuf:"bytes,1100,opt,name=auth_user_add,json=authUserAdd,proto3" json:"auth_user_add,omitempty"`
	AuthUserDelete           *AuthUserDeleteRequest                    `protobuf:"bytes,1101,opt,name=auth_user_delete,json=authUserDelete,proto3" json:"auth_user_delete,omitempty"`
	AuthUserGet              *AuthUserGetRequest                       `protobuf:"bytes,1102,opt,name=auth_user_get,json=authUserGet,proto3" json:"auth_user_get,omitempty"`
	AuthUserChangePassword   *AuthUserChangePasswordRequest            `protobuf:"bytes,1103,opt,name=auth_user_change_password,json=authUserChangePassword,proto3" json:"auth_user_change_password,omitempty"`
	AuthUserGrantRole        *AuthUserGrantRoleRequest                 `protobuf:"bytes,1104,opt,name=auth_user_grant_role,json=authUserGrantRole,proto3" json:"auth_user_grant_role,omitempty"`
	AuthUserRevokeRole       *AuthUserRevokeRoleRequest                `protobuf:"bytes,1105,opt,name=auth_user_revoke_role,json=authUserRevokeRole,proto3" json:"auth_user_revoke_role,omitempty"`
	AuthUserList             *AuthUserListRequest                      `protobuf:"bytes,1106,opt,name=auth_user_list,json=authUserList,proto3" json:"auth_user_list,omitempty"`
	AuthRoleList             *AuthRoleListRequest                      `protobuf:"bytes,1107,opt,name=auth_role_list,json=authRoleList,proto3" json:"auth_role_list,omitempty"`
	AuthRoleAdd              *AuthRoleAddRequest                       `protobuf:"bytes,1200,opt,name=auth_role_add,json=authRoleAdd,proto3" json:"auth_role_add,omitempty"`
	AuthRoleDelete           *AuthRoleDeleteRequest                    `protobuf:"bytes,1201,opt,name=auth_role_delete,json=authRoleDelete,proto3" json:"auth_role_delete,omitempty"`
	AuthRoleGet              *AuthRoleGetRequest                       `protobuf:"bytes,1202,opt,name=auth_role_get,json=authRoleGet,proto3" json:"auth_role_get,omitempty"`
	AuthRoleGrantPermission  *AuthRoleGrantPermissionRequest           `protobuf:"bytes,1203,opt,name=auth_role_grant_permission,json=authRoleGrantPermission,proto3" json:"auth_role_grant_permission,omitempty"`
	AuthRoleRevokePermission *AuthRoleRevokePermissionRequest          `protobuf:"bytes,1204,opt,name=auth_role_revoke_permission,json=authRoleRevokePermission,proto3" json:"auth_role_revoke_permission,omitempty"`
	ClusterVersionSet        *membershippb.ClusterVersionSetRequest    `protobuf:"bytes,1300,opt,name=cluster_version_set,json=clusterVersionSet,proto3" json:"cluster_version_set,omitempty"`
	ClusterMemberAttrSet     *membershippb.ClusterMemberAttrSetRequest `protobuf:"bytes,1301,opt,name=cluster_member_attr_set,json=clusterMemberAttrSet,proto3" json:"cluster_member_attr_set,omitempty"`
	DowngradeInfoSet         *membershippb.DowngradeInfoSetRequest     `protobuf:"bytes,1302,opt,name=downgrade_info_set,json=downgradeInfoSet,proto3" json:"downgrade_info_set,omitempty"`
	XXX_NoUnkeyedLiteral     struct{}                                  `json:"-"`
	XXX_unrecognized         []byte                                    `json:"-"`
	XXX_sizecache            int32                                     `json:"-"`
}

func (m *InternalRaftRequest) Marshal() (dAtA []byte, err error) {
	_ = m.Unmarshal
	a := ASD{
		Put:                      nil,
		Header:                   m.Header,
		ID:                       m.ID,
		V2:                       m.V2,
		Txn:                      m.Txn,
		DeleteRange:              m.DeleteRange,
		AuthRoleRevokePermission: m.AuthRoleRevokePermission,
		AuthRoleGet:              m.AuthRoleGet,
		AuthRoleDelete:           m.AuthRoleDelete,
		AuthUserList:             m.AuthUserList,
		AuthUserChangePassword:   m.AuthUserChangePassword,
		AuthStatus:               m.AuthStatus,
		LeaseCheckpoint:          m.LeaseCheckpoint,
		Alarm:                    m.Alarm,
		AuthDisable:              m.AuthDisable,
		LeaseRevoke:              m.LeaseRevoke,
		AuthEnable:               m.AuthEnable,
		AuthUserDelete:           m.AuthUserDelete,
		Authenticate:             m.Authenticate,
		AuthUserGet:              m.AuthUserGet,
		AuthRoleGrantPermission:  m.AuthRoleGrantPermission,
		AuthUserRevokeRole:       m.AuthUserRevokeRole,
		LeaseGrant:               m.LeaseGrant,
		Compaction:               m.Compaction,
		AuthRoleList:             m.AuthRoleList,
		AuthRoleAdd:              m.AuthRoleAdd,
		AuthUserGrantRole:        m.AuthUserGrantRole,
		AuthUserAdd:              m.AuthUserAdd,
		ClusterVersionSet:        m.ClusterVersionSet,
		ClusterMemberAttrSet:     m.ClusterMemberAttrSet,
		DowngradeInfoSet:         m.DowngradeInfoSet,
	}

	if m.Put != nil {
		a.Put = &xx{
			Key:         m.Put.Key,
			Value:       m.Put.Value,
			Lease:       m.Put.Lease,
			PrevKv:      m.Put.PrevKv,
			IgnoreValue: m.Put.IgnoreValue,
			IgnoreLease: m.Put.IgnoreLease,
		}
	}

	return json.Marshal(a)
}

func (m *InternalRaftRequest) Unmarshal(dAtA []byte) error {
	// a := `{"header":{"ID":7587861231285799685},"put":{"key":"YQ==","value":"Yg=="}}`
	//	b := `{"ID":7587861231285799684,"Method":"PUT","Path":"/0/version","Val":"3.5.0","Dir":false,"PrevValue":"","PrevIndex":0,"Expiration":0,"Wait":false,"Since":0,"Recursive":false,"Sorted":false,"Quorum":false,"Time":0,"Stream":false}`
	//	fmt.Println(json.Unmarshal([]byte(a), &etcdserverpb.InternalRaftRequest{}))   // 不能反序列化成功
	//	fmt.Println(json.Unmarshal([]byte(b), &etcdserverpb.InternalRaftRequest{}))

	if strings.Contains(string(dAtA), "Method") {
		return errors.New("特殊需求,不是使其反序列化成功")
	}
	a := ASD{}
	err := json.Unmarshal(dAtA, &a)
	if a.Put != nil {
		m.Put = &PutRequest{
			Key:         a.Put.Key,
			Value:       a.Put.Value,
			Lease:       a.Put.Lease,
			PrevKv:      a.Put.PrevKv,
			IgnoreValue: a.Put.IgnoreValue,
			IgnoreLease: a.Put.IgnoreLease,
		}
	}
	m.Header = a.Header
	m.ID = a.ID
	m.Txn = a.Txn
	m.V2 = a.V2
	m.DeleteRange = a.DeleteRange
	m.AuthRoleRevokePermission = a.AuthRoleRevokePermission
	m.AuthRoleGet = a.AuthRoleGet
	m.AuthRoleDelete = a.AuthRoleDelete
	m.AuthUserList = a.AuthUserList
	m.AuthUserChangePassword = a.AuthUserChangePassword
	m.AuthStatus = a.AuthStatus
	m.LeaseCheckpoint = a.LeaseCheckpoint
	m.Alarm = a.Alarm
	m.AuthDisable = a.AuthDisable
	m.LeaseRevoke = a.LeaseRevoke
	m.AuthEnable = a.AuthEnable
	m.AuthUserDelete = a.AuthUserDelete
	m.AuthRoleGrantPermission = a.AuthRoleGrantPermission
	m.Authenticate = a.Authenticate
	m.AuthUserGet = a.AuthUserGet
	m.AuthUserRevokeRole = a.AuthUserRevokeRole
	m.LeaseGrant = a.LeaseGrant
	m.Compaction = a.Compaction
	m.AuthRoleList = a.AuthRoleList
	m.AuthRoleAdd = a.AuthRoleAdd
	m.AuthUserGrantRole = a.AuthUserGrantRole
	m.AuthUserAdd = a.AuthUserAdd
	m.ClusterVersionSet = a.ClusterVersionSet
	m.ClusterMemberAttrSet = a.ClusterMemberAttrSet
	m.DowngradeInfoSet = a.DowngradeInfoSet
	return err
}

// 不能更改
