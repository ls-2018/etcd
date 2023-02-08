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

package transport

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"errors"
	"fmt"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/ls-2018/etcd_cn/client_sdk/pkg/fileutil"
	"github.com/ls-2018/etcd_cn/client_sdk/pkg/tlsutil"

	"go.uber.org/zap"
)

// NewListener creates a new listner.
func NewListener(addr, scheme string, tlsinfo *TLSInfo) (l net.Listener, err error) {
	return newListener(addr, scheme, WithTLSInfo(tlsinfo))
}

// NewListenerWithOpts OK
func NewListenerWithOpts(addr, scheme string, opts ...ListenerOption) (net.Listener, error) {
	return newListener(addr, scheme, opts...)
}

func newListener(addr, scheme string, opts ...ListenerOption) (net.Listener, error) {
	if scheme == "unix" || scheme == "unixs" {
		// unix sockets via unix://laddr
		return NewUnixListener(addr)
	}

	lnOpts := newListenOpts(opts...)

	switch {
	case lnOpts.IsSocketOpts():
		config, err := newListenConfig(lnOpts.socketOpts)
		if err != nil {
			return nil, err
		}
		lnOpts.ListenConfig = config
		fallthrough
	case lnOpts.IsTimeout(), lnOpts.IsSocketOpts():
		ln, err := lnOpts.ListenConfig.Listen(context.TODO(), "tcp", addr)
		if err != nil {
			return nil, err
		}
		lnOpts.Listener = &rwTimeoutListener{
			Listener:     ln,
			readTimeout:  lnOpts.readTimeout,
			writeTimeout: lnOpts.writeTimeout,
		}
	case lnOpts.IsTimeout():
		ln, err := net.Listen("tcp", addr)
		if err != nil {
			return nil, err
		}
		lnOpts.Listener = &rwTimeoutListener{
			Listener:     ln,
			readTimeout:  lnOpts.readTimeout,
			writeTimeout: lnOpts.writeTimeout,
		}
	default:
		ln, err := net.Listen("tcp", addr)
		if err != nil {
			return nil, err
		}
		lnOpts.Listener = ln
	}

	if lnOpts.skipTLSInfoCheck && !lnOpts.IsTLS() {
		return lnOpts.Listener, nil
	}
	return wrapTLS(scheme, lnOpts.tlsInfo, lnOpts.Listener)
}

func wrapTLS(scheme string, tlsinfo *TLSInfo, l net.Listener) (net.Listener, error) {
	if scheme != "https" && scheme != "unixs" {
		return l, nil
	}
	if tlsinfo != nil && tlsinfo.SkipClientSANVerify {
		return NewTLSListener(l, tlsinfo)
	}
	return newTLSListener(l, tlsinfo, checkSAN)
}

func newListenConfig(sopts *SocketOpts) (net.ListenConfig, error) {
	lc := net.ListenConfig{}
	if sopts != nil {
		ctls := getControls(sopts)
		if len(ctls) > 0 {
			lc.Control = ctls.Control
		}
	}
	return lc, nil
}

type TLSInfo struct {
	// CertFile 服务端证书,如果ClientCertFile为空,它也将被用作_客户证书.
	CertFile string
	// KeyFile 是CertFile的密钥.
	KeyFile string

	// ClientCertFile client 证书,且启用认证;则使用CertFile
	ClientCertFile string
	// ClientKeyFile 是ClientCertFile的密钥
	ClientKeyFile string

	TrustedCAFile       string // ca证书
	ClientCertAuth      bool   // 客户端证书验证;默认false
	CRLFile             string // 证书吊销列表文件的路径
	InsecureSkipVerify  bool
	SkipClientSANVerify bool

	// ServerName 在发现/虚拟主机的情况下,确保证书与给定的主机相匹配
	ServerName string

	// HandshakeFailure  当一个连接无法握手时,会被选择性地调用.之后,连接将被立即关闭.
	HandshakeFailure func(*tls.Conn, error)

	// CipherSuites 是一个支持的密码套件的列表.如果是空的,Go 默认会自动填充它.请注意,密码套件是按照给定的顺序进行优先排序的.
	CipherSuites []uint16

	selfCert bool // 自签

	// parseFunc 的存在是为了简化测试.通常情况下,parseFunc应该留为零.在这种情况下,将使用tls.X509KeyPair.
	parseFunc func([]byte, []byte) (tls.Certificate, error)

	// AllowedCN  客户端必须提供的common Name;在证书里
	AllowedCN string

	// AllowedHostname 是一个IP地址或主机名,必须与客户提供的TLS证书相匹配.
	AllowedHostname string

	Logger *zap.Logger

	// EmptyCN indicates that the cert must have empty CN.
	// If true, ClientConfig() will return an error for a cert with non empty CN.
	EmptyCN bool
}

func (info TLSInfo) String() string {
	return fmt.Sprintf("cert = %s, key = %s, client-cert=%s, client-key=%s, trusted-ca = %s, client-cert-auth = %v, crl-file = %s", info.CertFile, info.KeyFile, info.ClientCertFile, info.ClientKeyFile, info.TrustedCAFile, info.ClientCertAuth, info.CRLFile)
}

func (info TLSInfo) Empty() bool {
	return info.CertFile == "" && info.KeyFile == ""
}

func SelfCert(lg *zap.Logger, dirpath string, hosts []string, selfSignedCertValidity uint, additionalUsages ...x509.ExtKeyUsage) (info TLSInfo, err error) {
	info.Logger = lg
	if selfSignedCertValidity == 0 {
		err = fmt.Errorf("selfSignedCertValidity 是无效的,它应该大于0 ")
		info.Logger.Warn("不能生成证书", zap.Error(err))
		return
	}
	err = fileutil.TouchDirAll(dirpath)
	if err != nil {
		if info.Logger != nil {
			info.Logger.Warn("无法创建证书目录", zap.Error(err))
		}
		return
	}

	certPath, err := filepath.Abs(filepath.Join(dirpath, "cert.pem"))
	if err != nil {
		return
	}
	keyPath, err := filepath.Abs(filepath.Join(dirpath, "key.pem"))
	if err != nil {
		return
	}
	_, errcert := os.Stat(certPath)
	_, errkey := os.Stat(keyPath)
	if errcert == nil && errkey == nil {
		info.CertFile = certPath
		info.KeyFile = keyPath
		info.ClientCertFile = certPath
		info.ClientKeyFile = keyPath
		info.selfCert = true
		return
	}

	// 编号
	serialNumber, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 128))
	if err != nil {
		if info.Logger != nil {
			info.Logger.Warn("无法生成随机数", zap.Error(err))
		}
		return
	}

	tmpl := x509.Certificate{
		SerialNumber: serialNumber,
		Subject:      pkix.Name{Organization: []string{"etcd"}},
		NotBefore:    time.Now(),
		NotAfter:     time.Now().Add(time.Duration(selfSignedCertValidity) * 365 * (24 * time.Hour)),
		// 加密、解密
		KeyUsage: x509.KeyUsageKeyEncipherment | x509.KeyUsageDigitalSignature,
		// 服务端验证
		ExtKeyUsage:           append([]x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}, additionalUsages...),
		BasicConstraintsValid: true,
		IPAddresses:           []net.IP{},
		DNSNames:              []string{},
	}

	if info.Logger != nil {
		info.Logger.Warn("自动生成证书", zap.Time("certificate-validity-bound-not-after", tmpl.NotAfter))
	}

	for _, host := range hosts {
		h, _, _ := net.SplitHostPort(host)
		if ip := net.ParseIP(h); ip != nil {
			tmpl.IPAddresses = append(tmpl.IPAddresses, ip)
		} else {
			tmpl.DNSNames = append(tmpl.DNSNames, h)
		}
	}

	priv, err := ecdsa.GenerateKey(elliptic.P521(), rand.Reader)
	if err != nil {
		if info.Logger != nil {
			info.Logger.Warn("不能生成ECDSA密钥", zap.Error(err))
		}
		return
	}

	derBytes, err := x509.CreateCertificate(rand.Reader, &tmpl, &tmpl, &priv.PublicKey, priv)
	if err != nil {
		if info.Logger != nil {
			info.Logger.Warn("无法生成x509证书", zap.Error(err))
		}
		return
	}

	certOut, err := os.Create(certPath)
	if err != nil {
		info.Logger.Warn("无法创建证书文件", zap.String("path", certPath), zap.Error(err))
		return
	}
	// 证书文件
	pem.Encode(certOut, &pem.Block{Type: "CERTIFICATE", Bytes: derBytes})
	certOut.Close()
	if info.Logger != nil {
		info.Logger.Info("创建的Cert文件", zap.String("path", certPath))
	}

	b, err := x509.MarshalECPrivateKey(priv)
	if err != nil {
		return
	}
	keyOut, err := os.OpenFile(keyPath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o600)
	if err != nil {
		if info.Logger != nil {
			info.Logger.Warn("无法创建私钥文件", zap.String("path", keyPath), zap.Error(err))
		}
		return
	}
	// 秘钥
	pem.Encode(keyOut, &pem.Block{Type: "EC PRIVATE KEY", Bytes: b})
	keyOut.Close()
	if info.Logger != nil {
		info.Logger.Info("创建的私钥文件", zap.String("path", keyPath))
	}
	return SelfCert(lg, dirpath, hosts, selfSignedCertValidity)
}

// OK
func (info TLSInfo) baseConfig() (*tls.Config, error) {
	if info.KeyFile == "" || info.CertFile == "" {
		return nil, fmt.Errorf("KeyFile和CertFile必须同时存在[key: %v, cert: %v]", info.KeyFile, info.CertFile)
	}
	if info.Logger == nil {
		info.Logger = zap.NewNop()
	}

	_, err := tlsutil.NewCert(info.CertFile, info.KeyFile, info.parseFunc) // parseFunc 在主程序里是nil
	if err != nil {
		return nil, err
	}

	// 如果提供了客户证书和密钥,则对其进行预验证.这可以确保我们在接受任何连接之前崩溃.
	if (info.ClientKeyFile == "") != (info.ClientCertFile == "") {
		return nil, fmt.Errorf("ClientKeyFile和ClientCertFile必须同时存在或同时不存在.: key: %v, cert: %v]", info.ClientKeyFile, info.ClientCertFile)
	}
	if info.ClientCertFile != "" {
		_, err := tlsutil.NewCert(info.ClientCertFile, info.ClientKeyFile, info.parseFunc)
		if err != nil {
			return nil, err
		}
	}

	cfg := &tls.Config{
		MinVersion: tls.VersionTLS12,
		ServerName: info.ServerName,
	}

	if len(info.CipherSuites) > 0 {
		cfg.CipherSuites = info.CipherSuites
	}

	// 客户端证书可以通过CN上的精确匹配来验证,也可以通过对CN和san进行更一般的检查来验证.
	var verifyCertificate func(*x509.Certificate) bool
	if info.AllowedCN != "" {
		if info.AllowedHostname != "" {
			return nil, fmt.Errorf("AllowedCN and AllowedHostname 只能指定一个 (cn=%q, hostname=%q)", info.AllowedCN, info.AllowedHostname)
		}
		verifyCertificate = func(cert *x509.Certificate) bool {
			return info.AllowedCN == cert.Subject.CommonName
		}
	}
	if info.AllowedHostname != "" {
		verifyCertificate = func(cert *x509.Certificate) bool {
			return cert.VerifyHostname(info.AllowedHostname) == nil
		}
	}
	if verifyCertificate != nil {
		cfg.VerifyPeerCertificate = func(rawCerts [][]byte, verifiedChains [][]*x509.Certificate) error {
			for _, chains := range verifiedChains {
				if len(chains) != 0 {
					if verifyCertificate(chains[0]) {
						return nil
					}
				}
			}
			return errors.New("客户端证书认证失败")
		}
	}
	// 是有同一个CA签发的
	// 服务端获取证书
	cfg.GetCertificate = func(clientHello *tls.ClientHelloInfo) (cert *tls.Certificate, err error) {
		cert, err = tlsutil.NewCert(info.CertFile, info.KeyFile, info.parseFunc)
		if os.IsNotExist(err) {
			if info.Logger != nil {
				info.Logger.Warn(
					"未能找到peer的证书文件",
					zap.String("cert-file", info.CertFile),
					zap.String("key-file", info.KeyFile),
					zap.Error(err),
				)
			}
		} else if err != nil {
			if info.Logger != nil {
				info.Logger.Warn(
					"未能创建peer证书",
					zap.String("cert-file", info.CertFile),
					zap.String("key-file", info.KeyFile),
					zap.Error(err),
				)
			}
		}
		return cert, err
	}
	// 客户端获取证书
	cfg.GetClientCertificate = func(unused *tls.CertificateRequestInfo) (cert *tls.Certificate, err error) {
		certfile, keyfile := info.CertFile, info.KeyFile
		if info.ClientCertFile != "" {
			certfile, keyfile = info.ClientCertFile, info.ClientKeyFile
		}
		cert, err = tlsutil.NewCert(certfile, keyfile, info.parseFunc)
		if os.IsNotExist(err) {
			if info.Logger != nil {
				info.Logger.Warn(
					"未能找到peer的证书文件",
					zap.String("cert-file", certfile),
					zap.String("key-file", keyfile),
					zap.Error(err),
				)
			}
		} else if err != nil {
			if info.Logger != nil {
				info.Logger.Warn(
					"未能创建peer证书",
					zap.String("cert-file", certfile),
					zap.String("key-file", keyfile),
					zap.Error(err),
				)
			}
		}
		return cert, err
	}
	return cfg, nil
}

// OK
func (info TLSInfo) cafiles() []string {
	cs := make([]string, 0)
	if info.TrustedCAFile != "" {
		cs = append(cs, info.TrustedCAFile)
	}
	return cs
}

// ServerConfig generates a tls.Config object for use by an HTTP etcd.
func (info TLSInfo) ServerConfig() (*tls.Config, error) {
	cfg, err := info.baseConfig()
	if err != nil {
		return nil, err
	}

	if info.Logger == nil {
		info.Logger = zap.NewNop()
	}

	cfg.ClientAuth = tls.NoClientCert
	if info.TrustedCAFile != "" || info.ClientCertAuth {
		cfg.ClientAuth = tls.RequireAndVerifyClientCert
	}

	cs := info.cafiles()
	if len(cs) > 0 {
		info.Logger.Info("Loading cert pool", zap.Strings("cs", cs),
			zap.Any("tlsinfo", info))
		cp, err := tlsutil.NewCertPool(cs)
		if err != nil {
			return nil, err
		}
		cfg.ClientCAs = cp
	}

	// "h2" NextProtos is necessary for enabling HTTP2 for go's HTTP etcd
	cfg.NextProtos = []string{"h2"}

	// go1.13 enables TLS 1.3 by default
	// and in TLS 1.3, cipher suites are not configurable
	// setting Max TLS version to TLS 1.2 for go 1.13
	cfg.MaxVersion = tls.VersionTLS12

	return cfg, nil
}

// ClientConfig 生成一个tls.Config对象,供HTTP客户端使用.
func (info TLSInfo) ClientConfig() (*tls.Config, error) {
	var cfg *tls.Config
	var err error

	if !info.Empty() {
		cfg, err = info.baseConfig() // // 初始化TLS配置
		if err != nil {
			return nil, err
		}
	} else {
		cfg = &tls.Config{ServerName: info.ServerName}
	}
	cfg.InsecureSkipVerify = info.InsecureSkipVerify // 客户端是否验证服务端证书链和主机名

	cs := info.cafiles()
	if len(cs) > 0 {
		cfg.RootCAs, err = tlsutil.NewCertPool(cs)
		if err != nil {
			return nil, err
		}
	}

	if info.selfCert {
		cfg.InsecureSkipVerify = true
	}

	if info.EmptyCN {
		hasNonEmptyCN := false
		cn := ""
		_, err := tlsutil.NewCert(info.CertFile, info.KeyFile, func(certPEMBlock []byte, keyPEMBlock []byte) (tls.Certificate, error) {
			var block *pem.Block
			block, _ = pem.Decode(certPEMBlock)
			cert, err := x509.ParseCertificate(block.Bytes)
			if err != nil {
				return tls.Certificate{}, err
			}
			if len(cert.Subject.CommonName) != 0 {
				hasNonEmptyCN = true
				cn = cert.Subject.CommonName
			}
			return tls.X509KeyPair(certPEMBlock, keyPEMBlock)
		})
		if err != nil {
			return nil, err
		}
		if hasNonEmptyCN {
			return nil, fmt.Errorf("证书没有CN(%s): %s", cn, info.CertFile)
		}
	}

	cfg.MaxVersion = tls.VersionTLS12

	return cfg, nil
}

// IsClosedConnError returns true if the error is from closing listener, cmux.
// copied from golang.org/x/net/http2/http2.go
func IsClosedConnError(err error) bool {
	// 'use of closed network connection' (Go <=1.8)
	// 'use of closed file or network connection' (Go >1.8, internal/poll.ErrClosing)
	// 'mux: listener closed' (cmux.ErrListenerClosed)
	return err != nil && strings.Contains(err.Error(), "closed")
}
