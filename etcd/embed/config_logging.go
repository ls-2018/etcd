// Copyright 2018 The etcd Authors
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

package embed

import (
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"net/url"
	"os"

	"github.com/ls-2018/etcd_cn/client_sdk/pkg/logutil"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zapgrpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/grpclog"
	"gopkg.in/natefinch/lumberjack.v2"
)

// GetLogger
// err := cfg.ZapLoggerBuilder(cfg)
func (cfg Config) GetLogger() *zap.Logger {
	cfg.loggerMu.RLock()
	l := cfg.logger
	cfg.loggerMu.RUnlock()
	return l
}

// setupLogging 初始化etcd日志.必须在标志解析或完成配置embed.Config后调用.
func (cfg *Config) setupLogging() error {
	switch cfg.Logger {
	case "zap":
		if len(cfg.LogOutputs) == 0 {
			cfg.LogOutputs = []string{DefaultLogOutput}
		}
		if len(cfg.LogOutputs) > 1 {
			for _, v := range cfg.LogOutputs {
				if v == DefaultLogOutput {
					return fmt.Errorf("目前还不支持%q的多重日志输出", DefaultLogOutput)
				}
			}
		}
		// todo
		if cfg.EnableLogRotation {
			if err := setupLogRotation(cfg.LogOutputs, cfg.LogRotationConfigJSON); err != nil {
				return err
			}
		}

		outputPaths, errOutputPaths := make([]string, 0), make([]string, 0)
		isJournal := false
		for _, v := range cfg.LogOutputs {
			switch v {
			case DefaultLogOutput:
				outputPaths = append(outputPaths, StdErrLogOutput)
				errOutputPaths = append(errOutputPaths, StdErrLogOutput)

			case JournalLogOutput:
				isJournal = true

			case StdErrLogOutput:
				outputPaths = append(outputPaths, StdErrLogOutput)
				errOutputPaths = append(errOutputPaths, StdErrLogOutput)

			case StdOutLogOutput:
				outputPaths = append(outputPaths, StdOutLogOutput)
				errOutputPaths = append(errOutputPaths, StdOutLogOutput)

			default:
				var path string
				if cfg.EnableLogRotation {
					// append rotate scheme to logs managed by lumberjack log rotation
					if v[0:1] == "/" {
						path = fmt.Sprintf("rotate:/%%2F%s", v[1:])
					} else {
						path = fmt.Sprintf("rotate:/%s", v)
					}
				} else {
					path = v
				}
				outputPaths = append(outputPaths, path)
				errOutputPaths = append(errOutputPaths, path)
			}
		}

		if !isJournal {
			copied := logutil.DefaultZapLoggerConfig
			copied.OutputPaths = outputPaths
			copied.ErrorOutputPaths = errOutputPaths
			copied = logutil.MergeOutputPaths(copied)                                    // /dev/null 判断
			copied.Level = zap.NewAtomicLevelAt(logutil.ConvertToZapLevel(cfg.LogLevel)) // 是一个方便的函数,它创建一个AtomicLevel,然后用给定的级别调用SetLevel.
			if cfg.ZapLoggerBuilder == nil {
				lg, err := copied.Build() // 从配置和选项中构建一个logger
				if err != nil {
					return err
				}
				cfg.ZapLoggerBuilder = NewZapLoggerBuilder(lg)
			}
		} else {
			if len(cfg.LogOutputs) > 1 {
				for _, v := range cfg.LogOutputs {
					if v != DefaultLogOutput {
						return fmt.Errorf("运行systemd/journal,但其他 '--log-outputs' values (%q) 被配置为 'default'; 用其他的值重写 'default'", cfg.LogOutputs)
					}
				}
			}

			// 使用stderr作为后备方案
			syncer, lerr := getJournalWriteSyncer()
			if lerr != nil {
				return lerr
			}

			lvl := zap.NewAtomicLevelAt(logutil.ConvertToZapLevel(cfg.LogLevel))

			// WARN: 不要改变encoder 配置中的字段名 journald日志编写者假定字段名为"level" and "caller"
			cr := zapcore.NewCore(
				zapcore.NewJSONEncoder(logutil.DefaultZapLoggerConfig.EncoderConfig),
				syncer, lvl,
			)
			if cfg.ZapLoggerBuilder == nil {
				cfg.ZapLoggerBuilder = NewZapLoggerBuilder(zap.New(cr, zap.AddCaller(), zap.ErrorOutput(syncer)))
			}
		}

		err := cfg.ZapLoggerBuilder(cfg)
		if err != nil {
			return err
		}

		logTLSHandshakeFailure := func(conn *tls.Conn, err error) {
			// 记录tls握手失败
			state := conn.ConnectionState()
			remoteAddr := conn.RemoteAddr().String()
			serverName := state.ServerName
			if len(state.PeerCertificates) > 0 {
				cert := state.PeerCertificates[0]
				ips := make([]string, len(cert.IPAddresses))
				for i := range cert.IPAddresses {
					ips[i] = cert.IPAddresses[i].String()
				}
				cfg.logger.Warn(
					"拒绝连接",
					zap.String("remote-addr", remoteAddr),
					zap.String("etcd-name", serverName),
					zap.Strings("ip-addresses", ips),
					zap.Strings("dns-names", cert.DNSNames),
					zap.Error(err),
				)
			} else {
				cfg.logger.Warn(
					"拒绝连接",
					zap.String("remote-addr", remoteAddr),
					zap.String("etcd-name", serverName),
					zap.Error(err),
				)
			}
		}
		cfg.ClientTLSInfo.HandshakeFailure = logTLSHandshakeFailure
		cfg.PeerTLSInfo.HandshakeFailure = logTLSHandshakeFailure

	default:
		return fmt.Errorf("未知的Logger选项 %q", cfg.Logger)
	}

	return nil
}

// NewZapLoggerBuilder 生成一个zap logger builder,为embedded  etcd设置给定的loger.
func NewZapLoggerBuilder(lg *zap.Logger) func(*Config) error {
	return func(cfg *Config) error {
		cfg.loggerMu.Lock()
		defer cfg.loggerMu.Unlock()
		cfg.logger = lg
		return nil
	}
}

// SetupGlobalLoggers 配置全loggers (grpc, zapGlobal)基于cfg
// 该方法默认不被embed etcd执行(从3.5开始),以实现grpc/zap.Global日志独立配置或跨越独立生命周期的设置(如测试).
func (cfg *Config) SetupGlobalLoggers() {
	lg := cfg.GetLogger()
	if lg != nil {
		if cfg.LogLevel == "debug" {
			grpc.EnableTracing = true
			grpclog.SetLoggerV2(zapgrpc.NewLogger(lg))
		} else {
			grpclog.SetLoggerV2(grpclog.NewLoggerV2(ioutil.Discard, os.Stderr, os.Stderr))
		}
		zap.ReplaceGlobals(lg)
	}
}

type logRotationConfig struct {
	*lumberjack.Logger
}

// Sync implements zap.Sink
func (logRotationConfig) Sync() error { return nil }

// setupLogRotation 初始化单个文件路径目标的日志旋转.
func setupLogRotation(logOutputs []string, logRotateConfigJSON string) error {
	var logRotationConfig logRotationConfig
	outputFilePaths := 0
	for _, v := range logOutputs {
		switch v {
		case DefaultLogOutput, StdErrLogOutput, StdOutLogOutput:
			continue
		default:
			outputFilePaths++
		}
	}
	// 日志旋转需要文件目标
	if len(logOutputs) == 1 && outputFilePaths == 0 {
		return ErrLogRotationInvalidLogOutput
	}
	// support max 1 file target for log rotation
	if outputFilePaths > 1 {
		return ErrLogRotationInvalidLogOutput
	}

	if err := json.Unmarshal([]byte(logRotateConfigJSON), &logRotationConfig); err != nil {
		var unmarshalTypeError *json.UnmarshalTypeError
		var syntaxError *json.SyntaxError
		switch {
		case errors.As(err, &syntaxError):
			return fmt.Errorf("improperly formatted log rotation config: %w", err)
		case errors.As(err, &unmarshalTypeError):
			return fmt.Errorf("invalid log rotation config: %w", err)
		}
	}
	zap.RegisterSink("rotate", func(u *url.URL) (zap.Sink, error) {
		logRotationConfig.Filename = u.Path[1:]
		return &logRotationConfig, nil
	})
	return nil
}
