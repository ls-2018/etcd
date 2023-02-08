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

// Package flags implements command-line flag parsing.
package flags

import (
	"flag"
	"fmt"
	"os"
	"strings"

	"github.com/spf13/pflag"
	"go.uber.org/zap"
)

// SetFlagsFromEnv

// 环境变量采用flag的名称,但为大写字母,有给定的前缀,任何破折号都由下划线代替 - 例如:Some-flag => ETCD_SOME_FLAG
func SetFlagsFromEnv(lg *zap.Logger, prefix string, fs *flag.FlagSet) error {
	var err error
	alreadySet := make(map[string]bool)
	fs.Visit(func(f *flag.Flag) {
		alreadySet[FlagToEnv(prefix, f.Name)] = true
	})
	usedEnvKey := make(map[string]bool)
	fs.VisitAll(func(f *flag.Flag) {
		if serr := setFlagFromEnv(lg, fs, prefix, f.Name, usedEnvKey, alreadySet, true); serr != nil {
			err = serr
		}
	})
	// usedEnvKey 环境变量中有值,但是命令行没有设置的 并将其设置到了flagSet
	verifyEnv(lg, prefix, usedEnvKey, alreadySet)
	return err
}

// SetPflagsFromEnv is similar to SetFlagsFromEnv. However, the accepted flagset type is pflag.FlagSet
// and it does not do any logging.
func SetPflagsFromEnv(lg *zap.Logger, prefix string, fs *pflag.FlagSet) error {
	var err error
	alreadySet := make(map[string]bool)
	usedEnvKey := make(map[string]bool)
	fs.VisitAll(func(f *pflag.Flag) {
		if f.Changed {
			alreadySet[FlagToEnv(prefix, f.Name)] = true
		}
		if serr := setFlagFromEnv(lg, fs, prefix, f.Name, usedEnvKey, alreadySet, false); serr != nil {
			err = serr
		}
	})
	verifyEnv(lg, prefix, usedEnvKey, alreadySet)
	return err
}

// FlagToEnv 将标志字符串转换为大写的环境变量密钥字符串.
func FlagToEnv(prefix, name string) string {
	return prefix + "_" + strings.ToUpper(strings.Replace(name, "-", "_", -1))
}

func verifyEnv(lg *zap.Logger, prefix string, usedEnvKey, alreadySet map[string]bool) {
	for _, env := range os.Environ() {
		kv := strings.SplitN(env, "=", 2)
		if len(kv) != 2 {
			if lg != nil {
				lg.Warn("发现无效的环境变量", zap.String("environment-variable", env))
			}
		}
		if usedEnvKey[kv[0]] {
			continue
		}
		if alreadySet[kv[0]] {
			if lg != nil {
				lg.Fatal(
					"冲突的环境变量被相应的命令行标志所掩盖(取消环境变量或禁用标志)", zap.String("environment-variable", kv[0]),
				)
			}
		}
		if strings.HasPrefix(env, prefix+"_") {
			if lg != nil {
				lg.Warn("没有注册的环境变量", zap.String("environment-variable", env))
			}
		}
	}
}

type flagSetter interface {
	Set(fk string, fv string) error
}

func setFlagFromEnv(lg *zap.Logger, fs flagSetter, prefix, fname string, usedEnvKey, alreadySet map[string]bool, log bool) error {
	key := FlagToEnv(prefix, fname)
	if !alreadySet[key] {
		val := os.Getenv(key)
		if val != "" {
			usedEnvKey[key] = true
			if serr := fs.Set(fname, val); serr != nil {
				return fmt.Errorf("无效的值 %q for %s: %v", val, key, serr)
			}
			if log && lg != nil {
				lg.Info("确认和使用的环境变量", zap.String("variable-name", key), zap.String("variable-value", val))
			}
		}
	}
	return nil
}

func IsSet(fs *flag.FlagSet, name string) bool {
	set := false
	fs.Visit(func(f *flag.Flag) {
		if f.Name == name {
			set = true
		}
	})
	return set
}
