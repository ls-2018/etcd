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

package fileutil

import (
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"

	"go.uber.org/zap"
)

const (
	// PrivateFileMode 授予所有者读/写文件的权限.
	PrivateFileMode = 0o600
)

// IsDirWriteable checks if dir is writable by writing and removing a file
// to dir. It returns nil if dir is writable.
func IsDirWriteable(dir string) error {
	f, err := filepath.Abs(filepath.Join(dir, ".touch"))
	if err != nil {
		return err
	}
	if err := ioutil.WriteFile(f, []byte(""), PrivateFileMode); err != nil {
		return err
	}
	return os.Remove(f)
}

// TouchDirAll 与os.MkdirAll类似.如果任何目录不存在,它就用0700权限创建目录.TouchDirAll也确保给定的目录是可写的.
func TouchDirAll(dir string) error {
	// 如果路径已经是一个目录,MkdirAll不做任何事情,并返回nil,所以,首先检查dir是否存在,并有预期的权限模式.
	if Exist(dir) {
		err := CheckDirPermission(dir, PrivateDirMode)
		if err != nil {
			lg, _ := zap.NewProduction()
			if lg == nil {
				lg = zap.NewExample()
			}
			lg.Warn("check file permission", zap.Error(err))
		}
	} else {
		err := os.MkdirAll(dir, PrivateDirMode)
		if err != nil {
			// if mkdirAll("a/text") and "text" is not
			// a directory, this will return syscall.ENOTDIR
			return err
		}
	}

	return IsDirWriteable(dir)
}

// CreateDirAll is similar to TouchDirAll but returns error
// if the deepest directory was not empty.
func CreateDirAll(dir string) error {
	err := TouchDirAll(dir)
	if err == nil {
		var ns []string
		ns, err = ReadDir(dir)
		if err != nil {
			return err
		}
		if len(ns) != 0 {
			err = fmt.Errorf("expected %q to be empty, got %q", dir, ns)
		}
	}
	return err
}

// Exist 返回文件或目录是否存在
func Exist(name string) bool {
	_, err := os.Stat(name)
	return err == nil
}

// DirEmpty 返回文件是否创建,以及是一个空目录
func DirEmpty(name string) bool {
	ns, err := ReadDir(name)
	return len(ns) == 0 && err == nil
}

// ZeroToEnd 清空当前之后的数据,并固定分配文件空间
func ZeroToEnd(f *os.File) error {
	// offset是从0开始的, 可以比当前的文件内容长度大,多出的部分会用空(0)来代替
	off, err := f.Seek(0, io.SeekCurrent) // 返回当前的偏移量(相对开头)
	if err != nil {
		return err
	}
	lenf, lerr := f.Seek(0, io.SeekEnd) // 返回 文件大小
	if lerr != nil {
		return lerr
	}
	// 删除后面的内容,不管当前的偏移量在哪儿,都是从头开始截取不会影响当前的偏移量;改变文件的大小
	if err = f.Truncate(off); err != nil {
		return err
	}
	if err = Preallocate(f, lenf, true); err != nil {
		return err
	} // 预分配空间
	_, err = f.Seek(off, io.SeekStart) // 跳转到 要接着写的地方
	return err
}

// CheckDirPermission checks permission on an existing dir.
// Returns error if dir is empty or exist with a different permission than specified.
func CheckDirPermission(dir string, perm os.FileMode) error {
	if !Exist(dir) {
		return fmt.Errorf("directory %q empty, cannot check permission", dir)
	}
	// check the existing permission on the directory
	dirInfo, err := os.Stat(dir)
	if err != nil {
		return err
	}
	dirMode := dirInfo.Mode().Perm()
	if dirMode != perm {
		err = fmt.Errorf("directory %q exist, but the permission is %q. The recommended permission is %q to prevent possible unprivileged access to the data", dir, dirInfo.Mode(), os.FileMode(PrivateDirMode))
		return err
	}
	return nil
}

// RemoveMatchFile 移除格式匹配的文件
func RemoveMatchFile(lg *zap.Logger, dir string, matchFunc func(fileName string) bool) error {
	if lg == nil {
		lg = zap.NewNop()
	}
	if !Exist(dir) {
		return fmt.Errorf("目录不存在 %s", dir)
	}
	fileNames, err := ReadDir(dir)
	if err != nil {
		return err
	}
	var removeFailedFiles []string
	for _, fileName := range fileNames {
		if matchFunc(fileName) {
			file := filepath.Join(dir, fileName)
			if err = os.Remove(file); err != nil {
				removeFailedFiles = append(removeFailedFiles, fileName)
				lg.Error("删除文件失败",
					zap.String("file", file),
					zap.Error(err))
				continue
			}
		}
	}
	if len(removeFailedFiles) != 0 {
		return fmt.Errorf("删除文件(s) %v error", removeFailedFiles)
	}
	return nil
}
