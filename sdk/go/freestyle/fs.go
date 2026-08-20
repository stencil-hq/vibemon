package freestyle

import (
	"context"
	"fmt"
	"time"

	vmon "github.com/stencil-hq/vibemon/sdk/go"
)

// VmFs provides guest filesystem operations for one VM.
type VmFs struct{ sandbox *vmon.Sandbox }

// ReadFile reads raw guest file bytes.
func (fs *VmFs) ReadFile(ctx context.Context, path string) ([]byte, error) {
	return fs.sandbox.Files.Read(ctx, path)
}

// WriteFile writes raw guest file bytes.
func (fs *VmFs) WriteFile(ctx context.Context, path string, content []byte) error {
	return fs.sandbox.Files.Write(ctx, path, content)
}

// ReadTextFile reads a UTF-8 guest file.
func (fs *VmFs) ReadTextFile(ctx context.Context, path string) (string, error) {
	content, err := fs.ReadFile(ctx, path)
	if err != nil {
		return "", err
	}
	return string(content), nil
}

// WriteTextFile writes UTF-8 guest file content.
func (fs *VmFs) WriteTextFile(ctx context.Context, path, content string) error {
	return fs.WriteFile(ctx, path, []byte(content))
}

// DirEntry is one Freestyle-shaped directory entry.
type DirEntry struct {
	Name string
	Kind string
}

// ReadDir lists guest directory entries.
func (fs *VmFs) ReadDir(ctx context.Context, path string) ([]DirEntry, error) {
	entries, err := fs.sandbox.Files.List(ctx, path)
	if err != nil {
		return nil, err
	}
	result := make([]DirEntry, 0, len(entries))
	for _, entry := range entries {
		kind := entry.Type
		if kind == "" {
			kind = "other"
		}
		result = append(result, DirEntry{Name: entry.Name, Kind: kind})
	}
	return result, nil
}

// Mkdir creates a guest directory and any missing parents.
func (fs *VmFs) Mkdir(ctx context.Context, path string) error {
	return fs.sandbox.Files.Mkdir(ctx, path)
}

// RemoveOptions configures deletion of a guest path.
type RemoveOptions struct{ Recursive bool }

// Remove deletes a guest path.
func (fs *VmFs) Remove(ctx context.Context, path string, options *RemoveOptions) error {
	var remove vmon.DeleteOptions
	if options != nil {
		remove.Recursive = options.Recursive
	}
	return fs.sandbox.Files.Delete(ctx, path, remove)
}

// Exists reports false only when the daemon reports not_found.
func (fs *VmFs) Exists(ctx context.Context, path string) (bool, error) {
	_, err := fs.sandbox.Files.Stat(ctx, path)
	if err == nil {
		return true, nil
	}
	if isNotFound(err) {
		return false, nil
	}
	return false, err
}

// FileStat is Freestyle-shaped guest path metadata.
type FileStat struct {
	Size        uint64
	IsFile      bool
	IsDirectory bool
	IsSymlink   bool
	Permissions string
	Modified    time.Time
}

// Stat retrieves guest path metadata. vmon does not report owner or group.
func (fs *VmFs) Stat(ctx context.Context, path string) (FileStat, error) {
	info, err := fs.sandbox.Files.Stat(ctx, path)
	if err != nil {
		return FileStat{}, err
	}
	return FileStat{
		Size:        info.Size,
		IsFile:      info.Type == "file",
		IsDirectory: info.Type == "dir",
		IsSymlink:   info.Type == "symlink",
		Permissions: fmt.Sprintf("%03o", info.Mode&0o7777),
		Modified:    time.Unix(info.ModTime, 0).UTC(),
	}, nil
}
