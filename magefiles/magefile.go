//go:build mage

package main

import (
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"

	"github.com/magefile/mage/sh"
)

// Gen generates or updates code that is meant to be committed to the repository.
func Gen() error {
	if err := deleteGenFiles("./pb"); err != nil {
		return err
	}
	files, err := getProtoFiles("./proto")
	if err != nil {
		return err
	}

	options := []string{
		"--proto_path=./proto",
		"--go_out=./pb", "--go_opt=paths=source_relative",
		"--go-drpc_out=./pb", "--go-drpc_opt=paths=source_relative",
	}
	options = append(options, files...)
	if out, err := sh.Output("protoc", options...); err != nil {
		return fmt.Errorf("%w\n%s", err, strings.TrimSpace(out))
	}
	return nil
}

func getProtoFiles(dir string) ([]string, error) {
	var files []string
	if err := fs.WalkDir(os.DirFS(dir), ".", func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}
		if !strings.HasSuffix(d.Name(), ".proto") {
			return nil
		}

		files = append(files, filepath.Join(dir, path))
		return nil
	}); err != nil {
		return nil, err
	}

	return files, nil
}

func deleteGenFiles(dir string) error {
	return fs.WalkDir(os.DirFS(dir), ".", func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}
		if !strings.HasSuffix(d.Name(), ".pb.go") {
			return nil
		}

		return os.Remove(filepath.Join(dir, path))
	})
}
