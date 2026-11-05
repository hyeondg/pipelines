package pipelines

import (
	"fmt"
	"io"
	"os"
)

// CopyFile copies the contents of src to dst. Returns an error on failure.
func CopyFile(src string, dst string) error {
	srcFile, err := os.Open(src)
	if err != nil {
		return fmt.Errorf("CopyFile: opening %s: %w", src, err)
	}
	defer func() {
		_ = srcFile.Close()
	}()

	dstFile, err := os.Create(dst)
	if err != nil {
		return fmt.Errorf("CopyFile: creating %s: %w", dst, err)
	}
	defer func() {
		_ = dstFile.Close()
	}()

	_, err = io.Copy(dstFile, srcFile)
	if err != nil {
		return fmt.Errorf("CopyFile: copying %s -> %s: %w", src, dst, err)
	}

	err = dstFile.Sync()
	if err != nil {
		return fmt.Errorf("CopyFile: syncing %s: %w", dst, err)
	}
	return nil
}
