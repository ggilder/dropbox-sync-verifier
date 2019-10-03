package main

import (
	"container/heap"
	"crypto/sha256"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
)

type LocalDirectory struct {
	localRoot       string
	skipContentHash bool
	workerCount     int
	subdirectories  []string
}

func NewLocalDirectory(localRoot string, subdirectories []string, skipContentHash bool, workerCount int) *LocalDirectory {
	inst := LocalDirectory{
		localRoot:       localRoot,
		skipContentHash: skipContentHash,
		workerCount:     workerCount,
		subdirectories:  subdirectories,
	}
	return &inst
}

func (d *LocalDirectory) Manifest(updateChan chan<- *scanProgressUpdate) (manifest *FileHeap, errored []*FileError, err error) {
	contentHash := !d.skipContentHash
	localRootLowercase := strings.ToLower(d.localRoot)
	manifest = &FileHeap{}
	heap.Init(manifest)
	processChan := make(chan string)
	resultChan := make(chan *File)
	errorChan := make(chan *FileError)
	var wg sync.WaitGroup

	for i := 0; i < d.workerCount; i++ {
		// spin up workers
		wg.Add(1)
		go handleLocalFile(localRootLowercase, contentHash, processChan, resultChan, errorChan, &wg)
	}

	// walk in separate goroutine so that sends to errorChan don't block
	go func() {
		var pathsToWalk []string
		if len(d.subdirectories) > 0 {
			for _, dir := range d.subdirectories {
				pathsToWalk = append(pathsToWalk, filepath.Join(d.localRoot, dir))
			}
		} else {
			pathsToWalk = append(pathsToWalk, d.localRoot)
		}
		for _, path := range pathsToWalk {
			filepath.Walk(path, func(entryPath string, info os.FileInfo, err error) error {
				if err != nil {
					errorChan <- &FileError{Path: entryPath, Error: err}
					return nil
				}

				if info.Mode().IsDir() && skipLocalDir(entryPath) {
					return filepath.SkipDir
				}

				if info.Mode().IsRegular() && !skipLocalFile(entryPath) {
					processChan <- entryPath
				}

				return nil
			})
		}

		close(processChan)
	}()

	// Once processing goroutines are done, close result and error channels to indicate no more results streaming in
	go func() {
		wg.Wait()
		close(resultChan)
		close(errorChan)
	}()

	for {
		select {
		case result, ok := <-resultChan:
			if ok {
				heap.Push(manifest, result)
				updateChan <- &scanProgressUpdate{Type: localProgress, Count: manifest.Len()}
			} else {
				resultChan = nil
			}

		case e, ok := <-errorChan:
			if ok {
				errored = append(errored, e)
				updateChan <- &scanProgressUpdate{Type: errorProgress, Count: len(errored)}
			} else {
				errorChan = nil
			}
		}

		if resultChan == nil && errorChan == nil {
			break
		}
	}

	return
}

func skipLocalFile(path string) bool {
	if filepath.Base(path) == ".DS_Store" {
		return true
	}
	return false
}

func skipLocalDir(path string) bool {
	base := filepath.Base(path)
	for _, ignore := range ignoredDirectories {
		if base == ignore {
			return true
		}
	}
	return false
}

func handleLocalFile(localRootLowercase string, contentHash bool, processChan <-chan string, resultChan chan<- *File, errorChan chan<- *FileError, wg *sync.WaitGroup) {
	hashBuffer := make([]byte, hashBlockSize)
	for entryPath := range processChan {
		relPath, err := normalizePath(localRootLowercase, strings.ToLower(entryPath))
		if err != nil {
			errorChan <- &FileError{Path: entryPath, Error: err}
			continue
		}

		hash := ""
		if contentHash {
			hash, err = dropboxStyleContentHash(entryPath, hashBuffer)
			if err != nil {
				errorChan <- &FileError{Path: relPath, Error: err}
				continue
			}
		}

		resultChan <- &File{
			Path:        relPath,
			ContentHash: hash,
		}
	}
	wg.Done()
}

// See https://www.dropbox.com/developers/reference/content-hash
const hashBlockSize = 4 * 1024 * 1024

// Adapted from https://github.com/tj/go-dropbox
func dropboxStyleContentHash(path string, buf []byte) (string, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer f.Close()

	resultHash := sha256.New()
	n := hashBlockSize
	for n == hashBlockSize && err == nil {
		n, err = f.Read(buf)
		if err != nil && err != io.EOF {
			return "", err
		}
		if n > 0 {
			bufHash := sha256.Sum256(buf[:n])
			resultHash.Write(bufHash[:])
		}
	}
	return fmt.Sprintf("%x", resultHash.Sum(nil)), nil
}
