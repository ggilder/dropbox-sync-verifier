package main

import (
	"container/heap"
	"fmt"
	"github.com/jessevdk/go-flags"
	"github.com/tj/go-dropbox"
	"golang.org/x/text/unicode/norm"
	"os"
	"path/filepath"
	"strings"
	"time"
)

/* TODO
- Clean up output formatting
- Parallelize local/remote file listing
  - Maybe find additional ways to speed up? Generating local hashes is probably
    largest bottleneck, would this benefit at all from parallelization?
- Add progress printing - maybe collect progress from remote/local listing through channels
- Test for more case issues - already handling when root folder is lowercased
  by Dropbox, but maybe other path components could be as well?
- Ignore more file names in skipLocalFile - see https://www.dropbox.com/help/syncing-uploads/files-not-syncing
- Do a real retry + backoff for Dropbox API errors (do we have access to the Retry-After header?)
*/

// File stores the result of either Dropbox API or local file listing
type File struct {
	Path        string
	ContentHash string
}

// FileError records a local file that could not be read due to an error
type FileError struct {
	Path  string
	Error error
}

// FileHeap is a list of Files sorted by path
type FileHeap []*File

func (h FileHeap) Len() int           { return len(h) }
func (h FileHeap) Less(i, j int) bool { return h[i].Path < h[j].Path }
func (h FileHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }

// Push a File onto the heap
func (h *FileHeap) Push(x interface{}) {
	// Push and Pop use pointer receivers because they modify the slice's length,
	// not just its contents.
	*h = append(*h, x.(*File))
}

// Pop a File off the heap
func (h *FileHeap) Pop() interface{} {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}

// PopOrNil pops a File off the heap or returns nil if there's nothing left
func (h *FileHeap) PopOrNil() *File {
	if h.Len() > 0 {
		return heap.Pop(h).(*File)
	}
	return nil
}

// ManifestComparison records the relative paths that differ between remote and
// local versions of a directory
type ManifestComparison struct {
	OnlyRemote      []string
	OnlyLocal       []string
	ContentMismatch []string
	Errored         []FileError
	Matches         int
	Misses          int
}

func main() {
	token := os.Getenv("DROPBOX_ACCESS_TOKEN")
	if token == "" {
		fmt.Fprintln(os.Stderr, "Missing Dropbox OAuth token! Please set the DROPBOX_ACCESS_TOKEN environment variable.")
		os.Exit(1)
	}

	var opts struct {
		Verbose          bool   `short:"v" long:"verbose" description:"Show verbose debug information"`
		RemoteRoot       string `short:"r" long:"remote" description:"Directory in Dropbox to verify" default:"/"`
		LocalRoot        string `short:"l" long:"local" description:"Local directory to compare to Dropbox contents" default:"."`
		CheckContentHash bool   `long:"check" description:"Check content hash of local files"`
	}

	_, err := flags.Parse(&opts)
	if err != nil {
		fmt.Fprintln(os.Stderr, err.Error())
		os.Exit(1)
	}

	// Dropbox API uses empty string for root, but for figuring out relative
	// paths of the returned entries it's easier to use "/". Conversion is
	// handled before the API call.
	if opts.RemoteRoot == "" {
		opts.RemoteRoot = "/"
	}
	if opts.RemoteRoot[0] != '/' {
		opts.RemoteRoot = "/" + opts.RemoteRoot
	}

	localRoot, _ := filepath.Abs(opts.LocalRoot)

	dbxClient := dropbox.New(dropbox.NewConfig(token))

	fmt.Printf("Comparing Dropbox directory \"%v\" to local directory \"%v\"\n", opts.RemoteRoot, localRoot)
	if opts.CheckContentHash {
		fmt.Println("Checking content hashes.")
	}
	fmt.Println("")

	dropboxManifest, err := getDropboxManifest(dbxClient, opts.RemoteRoot)
	if err != nil {
		panic(err)
	}

	localManifest, errored, err := getLocalManifest(localRoot, opts.CheckContentHash)
	if err != nil {
		panic(err)
	}

	manifestComparison := compareManifests(dropboxManifest, localManifest, errored)

	fmt.Println("")

	printFileList(manifestComparison.OnlyRemote, "Files only in remote")
	printFileList(manifestComparison.OnlyLocal, "Files only in local")
	printFileList(manifestComparison.ContentMismatch, "Files whose contents don't match")

	fmt.Printf("Errored: %d\n\n", len(manifestComparison.Errored))
	if len(manifestComparison.Errored) > 0 {
		for _, rec := range manifestComparison.Errored {
			fmt.Printf("%s: %s\n", rec.Path, rec.Error)
		}
		if len(manifestComparison.Errored) > 0 {
			fmt.Print("\n\n")
		}
	}

	total := manifestComparison.Matches + manifestComparison.Misses
	fmt.Println("SUMMARY:")
	fmt.Printf("Files matched: %d/%d\n", manifestComparison.Matches, total)
	fmt.Printf("Files not matched: %d/%d\n", manifestComparison.Misses, total)
}

func getDropboxManifest(dbxClient *dropbox.Client, rootPath string) (manifest *FileHeap, err error) {
	manifest = &FileHeap{}
	heap.Init(manifest)
	cursor := ""
	keepGoing := true

	for keepGoing {
		var resp *dropbox.ListFolderOutput
		if cursor != "" {
			arg := &dropbox.ListFolderContinueInput{Cursor: cursor}
			resp, err = dbxClient.Files.ListFolderContinue(arg)
		} else {
			apiPath := rootPath
			if apiPath == "/" {
				apiPath = ""
			}
			arg := &dropbox.ListFolderInput{
				Path:             apiPath,
				Recursive:        true,
				IncludeMediaInfo: false,
				IncludeDeleted:   false,
			}
			resp, err = dbxClient.Files.ListFolder(arg)
		}
		if err != nil {
			if strings.HasPrefix(err.Error(), "too_many_requests") {
				fmt.Fprint(os.Stderr, "Dropbox returned too many requests error, sleeping 60 seconds...\n")
				fmt.Fprintf(os.Stderr, "Error: %v\n", err)
				fmt.Fprintf(os.Stderr, "Response: %v\n", resp)
				time.Sleep(60 * time.Second)
				continue
			}
			return
		}
		for _, entry := range resp.Entries {
			if entry.Tag == "file" {

				var relPath string
				relPath, err = normalizePath(rootPath, entry.PathDisplay)
				if err != nil {
					return
				}
				heap.Push(manifest, &File{
					Path:        relPath,
					ContentHash: entry.ContentHash,
				})
			}
		}

		cursor = resp.Cursor
		keepGoing = resp.HasMore

		// DEBUG info
		fmt.Fprintf(os.Stderr, "%d dropbox files listed          \r", manifest.Len())
	}

	return
}

func getLocalManifest(localRoot string, contentHash bool) (manifest *FileHeap, errored []FileError, err error) {
	manifest = &FileHeap{}
	heap.Init(manifest)

	err = filepath.Walk(localRoot, func(entryPath string, info os.FileInfo, err error) error {
		if err != nil {
			errored = append(errored, FileError{Path: entryPath, Error: err})
			return nil
		}

		if info.Mode().IsRegular() && !skipLocalFile(entryPath) {
			relPath, err := normalizePath(localRoot, entryPath)
			if err != nil {
				errored = append(errored, FileError{Path: entryPath, Error: err})
				return nil
			}

			hash := ""
			if contentHash {
				hash, err = dropbox.FileContentHash(entryPath)
				if err != nil {
					errored = append(errored, FileError{Path: relPath, Error: err})
					return nil
				}
			}

			heap.Push(manifest, &File{
				Path:        relPath,
				ContentHash: hash,
			})

			// DEBUG info
			fmt.Fprintf(os.Stderr, "%d local files listed (%d errors)    \r", manifest.Len(), len(errored))
		}

		return nil
	})

	return
}

func normalizePath(root string, entryPath string) (string, error) {
	relPath, err := filepath.Rel(root, entryPath)
	if err != nil {
		return "", err
	}
	if relPath[0:3] == "../" {
		// try lowercase root instead
		relPath, err = filepath.Rel(strings.ToLower(root), entryPath)
		if err != nil {
			return "", err
		}
	}

	// Normalize Unicode combining characters
	relPath = norm.NFC.String(relPath)
	return relPath, nil
}

func skipLocalFile(path string) bool {
	if filepath.Base(path) == ".DS_Store" {
		return true
	}
	return false
}

func compareManifests(remoteManifest, localManifest *FileHeap, errored []FileError) *ManifestComparison {
	// 1. Pop a path off both remote and local manifests.
	// 2. While remote & local are both not nil:
	//    Compare remote & local:
	//    a. If local is nil or local > remote, this file is only in remote. Record and pop remote again.
	//    b. If remote is nil or local < remote, this file is only in local. Record and pop local again.
	//    c. If local == remote, check for content mismatch. Record if necessary and pop both again.
	comparison := &ManifestComparison{Errored: errored}
	local := localManifest.PopOrNil()
	remote := remoteManifest.PopOrNil()
	for local != nil || remote != nil {
		if local == nil {
			comparison.OnlyRemote = append(comparison.OnlyRemote, remote.Path)
			comparison.Misses++
			remote = remoteManifest.PopOrNil()
		} else if remote == nil {
			comparison.OnlyLocal = append(comparison.OnlyLocal, local.Path)
			comparison.Misses++
			local = localManifest.PopOrNil()
		} else if local.Path > remote.Path {
			comparison.OnlyRemote = append(comparison.OnlyRemote, remote.Path)
			comparison.Misses++
			remote = remoteManifest.PopOrNil()
		} else if local.Path < remote.Path {
			comparison.OnlyLocal = append(comparison.OnlyLocal, local.Path)
			comparison.Misses++
			local = localManifest.PopOrNil()
		} else {
			// this must mean that remote.Path == local.Path
			if compareFileContents(remote, local) {
				comparison.Matches++
			} else {
				comparison.ContentMismatch = append(comparison.ContentMismatch, local.Path)
				comparison.Misses++
			}
			local = localManifest.PopOrNil()
			remote = remoteManifest.PopOrNil()
		}
	}
	return comparison
}

func compareFileContents(remote, local *File) bool {
	if remote.ContentHash == "" || local.ContentHash == "" {
		// Missing content hash for one of the files, possibly intentionally,
		// so can't compare. Assume that presence of both is enough to
		// validate.
		return true
	}
	return remote.ContentHash == local.ContentHash
}

func printFileList(files []string, description string) {
	fmt.Printf("%s: %d\n\n", description, len(files))
	for _, path := range files {
		fmt.Println(path)
	}
	if len(files) > 0 {
		fmt.Print("\n\n")
	}
}
