package main

import (
	"container/heap"
	"fmt"
	"io/ioutil"
	"math"
	"os"
	"path"
	"path/filepath"
	"runtime"
	"runtime/debug"
	"strings"
	"sync"
	"time"

	"github.com/dustin/go-humanize"
	"github.com/jessevdk/go-flags"
	"github.com/mitchellh/go-homedir"
	"github.com/pkg/profile"
	"github.com/tj/go-dropbox"

	"golang.org/x/text/unicode/norm"
)

/*
TODO
- Performance improvements:
	- Profile to find bottlenecks
	- Test if buffered channels improve performance in the parallel local file processing
	- Could printing progress for each local file result slow things down? (When processing lots of small files)
	- Print I/O usage? i.e. how many MB/s are we processing
- Memory usage improvements (if memory continues to be an issue):
	- Could use smaller representation of content hash (hex string -> fixed size byte array) - would probably cut out 30-35% of manifest size (60-70MB on my file set)
	- Evaluate heaps earlier to avoid hanging on to the entries that already match? This would increase complexity a lot and maybe require refactoring to use a map
- Clean up output formatting
	- Consolidate Dropbox error retry printing somehow? Maybe print retries on stderr, print info about error and how many retries it took to stdout
- Ignore more file names in skipLocalFile - see https://www.dropbox.com/help/syncing-uploads/files-not-syncing
- Do a real retry + backoff for Dropbox API errors (do we have access to the Retry-After header?)
*/

// File stores the result of either Dropbox API or local file listing
type File struct {
	Path        string
	ContentHash string
	Placeholder bool
	Symlink     bool
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
	Errored         []*FileError
	Matches         int
	Misses          int
	Placeholders    int
	Symlinks        int
}

type progressType int

const (
	remoteProgress progressType = iota
	localProgress
	errorProgress
)

type scanProgressUpdate struct {
	Type  progressType
	Count int
}

var ignoredFiles = [...]string{"Icon\r", ".DS_Store", ".dropbox"}
var ignoredDirectories = [...]string{"@eaDir", ".dropbox.cache"}

// Used for detecting symlinks that Dropbox doesn't actually sync
const EmptyFileContentHash = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"

func main() {
	tokenFromEnv := os.Getenv("DROPBOX_ACCESS_TOKEN")

	homeDir, err := homedir.Dir()
	if err != nil {
		fmt.Fprintln(os.Stderr, "Please set $HOME to a readable path!")
		fmt.Fprintln(os.Stderr, err.Error())
		os.Exit(1)
	}
	configDir := filepath.Join(homeDir, ".dropbox-sync-verifier")
	tokenPath := filepath.Join(configDir, "token")
	b, err := ioutil.ReadFile(tokenPath)
	tokenFromConfigFile := strings.TrimSpace(string(b))

	token := ""
	if tokenFromConfigFile != "" {
		token = tokenFromConfigFile
	} else {
		token = tokenFromEnv
	}

	if token == "" {
		fmt.Fprintf(os.Stderr, "Missing Dropbox OAuth token! Please add token to %s or set the DROPBOX_ACCESS_TOKEN environment variable.\n", tokenPath)
		os.Exit(1)
	}

	var opts struct {
		Verbose            bool   `short:"v" long:"verbose" description:"Show verbose debug information"`
		RemoteRoot         string `short:"r" long:"remote" description:"Directory in Dropbox to verify" default:""`
		LocalRoot          string `short:"l" long:"local" description:"Local directory to compare to Dropbox contents" default:"."`
		SelectiveSync      bool   `long:"selective" description:"Assume local is selectively synced - only check contents of top-level folders in local directory"`
		SkipContentHash    bool   `long:"skip-hash" description:"Skip checking content hash of local files"`
		WorkerCount        int    `short:"w" long:"workers" description:"Number of worker threads to use (defaults to 8) - set to 0 to use all CPU cores" default:"8"`
		FreeMemoryInterval int    `long:"free-memory-interval" description:"Interval (in seconds) to manually release unused memory back to the OS on low-memory systems" default:"0"`
		ProfileMemory      bool   `long:"profile-memory" description:"Generate a pprof memory profile"`
	}

	_, err = flags.Parse(&opts)
	if err != nil {
		fmt.Fprintln(os.Stderr, err.Error())
		os.Exit(1)
	}

	if opts.ProfileMemory {
		defer profile.Start(profile.MemProfile).Stop()
	}

	localRoot, _ := filepath.Abs(opts.LocalRoot)
	var localDirs []string
	if opts.SelectiveSync {
		localDirs, err = listFolders(opts.LocalRoot)
		if err != nil {
			fmt.Fprintln(os.Stderr, err.Error())
			os.Exit(1)
		}
	}

	// Dropbox API uses empty string for root, but for figuring out relative
	// paths of the returned entries it's easier to use "/". Conversion is
	// handled before the API call.
	remoteRoot := opts.RemoteRoot
	if remoteRoot == "" {
		remoteRoot = defaultRemoteRoot(localRoot)
	}
	if remoteRoot[0] != '/' {
		remoteRoot = "/" + remoteRoot
	}

	dbxClient := dropbox.New(dropbox.NewConfig(token))

	if opts.SelectiveSync {
		fmt.Printf("Comparing subfolders of Dropbox directory \"%v\" to local directory \"%v\"\n", remoteRoot, localRoot)
	} else {
		fmt.Printf("Comparing Dropbox directory \"%v\" to local directory \"%v\"\n", remoteRoot, localRoot)
	}

	if !opts.SkipContentHash {
		fmt.Println("Checking content hashes.")
	}
	workerCount := opts.WorkerCount
	if workerCount <= 0 {
		workerCount = int(math.Max(1, float64(runtime.NumCPU())))
	}
	fmt.Printf("Using %d local worker threads.\n", workerCount)
	fmt.Println("")

	updateChan := make(chan *scanProgressUpdate)
	var wg sync.WaitGroup
	wg.Add(2)

	var dropboxManifest *FileHeap
	var dropboxErr error
	go func() {
		dropboxManifest, dropboxErr = getDropboxManifest(updateChan, dbxClient, remoteRoot, localDirs)
		wg.Done()
	}()

	var localManifest *FileHeap
	var errored []*FileError
	var localErr error
	go func() {
		localDir := NewLocalDirectory(localRoot, localDirs, opts.SkipContentHash, workerCount)
		localManifest, errored, localErr = localDir.Manifest(updateChan)
		wg.Done()
	}()

	go func() {
		remoteCount := 0
		localCount := 0
		errorCount := 0
		for update := range updateChan {
			switch update.Type {
			case remoteProgress:
				remoteCount = update.Count
			case localProgress:
				localCount = update.Count
			case errorProgress:
				errorCount = update.Count
			}

			if opts.Verbose {
				fmt.Fprintf(os.Stderr, "Scanning: %d (remote) %d (local) %d (errored)\r", remoteCount, localCount, errorCount)
			}
		}
		fmt.Fprintf(os.Stderr, "\n")
	}()

	// set up manual garbage collection routine
	if opts.FreeMemoryInterval > 0 {
		go func() {
			for range time.Tick(time.Duration(opts.FreeMemoryInterval) * time.Second) {
				debug.FreeOSMemory()
				if opts.Verbose {
					var m runtime.MemStats
					runtime.ReadMemStats(&m)
					fmt.Fprintf(
						os.Stderr,
						"\n[%s] HeapAlloc: %s / HeapInuse: %s / HeapReleased: %s / Sys: %s / GCCPUFraction: %.2f%%\n",
						time.Now().Format("15:04:05"),
						humanize.Bytes(m.Alloc),
						humanize.Bytes(m.HeapInuse),
						humanize.Bytes(m.HeapReleased),
						humanize.Bytes(m.Sys),
						m.GCCPUFraction*100,
					)
				}
			}
		}()
	}

	// wait until remote and local scans are complete, then close progress reporting channel
	wg.Wait()
	close(updateChan)
	fmt.Printf("\nGenerated manifests for %d remote files, %d local files, with %d local errors\n\n", dropboxManifest.Len(), localManifest.Len(), len(errored))

	// check for fatal errors
	if dropboxErr != nil {
		panic(dropboxErr)
	}
	if localErr != nil {
		panic(localErr)
	}

	manifestComparison := compareManifests(dropboxManifest, localManifest, errored)

	if manifestComparison.Misses > 0 {
		fmt.Printf("❌ FAILURE: %d sync mismatches detected.\n", manifestComparison.Misses)
	} else {
		fmt.Printf("✅ SUCCESS: verified local sync.\n")
	}
	fmt.Println("")

	fmt.Println("DETAILS:")
	fmt.Println("")

	printFileList(manifestComparison.OnlyRemote, "Files only in remote")
	printFileList(manifestComparison.OnlyLocal, "Files only in local")
	printFileList(manifestComparison.ContentMismatch, "Files whose contents don't match")

	fmt.Printf("Errored: %d\n\n", len(manifestComparison.Errored))
	fmt.Printf("Placeholder files skipped: %d\n\n", manifestComparison.Placeholders)
	fmt.Printf("Symlink files skipped: %d\n\n", manifestComparison.Symlinks)

	if len(manifestComparison.Errored) > 0 {
		for _, rec := range manifestComparison.Errored {
			fmt.Printf("%s: %s\n", rec.Path, rec.Error)
		}
		if len(manifestComparison.Errored) > 0 {
			fmt.Print("\n\n")
		}
	}

	total := manifestComparison.Matches + manifestComparison.Misses + manifestComparison.Placeholders + manifestComparison.Symlinks
	fmt.Println("SUMMARY:")
	fmt.Printf("Files matched: %d/%d\n", manifestComparison.Matches, total)
	fmt.Printf("Files not matched: %d/%d\n", manifestComparison.Misses, total)
	fmt.Printf("Files skipped: %d/%d\n", manifestComparison.Placeholders+manifestComparison.Symlinks, total)

	if opts.SelectiveSync {
		fmt.Println("Subfolders verified:")
		for _, f := range localDirs {
			fmt.Println(f)
		}
	}

	if manifestComparison.Misses > 0 {
		os.Exit(1)
	}
}

func defaultRemoteRoot(localRoot string) string {
	relPath := ""
	for {
		dir, base := path.Split(localRoot)
		if base == "Dropbox" {
			return "/" + relPath
		} else if dir == "" {
			return "/"
		} else {
			relPath = path.Join(base, relPath)
			localRoot = strings.TrimRight(dir, "/")
		}
	}
}

func listFolders(localRoot string) (folders []string, err error) {
	root, err := filepath.Abs(localRoot)
	if err != nil {
		return
	}
	files, err := ioutil.ReadDir(root)
	if err != nil {
		return
	}
	for _, f := range files {
		if f.IsDir() && !skipLocalDir(f.Name()) {
			folders = append(folders, f.Name())
		}
	}

	return
}

func getDropboxManifest(updateChan chan<- *scanProgressUpdate, dbxClient *dropbox.Client, rootPath string, subdirectories []string) (manifest *FileHeap, err error) {
	// TODO handle the subdirectories
	manifest = &FileHeap{}
	heap.Init(manifest)

	var pathsToScan []string
	if len(subdirectories) > 0 {
		for _, dir := range subdirectories {
			pathsToScan = append(pathsToScan, path.Join(rootPath, dir))
		}
	} else {
		pathsToScan = append(pathsToScan, rootPath)
	}

	for _, scanPath := range pathsToScan {
		cursor := ""
		keepGoing := true
		retryCount := 0

		for keepGoing {
			var resp *dropbox.ListFolderOutput
			if cursor != "" {
				arg := &dropbox.ListFolderContinueInput{Cursor: cursor}
				resp, err = dbxClient.Files.ListFolderContinue(arg)
			} else {
				apiPath := scanPath
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
				// TODO: submit feature request for dropbox client to expose retry_after param
				if strings.HasPrefix(err.Error(), "too_many_requests") {
					fmt.Fprintf(os.Stderr, "\n[%s] [%d retries] Dropbox returned too many requests error, sleeping 60 seconds\n", time.Now().Format("15:04:05"), retryCount)
					// fmt.Fprintf(os.Stderr, "Error: %v\n", err)
					// fmt.Fprintf(os.Stderr, "Response: %v\n", resp)
					retryCount++
					time.Sleep(60 * time.Second)
					continue
				} else if retryCount < 10 { // TODO extract this magic number
					fmt.Fprintf(os.Stderr, "\n[%s] [%d retries] Error: %s - sleeping 1 second and retrying\n", time.Now().Format("15:04:05"), retryCount, err)
					fmt.Fprintf(os.Stderr, "Full Error: %#v\n", err)
					retryCount++
					time.Sleep(1 * time.Second)
					continue
				} else {
					fmt.Fprintf(os.Stderr, "\n[%s] Hit maximum of %d retries; aborting.\n", time.Now().Format("15:04:05"), retryCount)
					return
				}
			}
			// call was successful, reset retryCount
			retryCount = 0
			for _, entry := range resp.Entries {
				if entry.Tag == "file" {

					var relPath string
					relPath, err = normalizePath(rootPath, entry.PathLower)
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

			updateChan <- &scanProgressUpdate{Type: remoteProgress, Count: manifest.Len()}
		}
	}

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

func compareManifests(remoteManifest, localManifest *FileHeap, errored []*FileError) *ManifestComparison {
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
			// First check for placeholder files which shouldn't be compared
			if local.Placeholder {
				comparison.Placeholders++
			} else if local.Symlink && remote.ContentHash == EmptyFileContentHash {
				// Dropbox creates an empty file when it encounters a symlink
				comparison.Symlinks++
			} else if compareFileContents(remote, local) {
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
