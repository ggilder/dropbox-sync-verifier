package main

import (
	"fmt"
	"github.com/dropbox/dropbox-sdk-go-unofficial/dropbox"
	"github.com/dropbox/dropbox-sdk-go-unofficial/dropbox/files"
	"github.com/jessevdk/go-flags"
	"os"
	"path/filepath"
)

/* TODO
- Implement file hash comparison - see https://www.dropbox.com/developers/reference/content-hash
	- Add flag for whether to compare file SHAs
- Should limit actually apply to files or files + directories?
- Clean up output formatting
*/

func main() {
	token := os.Getenv("DROPBOX_ACCESS_TOKEN")
	if token == "" {
		fmt.Fprintln(os.Stderr, "Missing Dropbox OAuth token! Please set the DROPBOX_ACCESS_TOKEN environment variable.")
		os.Exit(1)
	}

	var opts struct {
		Verbose    bool   `short:"v" long:"verbose" description:"Show verbose debug information"`
		RemoteRoot string `short:"r" long:"remote" description:"Directory in Dropbox to verify" default:"/"`
		LocalRoot  string `short:"l" long:"local" description:"Local directory to compare to Dropbox contents" default:"."`
		MaxFiles   int    `short:"m" long:"max" description:"Maximum number of files to verify" default:"2000"`
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

	config := dropbox.Config{Token: token, Verbose: false}
	dbx := files.New(config)
	totalFilesListed := 0
	totalSyncSuccess := 0
	totalSyncError := 0
	cursor := ""
	keepGoing := true

	fmt.Printf("Comparing Dropbox directory \"%v\" to local directory \"%v\"\n", opts.RemoteRoot, localRoot)

	for keepGoing && totalFilesListed < opts.MaxFiles {
		var resp *files.ListFolderResult
		var err error
		if cursor != "" {
			arg := files.NewListFolderContinueArg(cursor)
			resp, err = dbx.ListFolderContinue(arg)
		} else {
			root := opts.RemoteRoot
			if root == "/" {
				root = ""
			}
			arg := &files.ListFolderArg{
				Path:                            root,
				Recursive:                       true,
				IncludeMediaInfo:                false,
				IncludeDeleted:                  false,
				IncludeHasExplicitSharedMembers: false,
			}
			resp, err = dbx.ListFolder(arg)
		}
		if err != nil {
			panic(err)
		}
		for _, entry := range resp.Entries {
			switch v := entry.(type) {
			case *files.FolderMetadata:
				// skip, folders are boring
			case *files.FileMetadata:
				synced := checkFileSynced(&v.Metadata.PathDisplay, &opts.RemoteRoot, &localRoot)
				if !synced {
					fmt.Printf("%v is not synced to local root!\n", v.Metadata.PathDisplay)
					totalSyncError++
				} else {
					totalSyncSuccess++
				}
			case *files.DeletedMetadata:
				// skip, should not be returned anyway
			default:
				fmt.Printf("Unexpected entry type! %#v\n", v)
			}
		}

		cursor = resp.Cursor
		keepGoing = resp.HasMore
		totalFilesListed += len(resp.Entries)

		fmt.Printf("%v entries listed\n", len(resp.Entries))
		fmt.Printf("%v entries out of %v limit\n", totalFilesListed, opts.MaxFiles)
		fmt.Printf("has more? %v\n", resp.HasMore)
		fmt.Printf("files verified: %v\n", totalSyncSuccess)
		fmt.Printf("files errored: %v\n", totalSyncError)
	}
}

func checkFileSynced(path, root, localRoot *string) bool {
	var relPath string
	var err error
	if relPath, err = filepath.Rel(*root, *path); err != nil {
		panic(err)
	}
	localPath := filepath.Join(*localRoot, relPath)
	if _, err := os.Stat(localPath); err != nil {
		// TODO: maybe print in a debug mode
		// fmt.Printf("%v\n", err.Error())
		return false
	}
	return true
}
