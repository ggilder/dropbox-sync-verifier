# dropbox-sync-verifier
Validate a local directory against Dropbox to make sure all your files are correctly synced.

## Notes

Cross-compile to make a binary that can run on the Synology NAS:

```
GOOS=linux GOARCH=amd64 go build verifier.go
```
