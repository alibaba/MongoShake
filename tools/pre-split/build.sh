set -ex

# build on mac and run on linux:
env GOOS=linux GOARCH=amd64 go build pre_split.go
mv pre_split pre_split.linux