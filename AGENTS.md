# AGENTS.md

## Cursor Cloud specific instructions

**gh-ost** is a triggerless online schema migration tool for MySQL, written in Go 1.21+. It is a single CLI binary with no runtime services needed for development (building, testing, linting).

### Build, Test, Lint

| Task | Command |
|------|---------|
| Build | `go build -o bin/gh-ost ./go/cmd/gh-ost/main.go` |
| Unit tests | `go test ./go/... -v` |
| Lint | `$(go env GOPATH)/bin/golangci-lint run --config=.golangci.yml` |

- Dependencies are **vendored** in `/vendor/`. No `go mod download` is needed.
- The repo's `script/build` uses a legacy GOPATH symlink setup via `script/bootstrap`; prefer direct `go build` with modules instead (works out of the box).
- `golangci-lint` v1.54.2 must be installed at `$(go env GOPATH)/bin/golangci-lint`. The `ifshort` linter deprecation warning is pre-existing and benign.
- Integration tests (`localtests/test.sh`) require a MySQL master-replica topology and are not runnable without MySQL infrastructure. Unit tests cover the core logic and are sufficient for local development validation.
- The binary requires a MySQL server to perform actual migrations; for development, `--check-flag`, `--version`, and `--help` can verify the binary works without a database.
