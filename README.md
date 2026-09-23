# snapshot-core

The storage core of the Versioned DB: it versions any kind of data. It stores
content-addressed, encrypted chunks on pluggable backends (local disk, several
mount points, S3), records commits, branches and tags, and diffs and merges by
delegating meaning to data-model plugins. It knows nothing about tables, SQL
or protocols; the table model lives in a consuming repository as a plugin.

- Spec: [`docs/specs/storage-core-spec.md`](docs/specs/storage-core-spec.md)
- How the spec became code, formats, protocols: [`docs/DESIGN.md`](docs/DESIGN.md)
- Testing standard: [`docs/TESTING.md`](docs/TESTING.md) · process: [`CONTRIBUTING.md`](CONTRIBUTING.md)

```
mise install && mise run ci
```

Pure Go (`CGO_ENABLED=0`). Depends on
[disknexus-engine](https://github.com/SmithOperatingSolutions/disknexus-engine)
as a pinned, unmodified module, imported only by `core/dnx`.
