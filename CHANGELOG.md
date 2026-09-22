# Changelog

## Unreleased

### Changed

- Inlined the helpers previously imported from the separate `py-shared-tools` library
  (`RestAdapter`, `ensure_dir`, `write_structured_file`) into `medicare_rebuild.utils`, and
  removed the git dependency. The repository no longer needs a second repository to install
  or run; `certifi` and `urllib3` are now direct dependencies. See
  [decision 0013](docs/decisions/0013-inline-the-shared-helpers.md).
