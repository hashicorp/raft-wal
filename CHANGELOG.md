## Unreleased

### Improvements

### Changes

### Fixed

- `GetLog` no longer returns logs that a head truncation (`DeleteRange` from the
  first index) removed from the unsealed tail segment; it returns `ErrNotFound`
  for any index before `FirstIndex`.

### Security
