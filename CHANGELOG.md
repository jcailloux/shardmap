# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/),
and this project adheres to [Semantic Versioning](https://semver.org/).

## [1.0.1] - 2026-02-17

### Changed

- Shard selection now uses O(1) partial hash for string-like keys instead of full `absl::Hash`, eliminating redundant double-hashing
- Refactored `shard_id_for` into `fast_shard_hash<T>` with 4 type-dispatched strategies: Fibonacci (integers), partial wyhash (string-like), recursive (tuples), and `absl::Hash` fallback with splitmix64 mixer
