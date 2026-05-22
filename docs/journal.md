# Knot — Development Journal

Daily log of progress, decisions, surprises, bugs, and notes. Source material for the eventual blog post(s).

**Format per day:** what I did, what surprised me, what I learned, what's blocked, what's next.

---

## 2026-05-19 — Day 0 (planning + scaffolding)

**Decisions locked:**
- Project name: **Knot** (small, simple, non-techy — short fall from "AstraDB" but better differentiated)
- License: MIT
- C++ standard: C++20
- Build: CMake + Ninja + vcpkg
- Initial deps: spdlog, fmt (others added in later weeks)

**Did:**
- Renamed working tree from `quorum` → `knot`; updated all doc references
- Created Day 1 scaffold: `.gitignore`, `LICENSE`, `README.md`, `CMakeLists.txt`, `CMakePresets.json`, `vcpkg.json`, `.clang-format`, `.clang-tidy`, hello-world `knotd` binary, GitHub Actions CI, `proto/knot.proto` skeleton
- Saved project context to memory

**Next:**
- Run `brew install cmake ninja clang-format`
- Bootstrap vcpkg locally
- Run first build, verify `knotd` prints log lines
- `git init` and push to GitHub
- Verify CI turns green

---

## Day 1 — (date TBD)

- [ ] Local build green (`cmake --preset=default && cmake --build build/default`)
- [ ] `./build/default/knotd` runs and logs three lines
- [ ] `git init` + initial commit
- [ ] GitHub repo created (public)
- [ ] First push, CI runs and passes
- [ ] Format check passes (no source files yet to format, but workflow runs)

**Surprises / notes:**

(fill in as you go)

---

## Template for future days

```
## Day N — YYYY-MM-DD

**Goal:** one-sentence what I'm trying to achieve today.

**Did:**
- bullet
- bullet

**Surprises / learned:**
- bullet (this becomes the blog post material)

**Bugs / blockers:**
- bullet (incident, what I tried, what worked)

**Next:**
- bullet
```
