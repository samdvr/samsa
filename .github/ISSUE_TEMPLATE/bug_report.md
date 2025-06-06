---
name: Bug Report
about: Create a report to help us improve Samsa
title: "[BUG] "
labels: ["bug", "needs-triage"]
assignees: ""
---

## Bug Description

**Brief description:**
A clear and concise description of what the bug is.

## Expected Behavior

A clear and concise description of what you expected to happen.

## Actual Behavior

A clear and concise description of what actually happened.

## Steps to Reproduce

1. Go to '...'
2. Run command '....'
3. Configure '....'
4. See error

## Environment

**System Information:**

- OS: [e.g., Ubuntu 22.04, macOS 13.1, Windows 11]
- Architecture: [e.g., x86_64, arm64]
- Samsa Version: [e.g., v0.1.0]
- Rust Version: [output of `rustc --version`]

**Configuration:**

- Deployment type: [e.g., single node, cluster]
- Object store: [e.g., local filesystem, S3, MinIO]
- etcd version: [e.g., v3.5.14]

## Error Logs

**Server logs:**

```
[paste relevant server logs here]
```

**CLI output:**

```
[paste CLI command and output here]
```

**etcd logs (if relevant):**

```
[paste etcd logs if applicable]
```

## Configuration Files

**Server configuration:**

```toml
[paste relevant config sections]
```

**Environment variables:**

```bash
# List relevant environment variables
NODE_ID=...
ETCD_ENDPOINTS=...
```

## Additional Context

**Screenshots:**
If applicable, add screenshots to help explain your problem.

**Related issues:**
Link to any related issues or discussions.

**Workarounds:**
If you found any workarounds, please describe them.

## Severity

<!-- Mark the appropriate severity level -->

- [ ] **Critical** - System is unusable, data loss possible
- [ ] **High** - Major functionality broken, no workaround
- [ ] **Medium** - Important functionality affected, workaround exists
- [ ] **Low** - Minor issue, doesn't affect core functionality

## Component

<!-- Mark the affected component(s) -->

- [ ] Server
- [ ] CLI (samsa-cli)
- [ ] Storage layer
- [ ] Metadata management
- [ ] gRPC API
- [ ] Configuration
- [ ] Testing/CI
- [ ] Documentation
- [ ] Other: \***\*\_\_\_\*\***

## Reproducibility

<!-- How often does this issue occur? -->

- [ ] Always (100%)
- [ ] Frequently (>50%)
- [ ] Sometimes (20-50%)
- [ ] Rarely (<20%)
- [ ] Unable to reproduce

## Impact

**Business impact:**
Describe how this affects your use case or workflow.

**User impact:**
How many users or systems are affected?

## Potential Solution

If you have ideas about what might be causing the issue or how to fix it, please share them here.

---

**For Maintainers:**

- [ ] Bug confirmed
- [ ] Severity assessment complete
- [ ] Component ownership assigned
- [ ] Priority set
- [ ] Milestone assigned
