# dr-exec

[![CI](https://github.com/danielle-rothermel/dr-exec/actions/workflows/ci.yaml/badge.svg)](https://github.com/danielle-rothermel/dr-exec/actions/workflows/ci.yaml)
[![PyPI](https://img.shields.io/pypi/v/dr-exec.svg)](https://pypi.org/project/dr-exec/)

| [Repo Definitions](https://danielle-rothermel.github.io/dr-exec/) | [dr-serialize v0.1.1](https://github.com/danielle-rothermel/dr-serialize) | [dr-store v0.1.1](https://github.com/danielle-rothermel/dr-store) |
| --- | --- | --- |

**dr-exec runs local processes through explicit, typed contracts.**
Production execution currently targets macOS and is organized into these
functional areas:

- **Declarations** describe trusted commands, untrusted commands, and
  untrusted Python together with their environment grants and resource
  budgets.
- **Execution** owns process startup, input and output transport, budget
  enforcement, cancellation, teardown, and outcome attribution.
- **Python runtime support** prepares isolated interpreter invocations and
  protects structured protocol messages from payload output.
- **Results and recording** represent outcomes as typed data and preserve
  declarations, process evidence, retained output, measurements, and recording
  health in durable run records.
- **Scheduling** runs finite batches and asynchronous streams through a shared
  capacity bound with completion-order delivery and intake backpressure.
- **Capability boundaries and fakes** let consumers substitute runtimes,
  stores, and executors while preserving the same behavioral contracts.
