---
packages:
  '@rhythm-app/streamer':
    type: patch
---

## Keep release playback moving after CPU contention

Remove an admitted current producer from the CPU wait queue in release builds as well as debug builds. A stale waiter could otherwise prevent preloading and automatic track promotion. Exercise repeated seek and promotion with one CPU worker in the existing release binding regression.
