# ASIMOV Dataset Command-Line Interface (CLI)

[![License](https://img.shields.io/badge/license-Public%20Domain-blue.svg)](https://unlicense.org)
[![Compatibility](https://img.shields.io/badge/rust-1.81%2B-blue)](https://blog.rust-lang.org/2024/09/05/Rust-1.81.0.html)
[![Package](https://img.shields.io/crates/v/asimov-dataset-cli)](https://crates.io/crates/asimov-dataset-cli)

🚧 _We are building in public. This is presently under heavy construction._

## ✨ Features

- 100% free and unencumbered public domain software.

## 🛠️ Prerequisites

- [Rust](https://rust-lang.org) 1.81+

## ⬇️ Installation

### Installation from Source Code

#### Installation via Cargo

```bash
cargo install asimov-dataset-cli@25.0.0-dev.2
```

### Installation using Package Manager

#### [Scoop](https://scoop.sh)

First things first, you need to add our custom scoop bucket.
This needs to be done only once, so that scoop knows where to find our packages.

```bash
scoop bucket add asimov-platform https://github.com/asimov-platform/scoop-bucket
```

Now, installing ASIMOV CLI is as easy as running:

```bash
scoop install asimov-platform/asimov-dataset-cli
```

## 👉 Examples

```bash
# publish RDF data in data1.ttl and data2.nt to an on-chain repository at your-repo.testnet
asimov-dataset publish --network testnet your-repo.testnet ./data1.ttl ./data2.nt
```

## 📚 Reference

TBD

## 👨‍💻 Development

```bash
git clone https://github.com/asimov-platform/asimov-dataset-cli.git
```

---

[![Share on X](https://img.shields.io/badge/share%20on-x-03A9F4?logo=x)](https://x.com/intent/post?url=https://github.com/asimov-platform/asimov-dataset-cli&text=ASIMOV%20Dataset%20Command-Line%20Interface%20%28CLI%29)
[![Share on Reddit](https://img.shields.io/badge/share%20on-reddit-red?logo=reddit)](https://reddit.com/submit?url=https://github.com/asimov-platform/asimov-dataset-cli&title=ASIMOV%20Dataset%20Command-Line%20Interface%20%28CLI%29)
[![Share on Hacker News](https://img.shields.io/badge/share%20on-hn-orange?logo=ycombinator)](https://news.ycombinator.com/submitlink?u=https://github.com/asimov-platform/asimov-dataset-cli&t=ASIMOV%20Dataset%20Command-Line%20Interface%20%28CLI%29)
[![Share on Facebook](https://img.shields.io/badge/share%20on-fb-1976D2?logo=facebook)](https://www.facebook.com/sharer/sharer.php?u=https://github.com/asimov-platform/asimov-dataset-cli)
[![Share on LinkedIn](https://img.shields.io/badge/share%20on-linkedin-3949AB?logo=linkedin)](https://www.linkedin.com/sharing/share-offsite/?url=https://github.com/asimov-platform/asimov-dataset-cli)
