RUSTUP_HOME=/opt/rust
export RUSTUP_HOME
CARGO_HOME=/opt/rust
export CARGO_HOME
curl https://sh.rustup.rs -sSf | sh -s -- -y --no-modify-path
printf '#!/bin/sh\n\nRUSTUP_HOME=/opt/rust exec /opt/rust/bin/${0##*/} \"$@\"' > /usr/local/bin/rustc
ln -sf /opt/rust/bin/cargo /usr/local/bin/cargo
ln -sf /opt/rust/bin/cargo-clippy /usr/local/bin/cargo-clippy
ln -sf /opt/rust/bin/cargo-fmt /usr/local/bin/cargo-fmt
ln -sf /opt/rust/bin/cargo-miri /usr/local/bin/cargo-miri
ln -sf /opt/rust/bin/clippy-driver /usr/local/bin/clippy-driver
ln -sf /opt/rust/bin/rls /usr/local/bin/rls
ln -sf /opt/rust/bin/rust-analyzer /usr/local/bin/rust-analyzer
ln -sf /opt/rust/bin/rustc /usr/local/bin/rustc
ln -sf /opt/rust/bin/rustdoc /usr/local/bin/rustdoc
ln -sf /opt/rust/bin/rustfmt /usr/local/bin/rustfmt
ln -sf /opt/rust/bin/rust-gdb /usr/local/bin/rust-gdb
ln -sf /opt/rust/bin/rust-gdbgui /usr/local/bin/rust-gdbgui
ln -sf /opt/rust/bin/rust-lldb /usr/local/bin/rust-lldb
ln -sf /opt/rust/bin/rustup /usr/local/bin/rustup
source "/opt/rust/env"
