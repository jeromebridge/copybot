FROM python:3.11-bookworm

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        build-essential curl git libssl-dev pkg-config \
    && rm -rf /var/lib/apt/lists/*

RUN useradd -m -s /bin/bash vscode \
    && mkdir -p /workspace /home/vscode/.cargo/registry /home/vscode/.cargo/git /home/vscode/.cache/pip \
    && chown -R vscode:vscode /workspace /home/vscode

ENV RUSTUP_HOME=/home/vscode/.rustup \
    CARGO_HOME=/home/vscode/.cargo \
    PATH=/home/vscode/.cargo/bin:$PATH

USER vscode

RUN curl --proto '=https' --tlsv1.2 -fsSL https://sh.rustup.rs \
    | sh -s -- -y --profile minimal --default-toolchain none \
    && rustup toolchain install 1.93.1 --profile minimal --component rustfmt --component clippy --component rust-src \
    && rustup default 1.93.1

WORKDIR /workspace
