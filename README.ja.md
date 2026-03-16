## FrozenOS Builder (日本語)

**FrozenOS Builder** は Ubuntu インストール ISO をカスタマイズするための CLI ツールです。

英語版 README: [`README.md`](./README.md)

## 必要要件

* Rust ツールチェーン (Cargo)
* `xorriso`
  インストール:

```bash
sudo apt install xorriso
```

## クイックスタート

```bash
cargo build
sudo ./target/debug/frozenos_builder build -f examples/minimal.yml
```

出力例:

```text
dist/frozenos-minimal.iso
```

## レシピ

最小構成の例: `examples/minimal.yml`  
ROS 2 Jazzy カスタマイズ例: `examples/ros2.yml`
`examples/overlay/autoinstall.yaml` は Ubuntu Desktop 向け自動インストールのサンプルで、レシピの copy ステップでコピーされます。
