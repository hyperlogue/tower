{
  pkgs,
  rustToolchain,
}: let
  mkShell =
    if pkgs.stdenv.hostPlatform.isLinux
    then
      pkgs.mkShell.override {
        stdenv = pkgs.stdenvAdapters.useMoldLinker pkgs.clangStdenv;
      }
    else pkgs.mkShell;
in
  mkShell {
    name = "tower-dev";

    nativeBuildInputs =
      [rustToolchain]
      ++ pkgs.lib.optionals pkgs.stdenv.hostPlatform.isDarwin [pkgs.rustPlatform.bindgenHook];

    packages = with pkgs; [
      rust-analyzer
      cargo-edit
      cargo-watch
      pkg-config
      sqlite
      jq
      alejandra
      actionlint
      shellcheck
      direnv
      nix-direnv
    ];

    shellHook = ''
      tower_repo_root=$(git rev-parse --show-toplevel 2>/dev/null || pwd)
      export CARGO_TARGET_DIR="$tower_repo_root/target"
      unset tower_repo_root
    '';
  }
