{
  description = "tower Rust development environment";

  inputs = {
    nixpkgs.url = "nixpkgs/nixos-unstable";
    flake-parts = {
      url = "github:hercules-ci/flake-parts";
      inputs.nixpkgs-lib.follows = "nixpkgs";
    };
    fenix = {
      url = "github:nix-community/fenix";
      inputs.nixpkgs.follows = "nixpkgs";
    };
    crane.url = "github:ipetkov/crane";
  };

  outputs = inputs @ {
    nixpkgs,
    flake-parts,
    fenix,
    crane,
    ...
  }:
    flake-parts.lib.mkFlake {inherit inputs;} {
      systems = ["x86_64-linux" "aarch64-linux" "aarch64-darwin"];

      perSystem = {
        pkgs,
        system,
        ...
      }: let
        rustToolchain = fenix.packages.${system}.stable.withComponents [
          "cargo"
          "clippy"
          "rust-src"
          "rustc"
          "rustfmt"
        ];
        craneLib = (crane.mkLib pkgs).overrideToolchain rustToolchain;
        commonArgs = {
          pname = "tower";
          src = pkgs.lib.cleanSourceWith {
            src = ./.;
            filter = path: type:
              craneLib.filterCargoSources path type
              || pkgs.lib.hasSuffix ".sql" path
              || pkgs.lib.hasSuffix ".mjs" path
              || pkgs.lib.hasPrefix "LICENSE" (baseNameOf path);
          };
          strictDeps = true;
          nativeCheckInputs = [pkgs.nodejs pkgs.git];
          cargoExtraArgs = "--workspace --locked";
          nativeBuildInputs = pkgs.lib.optionals pkgs.stdenv.hostPlatform.isDarwin [pkgs.rustPlatform.bindgenHook];
        };
        cargoArtifacts = craneLib.buildDepsOnly commonArgs;
        tower = craneLib.buildPackage (commonArgs
          // {
            inherit cargoArtifacts;
            meta.mainProgram = "tower-cli";
            postInstall = ''
              for program in tower tower-cli tower-terminal-pool; do
                test -x "$out/bin/$program"
              done
              test "$(ls -A "$out/bin" | wc -l)" -eq 3
              install -Dm644 LICENSE-MIT "$out/share/licenses/tower/LICENSE-MIT"
              install -Dm644 LICENSE-APACHE "$out/share/licenses/tower/LICENSE-APACHE"
              install -Dm644 LICENSE-CAPTAIN-MIAO "$out/share/licenses/tower/LICENSE-CAPTAIN-MIAO"
            '';
          });
      in {
        packages = {
          default = tower;
          inherit tower;
        };

        checks = {
          build = tower;
          core = craneLib.cargoClippy (commonArgs
            // {
              inherit cargoArtifacts;
              cargoExtraArgs = "-p tower-server --locked";
              cargoClippyExtraArgs = "--all-targets -- -D warnings";
            });
          clippy = craneLib.cargoClippy (commonArgs
            // {
              inherit cargoArtifacts;
              cargoClippyExtraArgs = "--all-targets -- -D warnings";
            });
          fmt = craneLib.cargoFmt {inherit (commonArgs) pname src;};
        };

        devShells.default = import ./nix/shell.nix {inherit pkgs rustToolchain;};
        formatter = pkgs.alejandra;
      };
    };
}
