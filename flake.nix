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
              craneLib.filterCargoSources path type || pkgs.lib.hasSuffix ".sql" path;
          };
          strictDeps = true;
          cargoExtraArgs = "--workspace --locked";
          nativeBuildInputs = pkgs.lib.optionals pkgs.stdenv.hostPlatform.isDarwin [pkgs.rustPlatform.bindgenHook];
        };
        cargoArtifacts = craneLib.buildDepsOnly commonArgs;
        tower = craneLib.buildPackage (commonArgs
          // {
            inherit cargoArtifacts;
            meta.mainProgram = "tower";
          });
      in {
        packages = {
          default = tower;
          inherit tower;
        };

        checks = {
          build = tower;
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
