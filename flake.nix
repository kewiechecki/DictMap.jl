{
  description = "Flake for DictMap.jl";
  nixConfig = {
    bash-prompt = "\[DictMap$(__git_ps1 \" (%s)\")\]$ ";
  };

  inputs = {
    flake-utils.url = "github:numtide/flake-utils";
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
  };

  outputs = { self, nixpkgs, flake-utils }:
    flake-utils.lib.eachDefaultSystem (system:
      let
        pkgs = import nixpkgs { 
          inherit system;
          config.allowUnfree = true;
          # config.cudaSupport = system == "x86_64-linux";
		};

        juliaPkgs = pkgs.juliaPackages;

		shellPkgsNested = with pkgs; [
		  julia 
		  git
		];

        shellPkgs = pkgs.lib.flatten shellPkgsNested;

        dictMapBuilt = juliaPkgs.buildJuliaPackage {
          pname = "DictMap";
          version = "0.1.1"; # TODO: FIX THIS
          src = ./.;
          propagatedBuildInputs = [  ];
		};

      in {
        # A derivation for your package.
        packages.dictMap = dictMapBuilt;
        packages.default = self.packages.${system}.dictMap;

        # A development shell that provides Julia with your package instantiated.
        devShell = with pkgs; mkShell {
          name = "autoencoders-dev-shell";
          buildInputs = shellPkgs;
          shellHook = ''
source ${git}/share/bash-completion/completions/git-prompt.sh
export JULIA_PROJECT="@."
          '';
        };
      }
    );
}

