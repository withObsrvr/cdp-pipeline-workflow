{
  description = "CDP Pipeline Workflow - Stellar blockchain data processing pipeline";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
    flake-utils.url = "github:numtide/flake-utils";
  };

  outputs = { self, nixpkgs, flake-utils }:
    flake-utils.lib.eachDefaultSystem (system:
      let
        pkgs = nixpkgs.legacyPackages.${system};

        # Override DuckDB to version 1.4.1 for DuckLake 0.3 support
        # DuckLake 0.3 includes better constraint handling and MERGE INTO support
        # Reference: https://motherduck.com/blog/announcing-duckdb-141-motherduck/
        duckdb_1_4_1 = pkgs.duckdb.overrideAttrs (oldAttrs: rec {
          version = "1.4.1";

          src = pkgs.fetchFromGitHub {
            owner = "duckdb";
            repo = "duckdb";
            rev = "v${version}";
            hash = "sha256-w/mELyRs4B9hJngi1MLed0fHRq/ldkkFV+SDkSxs3O8=";
          };
        });

        # System dependencies required for CGO
        systemDeps = with pkgs; [
          zeromq
          czmq
          libsodium.dev  # Use dev output for pkg-config files
          arrow-cpp
          duckdb_1_4_1  # Use DuckDB 1.4.1 instead of default 1.3.2
          pkg-config
          gcc
          gnumake
          cmake
          openssl.dev    # Use dev output for pkg-config files
        ];

        # Development tools
        devTools = with pkgs; [
          # Container tools
          docker
          skopeo
          
          # Development utilities
          yq-go
          jq
          curl
          git
          vim
          htop
          nettools
          iputils
          
          # Go tools
          gotools
          gopls
          delve
        ];

        # Build the CDP Pipeline application
        cdp-pipeline = pkgs.buildGoModule rec {
          pname = "cdp-pipeline-workflow";
          version = "0.1.0";

          src = ./.;

          # IMPORTANT: Update this hash after first build attempt
          # Run: nix build 2>&1 | grep "got:" | awk '{print $2}'
          vendorHash = "sha256-W4uBPhzZdPfoC5SHDt9pjc4pbfhKZR6Vyikf3JLrhm0=";

          nativeBuildInputs = systemDeps;
          buildInputs = systemDeps;

          # Enable CGO
          env.CGO_ENABLED = "1";

          # Build flags  
          ldflags = [
            "-s"
            "-w"
          ];

          # Set build environment
          preBuild = ''
            export CGO_ENABLED=1
            export CGO_CFLAGS="-I${pkgs.zeromq}/include -I${pkgs.czmq}/include -I${pkgs.libsodium.dev}/include -I${pkgs.arrow-cpp}/include -I${duckdb_1_4_1}/include"
            export CGO_LDFLAGS="-L${pkgs.zeromq}/lib -L${pkgs.czmq}/lib -L${pkgs.libsodium}/lib -L${pkgs.arrow-cpp}/lib -L${duckdb_1_4_1}/lib"
            export PKG_CONFIG_PATH="${pkgs.zeromq}/lib/pkgconfig:${pkgs.czmq}/lib/pkgconfig:${pkgs.libsodium.dev}/lib/pkgconfig:${pkgs.arrow-cpp}/lib/pkgconfig"
          '';

          # Skip tests as they don't exist yet
          doCheck = false;

          meta = with pkgs.lib; {
            description = "Stellar blockchain data processing pipeline";
            homepage = "https://github.com/withObsrvr/cdp-pipeline-workflow";
            license = licenses.mit;
            maintainers = [ ];
            platforms = platforms.linux;
          };
        };


        # Docker push scripts (standalone - build containers on demand)
        pushToProd = pkgs.writeShellScriptBin "push-to-dockerhub-prod" ''
          set -e
          
          IMAGE_NAME="obsrvr-flow-pipeline"
          TAG="''${1:-latest}"
          USERNAME="''${2:-withobsrvr}"
          
          echo "Building production container with Docker..."
          ${pkgs.docker}/bin/docker build -f Dockerfile.nix -t "docker.io/$USERNAME/$IMAGE_NAME:$TAG" .
          
          echo "Pushing to DockerHub..."
          ${pkgs.docker}/bin/docker push "docker.io/$USERNAME/$IMAGE_NAME:$TAG"
          
          echo "Successfully pushed docker.io/$USERNAME/$IMAGE_NAME:$TAG"
        '';

        pushToDev = pkgs.writeShellScriptBin "push-to-dockerhub-dev" ''
          set -e
          
          IMAGE_NAME="cdp-pipeline-dev"
          TAG="''${1:-latest}"
          USERNAME="''${2:-withobsrvr}"
          
          echo "Building development container with Docker..."
          ${pkgs.docker}/bin/docker build -f Dockerfile.nix -t "docker.io/$USERNAME/$IMAGE_NAME:$TAG" .
          
          echo "Pushing to DockerHub..."
          ${pkgs.docker}/bin/docker push "docker.io/$USERNAME/$IMAGE_NAME:$TAG"
          
          echo "Successfully pushed docker.io/$USERNAME/$IMAGE_NAME:$TAG"
        '';

      in
      {
        # Development shell
        devShells.default = pkgs.mkShell {
          buildInputs = with pkgs; [
            # Go development
            go_1_25
            
            # System dependencies and development tools
          ] ++ systemDeps ++ devTools;

          shellHook = ''
            echo "🚀 CDP Pipeline Workflow Development Environment"
            echo ""
            echo "Available commands:"
            echo "  go build -o cdp-pipeline-workflow        - Build Go application locally"
            echo "  nix build                                - Build the Go application with Nix"
            echo "  docker build -f Dockerfile.nix .         - Build Docker container"
            echo "  nix build .#push-to-dockerhub-prod       - Build DockerHub push script (prod)"
            echo "  nix build .#push-to-dockerhub-dev        - Build DockerHub push script (dev)"
            echo ""
            echo "Environment setup:"
            echo "  CGO_ENABLED=1"
            echo "  Go version: $(go version)"
            echo ""

            # Set up CGO environment
            export CGO_ENABLED=1
            export CGO_CFLAGS="-I${pkgs.zeromq}/include -I${pkgs.czmq}/include -I${pkgs.libsodium.dev}/include -I${pkgs.arrow-cpp}/include -I${duckdb_1_4_1}/include"
            export CGO_LDFLAGS="-L${pkgs.zeromq}/lib -L${pkgs.czmq}/lib -L${pkgs.libsodium}/lib -L${pkgs.arrow-cpp}/lib -L${duckdb_1_4_1}/lib"
            export PKG_CONFIG_PATH="${pkgs.zeromq}/lib/pkgconfig:${pkgs.czmq}/lib/pkgconfig:${pkgs.libsodium.dev}/lib/pkgconfig:${pkgs.arrow-cpp}/lib/pkgconfig"
            
            # Set custom prompt to indicate Nix development environment
            export PS1="\[\033[1;34m\][nix-cdp]\[\033[0m\] \[\033[1;32m\]\u@\h\[\033[0m\]:\[\033[1;34m\]\w\[\033[0m\]\$ "
          '';
        };

        # Package outputs
        packages = {
          default = cdp-pipeline;
          cdp-pipeline = cdp-pipeline;
          push-to-dockerhub-prod = pushToProd;
          push-to-dockerhub-dev = pushToDev;
        };

        # Application output
        apps = {
          default = flake-utils.lib.mkApp {
            drv = cdp-pipeline;
            exePath = "/bin/cdp-pipeline-workflow";
          };
        };

        # Formatter for `nix fmt`
        formatter = pkgs.nixpkgs-fmt;
      });
}