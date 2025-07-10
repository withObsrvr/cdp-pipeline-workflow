{
  description = "CDP Pipeline Workflow - Stellar blockchain data processing pipeline";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
    flake-utils.url = "github:numtide/flake-utils";
    nix2container = {
      url = "github:nlewo/nix2container";
      inputs.nixpkgs.follows = "nixpkgs";
    };
  };

  outputs = { self, nixpkgs, flake-utils, nix2container }:
    flake-utils.lib.eachDefaultSystem (system:
      let
        pkgs = nixpkgs.legacyPackages.${system};
        nix2containerPkgs = nix2container.packages.${system};

        # System dependencies required for CGO
        systemDeps = with pkgs; [
          zeromq
          czmq
          libsodium
          arrow-cpp
          pkg-config
          gcc
          gnumake
          cmake
          openssl
        ];

        # Development tools
        devTools = with pkgs; [
          # Container tools
          nerdctl
          containerd
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
          vendorHash = "sha256-txBhNeCZYonzYJr3V7VyqEr638kzH38wXTwt1XbP7WM=";

          nativeBuildInputs = systemDeps;
          buildInputs = systemDeps;

          # Enable CGO
          env.CGO_ENABLED = "1";

          # Build flags
          ldflags = [
            "-s"
            "-w"
            "-extldflags=-static"
          ];

          # Set build environment
          preBuild = ''
            export CGO_ENABLED=1
            export CGO_CFLAGS="-I${pkgs.zeromq}/include -I${pkgs.czmq}/include -I${pkgs.libsodium}/include -I${pkgs.arrow-cpp}/include"
            export CGO_LDFLAGS="-L${pkgs.zeromq}/lib -L${pkgs.czmq}/lib -L${pkgs.libsodium}/lib -L${pkgs.arrow-cpp}/lib"
            export PKG_CONFIG_PATH="${pkgs.zeromq}/lib/pkgconfig:${pkgs.czmq}/lib/pkgconfig:${pkgs.libsodium}/lib/pkgconfig:${pkgs.arrow-cpp}/lib/pkgconfig"
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

        # Create a minimal production container
        container-prod = nix2containerPkgs.nix2container.buildImage {
          name = "obsrvr-flow-pipeline";
          tag = "latest";
          
          copyToRoot = pkgs.buildEnv {
            name = "container-root";
            paths = with pkgs; [
              cdp-pipeline
              coreutils
              bash
              cacert
              tzdata
              # Runtime libraries
              zeromq
              czmq
              libsodium
            ];
            pathsToLink = [ "/bin" "/lib" "/share" ];
          };

          config = {
            Entrypoint = [ "${cdp-pipeline}/bin/cdp-pipeline-workflow" ];
            WorkingDir = "/app";
            User = "1000:1000";
            Env = [
              "PATH=/bin"
              "SSL_CERT_FILE=${pkgs.cacert}/etc/ssl/certs/ca-bundle.crt"
            ];
            ExposedPorts = {
              "8080/tcp" = {};
            };
          };
        };

        # Create a development container with debugging tools
        container-dev = nix2containerPkgs.nix2container.buildImage {
          name = "cdp-pipeline-dev";
          tag = "latest";
          
          copyToRoot = pkgs.buildEnv {
            name = "container-dev-root";
            paths = with pkgs; [
              cdp-pipeline
              coreutils
              bash
              cacert
              tzdata
              # Runtime libraries
              zeromq
              czmq
              libsodium
              # Development tools
              curl
              jq
              yq-go
              vim
              htop
              nettools
              iputils
              sudo
            ];
            pathsToLink = [ "/bin" "/lib" "/share" "/etc" ];
          };

          config = {
            Entrypoint = [ "/bin/bash" ];
            WorkingDir = "/app";
            User = "1000:1000";
            Env = [
              "PATH=/bin"
              "SSL_CERT_FILE=${pkgs.cacert}/etc/ssl/certs/ca-bundle.crt"
              "DEV_MODE=true"
              "LOG_LEVEL=debug"
            ];
            ExposedPorts = {
              "8080/tcp" = {};
              "5555/tcp" = {}; # ZeroMQ
            };
            Volumes = {
              "/app/config" = {};
              "/app/data" = {};
            };
          };
        };

        # Docker push scripts
        pushToProd = pkgs.writeShellScriptBin "push-to-dockerhub-prod" ''
          set -e
          
          IMAGE_NAME="obsrvr-flow-pipeline"
          TAG="''${1:-latest}"
          REGISTRY="''${2:-docker.io}"
          
          echo "Loading container image..."
          ${pkgs.nerdctl}/bin/nerdctl load < ${container-prod}
          
          echo "Tagging image..."
          ${pkgs.nerdctl}/bin/nerdctl tag "$IMAGE_NAME:latest" "$REGISTRY/$IMAGE_NAME:$TAG"
          
          echo "Pushing to DockerHub..."
          ${pkgs.nerdctl}/bin/nerdctl push "$REGISTRY/$IMAGE_NAME:$TAG"
          
          echo "Successfully pushed $REGISTRY/$IMAGE_NAME:$TAG"
        '';

        pushToDev = pkgs.writeShellScriptBin "push-to-dockerhub-dev" ''
          set -e
          
          IMAGE_NAME="cdp-pipeline-dev"
          TAG="''${1:-latest}"
          REGISTRY="''${2:-docker.io}"
          
          echo "Loading container image..."
          ${pkgs.nerdctl}/bin/nerdctl load < ${container-dev}
          
          echo "Tagging image..."
          ${pkgs.nerdctl}/bin/nerdctl tag "$IMAGE_NAME:latest" "$REGISTRY/$IMAGE_NAME:$TAG"
          
          echo "Pushing to DockerHub..."
          ${pkgs.nerdctl}/bin/nerdctl push "$REGISTRY/$IMAGE_NAME:$TAG"
          
          echo "Successfully pushed $REGISTRY/$IMAGE_NAME:$TAG"
        '';

      in
      {
        # Development shell
        devShells.default = pkgs.mkShell {
          buildInputs = with pkgs; [
            # Go development
            go_1_23
            
            # System dependencies
          ] ++ systemDeps ++ devTools ++ [
            # Push scripts
            pushToProd
            pushToDev
          ];

          shellHook = ''
            echo "🚀 CDP Pipeline Workflow Development Environment"
            echo ""
            echo "Available commands:"
            echo "  nix build                    - Build the Go application"
            echo "  nix build .#container-prod   - Build production container"
            echo "  nix build .#container-dev    - Build development container"
            echo "  push-to-dockerhub-prod      - Push production image to DockerHub"
            echo "  push-to-dockerhub-dev       - Push development image to DockerHub"
            echo ""
            echo "Environment setup:"
            echo "  CGO_ENABLED=1"
            echo "  Go version: $(go version)"
            echo ""
            
            # Set up CGO environment
            export CGO_ENABLED=1
            export CGO_CFLAGS="-I${pkgs.zeromq}/include -I${pkgs.czmq}/include -I${pkgs.libsodium}/include -I${pkgs.arrow-cpp}/include"
            export CGO_LDFLAGS="-L${pkgs.zeromq}/lib -L${pkgs.czmq}/lib -L${pkgs.libsodium}/lib -L${pkgs.arrow-cpp}/lib"
            export PKG_CONFIG_PATH="${pkgs.zeromq}/lib/pkgconfig:${pkgs.czmq}/lib/pkgconfig:${pkgs.libsodium}/lib/pkgconfig:${pkgs.arrow-cpp}/lib/pkgconfig"
          '';
        };

        # Package outputs
        packages = {
          default = cdp-pipeline;
          cdp-pipeline = cdp-pipeline;
          container-prod = container-prod;
          container-dev = container-dev;
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