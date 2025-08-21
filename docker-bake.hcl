variable "tag" {
  default = "ghcr.io/facebookincubator/velox-dev"
}

# Variable to control cache pushing
variable "DOCKER_UPLOAD_CACHE" {
  default = false
}

function "cache-to-arch" {
  params = [name, arch]
  # We don't want to push local build to the cache by accident
  # so we remove cache-to unless we are running in CI
  result = DOCKER_UPLOAD_CACHE ? [{
    type        = "registry"
    ref         = "${tag}:build-cache-${name}-${arch}"
    mode        = "max"
    compression = "zstd"
  }] : []
}

function "cache-from-arch" {
  params = [name, arch]
  result = [{
    type = "registry"
    ref  = "${tag}:build-cache-${name}-${arch}"
  }]
}

function "ci_images_by_arch" {
  params = [arch]
  result = ["centos9-${arch}", "adapters-${arch}"]
}

group "ci-amd64" {
  targets = ci_images_by_arch("amd64")
}

group "ci-arm64" {
  targets = ci_images_by_arch("arm64")
}

group "default" {
  targets = []
}

target "base" {
  output = [
    DOCKER_UPLOAD_CACHE ? {
      type           = "registry"
      compression    = "zstd"
      oci-mediatypes = true
      } : {
      # For local builds with the docker driver the image will be loaded
      # even without explicitly adding the `docker` exporter
      type = "cacheonly"
    }

  ]
}

target "pyvelox" {
  inherits   = ["base"]
  context    = "."
  name       = "pyvelox-${arch}"
  dockerfile = "scripts/docker/centos-multi.dockerfile"
  target     = "pyvelox"
  args = {
    image              = "quay.io/pypa/manylinux_2_28:latest"
    VELOX_BUILD_SHARED = "OFF"
  }
  matrix = {
    arch = ["amd64", "arm64"]
  }
  platforms  = ["linux/${arch}"]
  tags       = ["${tag}:pyvelox-${arch}"]
  cache-to   = cache-to-arch("pyvelox", "${arch}")
  cache-from = cache-from-arch("pyvelox", "${arch}")
}

target "adapters" {
  inherits = ["base","adapters-cpp"]
  name     = "adapters-${arch}"
  matrix = {
    arch = ["amd64", "arm64"]
  }
  platforms  = ["linux/${arch}"]
  tags       = ["${tag}:adapters-${arch}"]
  cache-to   = cache-to-arch("adapters", "${arch}")
  cache-from = cache-from-arch("adapters", "${arch}")
}

target "centos9" {
  inherits = ["base","centos-cpp"]
  name     = "centos9-${arch}"
  matrix = {
    arch = ["amd64", "arm64"]
  }
  platforms  = ["linux/${arch}"]
  tags       = ["${tag}:centos9-${arch}"]
  cache-to   = cache-to-arch("centos9", "${arch}")
  cache-from = cache-from-arch("centos9", "${arch}")
}

target "ubuntu-amd64" {
  inherits   = ["base","ubuntu-cpp"]
  cache-to   = cache-to-arch("ubuntu", "amd64")
  cache-from = cache-from-arch("ubuntu", "amd64")
}

group "ubuntu-arm64" {
  # We don't actually want to build the ubuntu arm image, this is a trick to simplify CI
  # Empty targets don't fail the build.
  targets = []
}

group "fedora-arm64" {
  # We don't actually want to build the fedora arm image, this is a trick to simplify CI
  # Empty targets don't fail the build.
  targets = []
}

target "fedora-amd64" {
  inherits   = ["base", "fedora"]
  dockerfile = "scripts/docker/fedora.dockerfile"
  cache-to   = cache-to-arch("fedora", "amd64")
  cache-from = cache-from-arch("fedora", "amd64")
}

group "java" {
  # The main work is in the well cached download steps and the shared base stage,
  # so these can easily be run on the same node in ci
  targets = ["spark-server", "presto-java"]
}

target "spark-server" {
  inherits = ["base"]
  target     = "spark-server"
  cache-to   = cache-to-arch("spark-server", "amd64")
  cache-from = cache-from-arch("spark-server", "amd64")
}

target "presto-java" {
  inherits = ["base"]
  target     = "presto-java"
  cache-to   = cache-to-arch("presto-java", "amd64")
  cache-from = cache-from-arch("presto-java", "amd64")
}
