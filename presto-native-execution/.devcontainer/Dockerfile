FROM presto/prestissimo-dependency:centos9

# Install necesary packages to run dev containers in CLion
# https://www.jetbrains.com/help/clion/prerequisites-for-dev-containers.html#remote_container
RUN dnf -y update && dnf -y install epel-release
RUN dnf -y --skip-broken install \
    curl \
    unzip \
    procps \
    libXext \
    libXrender \
    libXtst \
    libXi \
    freetype \
    procps \
    java-17-openjdk-headless \
    python3.12

RUN pip3 install pre-commit
