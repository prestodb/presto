FROM ubuntu:24.04

# Install necessary tools
RUN apt-get update && apt-get install -y \
    # Intellij IDEA dev container prerequisites
	curl \
	git \
	unzip \
    # Java 8 and 17 jdk
	openjdk-8-jdk \
    openjdk-17-jdk \
    # for documentation
    python3 \
    python3-pip \
    python3.12-venv \
    fonts-freefont-otf \
    xindy

