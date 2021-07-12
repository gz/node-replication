#!/bin/bash

set -ex

install_deps()
{
  sudo apt update --yes

  sudo apt install autoconf automake build-essential cmake libdaxctl-dev \
	  libndctl-dev libnuma-dev libtbb-dev libtool rapidjson-dev --yes

  sudo apt install pandoc libglib2.0-dev libfabric-dev valgrind --yes

  sudo apt install python3-pip keychain jq numactl libhwloc-dev --yes
}

install_pmdk()
{
  SRCDIR=$(pwd)
  DEPDIR=${SRCDIR}/deps

  mkdir -p ${DEPDIR} || die 'cannot create ${DEPDIR}'

  # Install pmdk
  cd ${DEPDIR}
  git clone https://github.com/pmem/pmdk
  cd pmdk
  make -j$(nproc)
  sudo make install

  # Install libpmemobj C++ bindings
  cd ${DEPDIR}
  git clone https://github.com/pmem/libpmemobj-cpp
  cd libpmemobj-cpp
  mkdir build
  cd build
  cmake ..
  make -j$(nproc)
  sudo make install

  # Install memkind library
  cd ${DEPDIR}
  git clone https://github.com/memkind/memkind
  cd memkind
  ./autogen.sh
  ./configure
  make
  sudo make install

  #Install pmemkv library
  cd ${DEPDIR}
  git clone https://github.com/pmem/pmemkv
  cd pmemkv
  mkdir ./build
  cd ./build
  cmake ..
  make -j$(nproc)
  sudo make install

  # Download examples
  cd ${DEPDIR}
  git clone https://github.com/Apress/programming-persistent-memory.git
}

install_rust()
{
  if [ -f $HOME/.cargo/env ]; then
    source $HOME/.cargo/env
  fi

  # Make sure rust is up-to-date
  if [ ! -x "$(command -v rustup)" ] ; then
      curl https://sh.rustup.rs -sSf | sh -s -- -y
  fi

  source $HOME/.cargo/env
  rustup default nightly
  rustup component add rust-src
  rustup update
}

# Install all the dependencies
install_deps
install_rust

# Install pmdk libraries
#install_pmdk
