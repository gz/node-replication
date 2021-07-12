#!/bin/bash
set -ex

nodes=$(numactl -H | grep nodes | cut -d' ' -f2)

mount() {
  for (( i = 0; i < $nodes; i++ )) ; do
    mntpoint="/mnt/node$i"
    sudo mkdir -p $mntpoint

    # Unmount if already mounted
    sudo umount -f $mntpoint || true
    sudo mount -t ramfs -o dax,size=64g,mpol=bind:$i ext2 $mntpoint
  done
}

umount() {
  for (( i = 0; i < $nodes; i++ )) ; do
    mntpoint="/mnt/node$i"
    sudo umount -f $mntpoint || true
  done
}

#Run the function given as the input argument
func_name=$1
$func_name