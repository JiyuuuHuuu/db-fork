#!/bin/bash
# Create sda4 in the largest free gap of sda (between root sda3 and swap sda99 on xl170), ext4, mount /mnt/ssd.
# Idempotent. Run on a node: ssh node 'bash -s' < setup_ssd.sh
set -eu
if mountpoint -q /mnt/ssd; then echo "$(hostname -s): already mounted"; df -h /mnt/ssd | tail -1; exit 0; fi
if [ ! -b /dev/sda4 ]; then
  command -v sgdisk >/dev/null || sudo DEBIAN_FRONTEND=noninteractive apt-get -y -qq install gdisk >/dev/null
  sudo sgdisk -e /dev/sda >/dev/null 2>&1 || true          # move backup GPT to the true end of the disk
  sudo sgdisk -n 4:0:0 -t 4:8300 -c 4:ssd /dev/sda >/dev/null   # 0:0 = largest free block
  sudo partprobe /dev/sda; sudo udevadm settle
fi
sudo blkid /dev/sda4 | grep -q ext4 || sudo mkfs.ext4 -q -L ssd /dev/sda4
sudo mkdir -p /mnt/ssd
U=$(sudo blkid -s UUID -o value /dev/sda4)
grep -q "$U" /etc/fstab || echo "UUID=$U /mnt/ssd ext4 defaults,noatime,nofail 0 2" | sudo tee -a /etc/fstab >/dev/null
sudo mount /mnt/ssd && sudo chown JiyuHu23:rasl-PG0 /mnt/ssd
echo "$(hostname -s): $(lsblk -no NAME,SIZE /dev/sda4) $(df -h /mnt/ssd | tail -1)"
