#!/bin/bash
# Hardware facts for one node; run remotely: ssh node 'bash -s' < hwprobe.sh
s(){ echo; echo "### $*"; eval "$@" 2>&1; }
s hostname -f; s uname -r; s 'lsb_release -ds'
s lscpu
s 'numactl -H || lscpu | grep -i numa'
s free -g
s 'sudo dmidecode -t memory | grep -E "^\s+(Size|Speed|Type|Configured Memory Speed):" | sort | uniq -c'
s 'lsblk -d -o NAME,MODEL,SIZE,ROTA,TRAN'
s 'df -h -x tmpfs -x devtmpfs -x squashfs -x overlay'
s 'ip -br a'; s 'ip route'
s 'for i in $(ls /sys/class/net | grep -v lo); do echo "== $i"; ethtool $i | grep -E "Speed|Duplex|Link detected"; ethtool -i $i | grep -E "driver|^version|firmware|bus-info"; cat /sys/class/net/$i/mtu; done'
s 'lspci | grep -iE "ethernet|infiniband|mellanox"'
s 'ofed_info -s'; s 'modinfo mlx5_core | grep -E "^version|^filename"'
s ibv_devices; s 'ibv_devinfo -v | grep -E "hca_id|transport|fw_ver|node_guid|vendor_part|board_id|phys_port_cnt|port:|state|active_mtu|link_layer|active_width|active_speed|max_qp:|GID"'
s 'rdma link'; s 'which ib_write_lat ib_read_lat iperf3 fio'
s 'for d in /sys/class/infiniband/*; do for p in $d/ports/*; do echo "== $d $p"; for g in $p/gids/*; do v=$(cat $g); [ "$v" != "0000:0000:0000:0000:0000:0000:0000:0000" ] && echo "$(basename $g) $v $(cat $p/gid_attrs/types/$(basename $g) 2>/dev/null) $(cat $p/gid_attrs/ndevs/$(basename $g) 2>/dev/null)"; done; done; done'
s 'sysctl net.core.rmem_max net.core.wmem_max net.ipv4.tcp_rmem net.ipv4.tcp_wmem net.ipv4.tcp_congestion_control'
s 'sudo iptables -S | head; sudo ufw status'
s 'cat /sys/devices/system/cpu/cpu0/cpufreq/scaling_governor; cat /sys/devices/system/cpu/intel_pstate/no_turbo'
