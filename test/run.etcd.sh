etcd \
  --data-dir "./etcd.data" \
  --listen-client-urls "http://localhost:2379" \
  --advertise-client-urls "http://localhost:2379" \
  --listen-peer-urls "http://localhost:2380" \
  --initial-advertise-peer-urls "http://localhost:2380" \
  --log-level warn
