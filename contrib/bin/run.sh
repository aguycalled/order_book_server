sudo sysctl -w net.ipv6.conf.all.disable_ipv6=1
sudo sysctl -w net.ipv6.conf.default.disable_ipv6=1
sudo sysctl -w net.ipv6.conf.lo.disable_ipv6=1

~/hl-visor run-non-validator --write-fills --write-order-statuses --write-raw-book-diffs --stream-with-block-info --disable-output-file-buffering --replica-cmds-style actions-and-responses --serve-info --write-system-and-core-writer-actions --write-misc-events
