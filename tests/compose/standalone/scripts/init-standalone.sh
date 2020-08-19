#!/usr/bin/env bash

bin/apply-config-from-env.py conf/client.conf

sleep 5

bin/pulsar-admin namespaces create public/retention
bin/pulsar-admin namespaces create public/deduplication
bin/pulsar-admin namespaces set-retention public/retention --time 3h --size 1G
bin/pulsar-admin namespaces set-deduplication public/deduplication --enable
bin/pulsar-admin namespaces set-schema-validation-enforce --enable public/default
bin/pulsar-admin topics create-partitioned-topic persistent://public/default/partitioned --partitions 3
bin/pulsar-admin topics create-partitioned-topic persistent://public/default/partitioned2 --partitions 2
bin/pulsar-admin topics create-partitioned-topic persistent://public/default/partitioned3 --partitions 3
bin/pulsar-admin topics create-partitioned-topic persistent://public/default/partitioned4 --partitions 2
bin/pulsar-admin topics create-partitioned-topic persistent://public/default/partitioned5 --partitions 2
bin/pulsar-admin topics create-partitioned-topic persistent://public/deduplication/partitioned --partitions 3