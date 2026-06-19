#!/bin/bash
rsync -avz --exclude='.git/' --exclude='target/' --exclude='*.rmp' ./ alex@68.233.33.13:/home/alex/order_book_server_v2/
