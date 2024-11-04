#!/bin/bash

sudo nohup python3 binance_socket.py -s BIN > z_bin_socket.out 2>&1 &
