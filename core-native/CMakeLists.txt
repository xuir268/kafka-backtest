cmake_minimum_required(VERSION 3.20)
project(kbt_core_native LANGUAGES CXX)
# C ABI only. Hostile-input hardening per spec section 5.4.
# TODO: fixture reader, deterministic merge, diff core
