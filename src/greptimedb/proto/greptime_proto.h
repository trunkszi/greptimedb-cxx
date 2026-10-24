/** GreptimeDB wire messages and reflected RPC contracts at the pinned protocol revision. */
#pragma once

#include <Flight.grpcxx.h>
#include <greptime/v1/database.grpcxx.h>
#include <greptime/v1/health.grpcxx.h>
#include <greptime/v1/prom.grpcxx.h>
#include <prometheus/io/prometheus/write/v2/types.pb.h>
#include <prometheus/remote/remote.pb.h>
#include <substrait_extension/dist_plan.pb.h>
#include <substrait_extension/promql_plan.pb.h>
