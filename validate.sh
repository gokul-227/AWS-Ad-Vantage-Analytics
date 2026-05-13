#!/usr/bin/env bash
# ============================================================
# validate.sh — Health checks and pipeline validation
# ============================================================

PASS=0
FAIL=0

check() {
    local name="$1"
    local cmd="$2"
    echo -n "  $name: "
    if eval "$cmd" >/dev/null 2>&1; then
        echo "✅ OK"
        PASS=$((PASS + 1))
    else
        echo "❌ FAILED"
        FAIL=$((FAIL + 1))
    fi
}

echo "╔══════════════════════════════════════════════════╗"
echo "║   Pipeline Validation                           ║"
echo "╚══════════════════════════════════════════════════╝"
echo ""

echo "🐳 Docker Services:"
check "Kafka container" "docker compose ps kafka --format json | grep -i 'running'"
check "JobManager container" "docker compose ps jobmanager --format json | grep -i 'running'"
check "TaskManager container" "docker compose ps taskmanager --format json | grep -i 'running'"
check "Producer container" "docker compose ps producer --format json | grep -i 'running'"
check "Consumer container" "docker compose ps consumer --format json | grep -i 'running'"

echo ""
echo "📡 Kafka Topics:"
check "views-topic exists" "docker compose exec -T kafka kafka-topics --bootstrap-server localhost:9092 --describe --topic views-topic"
check "clicks-topic exists" "docker compose exec -T kafka kafka-topics --bootstrap-server localhost:9092 --describe --topic clicks-topic"
check "metrics-topic exists" "docker compose exec -T kafka kafka-topics --bootstrap-server localhost:9092 --describe --topic metrics-topic"

echo ""
echo "🔧 Flink Cluster:"
check "Flink Dashboard (8081)" "curl -sf http://localhost:8081/overview"
check "Flink has running jobs" "curl -sf http://localhost:8081/jobs/overview | grep -i 'RUNNING'"

echo ""
echo "🌐 Web UIs:"
check "Kafka UI (8080)" "curl -sf http://localhost:8080"

echo ""
echo "📊 Data Flow:"
# Check if we can read at least 1 message
METRICS_COUNT=$(docker compose exec -T kafka kafka-console-consumer \
    --bootstrap-server localhost:9092 --topic metrics-topic \
    --from-beginning --max-messages 1 --timeout-ms 5000 2>/dev/null | grep -c "metricName" || echo "0")
echo "  Metrics produced: $METRICS_COUNT messages"
if [ "$METRICS_COUNT" -gt 0 ]; then
    echo "  ✅ Pipeline is producing metrics"
    PASS=$((PASS + 1))
else
    echo "  ⚠️  No metrics yet (pipeline may still be starting)"
fi

echo ""
echo "═══════════════════════════════════════════════════"
echo "  Results: $PASS passed, $FAIL failed"
echo "═══════════════════════════════════════════════════"

if [ "$FAIL" -gt 0 ]; then
    exit 1
fi
