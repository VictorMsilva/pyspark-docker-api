#!/bin/bash

echo "🚀 Starting PySpark Docker API Environment..."

# Função para mostrar ajuda
show_help() {
    echo "Usage: $0 [OPTION]"
    echo ""
    echo "Options:"
    echo "  start     - Start Spark cluster and API"
    echo "  job       - Run Spark job only"
    echo "  api       - Start API only"
    echo "  stop      - Stop all services"
    echo "  logs      - Show logs"
    echo "  clean     - Clean up containers and volumes"
    echo "  help      - Show this help message"
}

case "$1" in
    start)
        echo "🔥 Starting Spark cluster and API..."
        docker-compose up -d spark-master spark-worker taxi-api
        echo "✅ Services started!"
        echo "📊 Spark Master UI: http://localhost:8080"
        echo "⚙️  Spark Worker UI: http://localhost:8081"
        echo "🌐 API Docs: http://localhost:8001/docs"
        ;;
    job)
        echo "⚡ Running Spark job..."
        docker-compose --profile job up spark-job
        ;;
    api)
        echo "🌐 Starting API only..."
        docker-compose up -d taxi-api
        echo "✅ API started at http://localhost:8001"
        ;;
    stop)
        echo "🛑 Stopping all services..."
        docker-compose down
        ;;
    logs)
        echo "📋 Showing logs..."
        docker-compose logs -f
        ;;
    clean)
        echo "🧹 Cleaning up containers and volumes..."
        docker-compose down -v --remove-orphans
        docker system prune -f
        ;;
    help|*)
        show_help
        ;;
esac
