#!/bin/bash
set -e

# docker-helpers.sh
# Utilidades para gestionar Docker en desarrollo

# Colores para output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Funciones
print_info() {
    echo -e "${BLUE} $1${NC}"
}

print_success() {
    echo -e "${GREEN} $1${NC}"
}

print_warning() {
    echo -e "${YELLOW} $1${NC}"
}

print_error() {
    echo -e "${RED} $1${NC}"
}

# ============================================================================
# FUNCIONES PRINCIPALES
# ============================================================================

docker_dev_up() {
    echo ""
    print_info "Levantando Docker (DESARROLLO - sin persistencia)..."
    docker-compose -f docker-compose.yml -f docker-compose.dev.yml up -d
    
    print_info "Esperando a que Kafka esté listo (20 segundos)..."
    sleep 20
    
    print_success "Docker dev listo"
    docker_status
}

docker_prod_up() {
    echo ""
    print_info "Levantando Docker (PRODUCCIÓN - con persistencia)..."
    docker-compose -f docker-compose.yml up -d
    
    print_info "Esperando a que Kafka esté listo (20 segundos)..."
    sleep 20
    
    print_success "Docker prod listo"
    docker_status
}

docker_clean() {
    echo ""
    print_warning "Limpiando Docker completamente..."
    
    print_info "Deteniendo containers..."
    docker-compose down -v 2>/dev/null || true
    
    print_info "Limpiando volúmenes huérfanos..."
    docker volume prune -f 2>/dev/null || true
    
    print_info "Limpiando imágenes dangling..."
    docker image prune -f 2>/dev/null || true
    
    print_success "Limpieza completada"
}

docker_reset_dev() {
    echo ""
    print_warning "Reset DEV (limpia todo y levanta fresh)..."
    docker_clean
    docker_dev_up
}

docker_reset_prod() {
    echo ""
    print_warning "Reset PROD (limpia todo y levanta fresh)..."
    docker_clean
    docker_prod_up
}

docker_stop() {
    echo ""
    print_info "Deteniendo Docker..."
    docker-compose stop
    print_success "Docker detenido"
}

docker_status() {
    echo ""
    print_info "Status de containers:"
    docker-compose ps
    echo ""
}

docker_logs_kafka() {
    echo ""
    print_info "Logs de Kafka (últimas 50 líneas)..."
    docker-compose logs kafka | tail -50
}

docker_logs_zk() {
    echo ""
    print_info "Logs de Zookeeper (últimas 50 líneas)..."
    docker-compose logs zookeeper | tail -50
}

docker_shell_kafka() {
    echo ""
    print_info "Accediendo a shell de Kafka..."
    docker-compose exec kafka bash
}

docker_test_kafka() {
    echo ""
    print_info "Testeando conexión a Kafka..."
    docker-compose exec kafka kafka-topics --list --bootstrap-server localhost:9092
    print_success "Kafka respondiendo correctamente"
}

# ============================================================================
# MENU PRINCIPAL
# ============================================================================

show_help() {
    echo ""
    echo -e "${BLUE}Docker Helper - Utilidades para gestionar Docker${NC}"
    echo ""
    echo -e "${YELLOW}Uso:${NC}"
    echo "  $0 <comando>"
    echo ""
    echo -e "${YELLOW}Comandos:${NC}"
    echo -e "  ${GREEN}dev-up${NC}              Levanta Docker en modo DESARROLLO (sin persistencia)"
    echo -e "  ${GREEN}prod-up${NC}             Levanta Docker en modo PRODUCCIÓN (con persistencia)"
    echo -e "  ${GREEN}reset-dev${NC}           Limpia y levanta Docker en modo DESARROLLO"
    echo -e "  ${GREEN}reset-prod${NC}          Limpia y levanta Docker en modo PRODUCCIÓN"
    echo -e "  ${GREEN}stop${NC}                Detiene todos los containers"
    echo -e "  ${GREEN}clean${NC}               Limpia volúmenes y containers"
    echo -e "  ${GREEN}status${NC}              Muestra estado de los containers"
    echo -e "  ${GREEN}logs-kafka${NC}          Muestra logs de Kafka"
    echo -e "  ${GREEN}logs-zk${NC}             Muestra logs de Zookeeper"
    echo -e "  ${GREEN}shell-kafka${NC}         Accede a shell de Kafka"
    echo -e "  ${GREEN}test-kafka${NC}          Testa conexión a Kafka"
    echo -e "  ${GREEN}help${NC}                Muestra esta ayuda"
    echo ""
    echo -e "${YELLOW}Ejemplos:${NC}"
    echo "  $0 dev-up                # Levanta dev"
    echo "  $0 reset-dev             # Reset dev (limpia todo)"
    echo "  $0 logs-kafka            # Ver logs de Kafka"
    echo "  $0 test-kafka            # Verificar que Kafka funciona"
    echo ""
}

# ============================================================================
# MAIN
# ============================================================================

if [ $# -eq 0 ]; then
    show_help
    exit 0
fi

case "$1" in
    dev-up)
        docker_dev_up
        ;;
    prod-up)
        docker_prod_up
        ;;
    reset-dev)
        docker_reset_dev
        ;;
    reset-prod)
        docker_reset_prod
        ;;
    stop)
        docker_stop
        ;;
    clean)
        docker_clean
        ;;
    status)
        docker_status
        ;;
    logs-kafka)
        docker_logs_kafka
        ;;
    logs-zk)
        docker_logs_zk
        ;;
    shell-kafka)
        docker_shell_kafka
        ;;
    test-kafka)
        docker_test_kafka
        ;;
    help)
        show_help
        ;;
    *)
        print_error "Comando desconocido: $1"
        echo ""
        show_help
        exit 1
        ;;
esac

echo ""