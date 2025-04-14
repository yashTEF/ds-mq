#!/bin/bash
set -e

GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m' # No Color

echo -e "${YELLOW}Starting Docker cleaning process...${NC}"

# Stop all running containers
echo -e "${YELLOW}Stopping running containers...${NC}"
docker stop $(docker ps -q) 2>/dev/null || echo -e "${GREEN}No running containers to stop.${NC}"

# Remove all containers
echo -e "${YELLOW}Removing containers...${NC}"
docker rm $(docker ps -a -q) 2>/dev/null || echo -e "${GREEN}No containers to remove.${NC}"

# # Remove all images
# echo -e "${YELLOW}Removing images...${NC}"
# docker rmi $(docker images -q) 2>/dev/null || echo -e "${GREEN}No images to remove.${NC}"

# Prune all unused volumes
echo -e "${YELLOW}Pruning unused volumes...${NC}"
docker volume prune -f

# Prune all unused networks
echo -e "${YELLOW}Pruning unused networks...${NC}"
docker network prune -f

echo -e "${GREEN}Docker cleaning process completed successfully!${NC}"
