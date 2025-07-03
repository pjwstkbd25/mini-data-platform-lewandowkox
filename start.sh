#!/bin/bash

echo "Tworzenie sieci dockerowej (jeśli nie istnieje)..."
docker network create mini-net 2>/dev/null || true

echo "Uruchamianie docker-compose z .env..."
docker-compose --env-file .env up --build -d

echo " Gotowe! Sprawdź działanie: docker ps"